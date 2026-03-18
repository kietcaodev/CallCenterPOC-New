using ContactCenterPOC.Hubs;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

namespace ContactCenterPOC.Models
{
    /// <summary>
    /// WebSocket handler for FreeSWITCH audio streaming.
    /// Receives binary PCM 16kHz from FreeSWITCH, resamples to 24kHz, sends to OpenAI/VoiceLive.
    /// Receives AI audio at 24kHz, resamples to 16kHz, sends binary back to FreeSWITCH.
    /// </summary>
    public class FreeSwitchMediaHandler : IMediaStreamingHandler
    {
        private WebSocket _webSocket;
        private CancellationTokenSource _cts;
        private AzureOpenAIService? _aiServiceHandler;
        private VoiceLiveService? _vlServiceHandler;
        private readonly IConfiguration _configuration;
        private readonly ILogger<CallService> _logger;
        private readonly CallLogger _log;
        private readonly IHubContext<TranscriptHub> _hubContext;
        private readonly string _callConnectionId;
        private readonly Func<string, Task>? _hangUpCallback;
        private readonly SentimentAnalysisService? _sentimentService;
        private readonly EmotionAnalysisService? _emotionService;
        private readonly ConcurrentDictionary<string, ActiveCall>? _activeCalls;
        private readonly string _selectedVoice;
        private readonly string _voiceApiMode;
        private readonly string? _voiceLiveModel;
        private readonly string? _selectedVoiceLiveVoice;
        private readonly VoiceLiveConfig? _voiceLiveConfig;
        private readonly FreeSwitchService? _freeSwitchService;

        // Audio format: FreeSWITCH sends 16kHz, OpenAI expects 24kHz
        private const int FreeSwitchSampleRate = 16000;
        private const int OpenAiSampleRate = 24000;

        // Pre-allocated silence frame at 24kHz PCM16 mono (20ms = 960 bytes).
        // Sent to OpenAI during playback mute to maintain continuous audio stream;
        // without this, server-side VAD loses context and won't detect speech onset
        // when real mic audio resumes after the gap.
        private static readonly byte[] SilenceFrame24k = new byte[960];

        // Auto-flush threshold: flush buffer during generation every ~2 seconds of audio
        // to minimize silence gaps when AI generates slower than real-time
        // while keeping segments large enough to avoid choppy ESL playback.
        // (PCM 16kHz mono 16-bit = 32000 bytes/sec, so 64000 = 2s)
        private const int AutoFlushThresholdBytes = 64000;

        // Minimum buffer size for an eager flush (1s = 32000 bytes).
        // Prevents micro-segments that would cause choppy playback via ESL.
        // Each uuid_broadcast has ~50-100ms overhead; sub-second files make this
        // overhead a significant fraction of the segment duration.
        private const int MinImmediateFlushBytes = 32000;

        // Accumulation window (ms) for eager flush. When playback queue empties and
        // _flushOnNextChunk is set, we wait this long before flushing to let more
        // audio accumulate. This converts 7+ micro-segments into 2-3 larger ones.
        private const int EagerAccumulationMs = 300;

        // Buffer to accumulate audio chunks per AI turn, then play via ESL
        private readonly MemoryStream _audioBuffer = new();
        private int _turnCounter;

        // Playback queue: ensures turns play sequentially without overlapping
        private readonly ConcurrentQueue<(string filePath, int pcmBytes)> _playbackQueue = new();
        private int _playbackRunning; // 0 = idle, 1 = running

        // True while uuid_broadcast is playing — mutes upstream audio to prevent echo
        private volatile bool _isPlayingBack;

        // When set, the next AI audio chunk triggers an immediate flush regardless of threshold.
        // Set when playback queue empties so the next audio segment plays without delay.
        private volatile bool _flushOnNextChunk;

        // Timestamp of last FlushAudioAsync — used to ignore stale VAD events
        // (OpenAI delivers SpeechStarted AFTER ResponseFinished for already-processed speech)
        private DateTime _lastFlushAt = DateTime.MinValue;

        // Timestamp when playback ended — used to extend silence period after playback.
        // mod_audio_stream may still carry residual echo from uuid_broadcast in the READ
        // stream for a short period after PLAYBACK_STOP. Without this grace period,
        // that echo feeds OpenAI VAD → phantom transcripts.
        private DateTime _playbackEndedAt = DateTime.MinValue;
        private const int PostPlaybackSilenceMs = 800;

        // Timestamp when _flushOnNextChunk first saw eligible audio — used for
        // accumulation window. Reset when eager flush fires or flushOnNextChunk is cleared.
        private DateTime _eagerAccumulateStart = DateTime.MinValue;

        public FreeSwitchMediaHandler(
            WebSocket webSocket,
            IConfiguration configuration,
            ILogger<CallService> logger,
            IHubContext<TranscriptHub> hubContext,
            string callConnectionId,
            Func<string, Task>? hangUpCallback = null,
            SentimentAnalysisService? sentimentService = null,
            ConcurrentDictionary<string, ActiveCall>? activeCalls = null,
            EmotionAnalysisService? emotionService = null,
            string? selectedVoice = null,
            string voiceApiMode = "ChatGPT",
            string? voiceLiveModel = null,
            string? selectedVoiceLiveVoice = null,
            VoiceLiveConfig? voiceLiveConfig = null,
            FreeSwitchService? freeSwitchService = null)
        {
            _webSocket = webSocket;
            _configuration = configuration;
            _cts = new CancellationTokenSource();
            _logger = logger;
            _log = new CallLogger(logger, callConnectionId, "FS");
            _hubContext = hubContext;
            _callConnectionId = callConnectionId;
            _hangUpCallback = hangUpCallback;
            _sentimentService = sentimentService;
            _emotionService = emotionService;
            _activeCalls = activeCalls;
            _selectedVoice = selectedVoice ?? "alloy";
            _voiceApiMode = voiceApiMode;
            _voiceLiveModel = voiceLiveModel;
            _selectedVoiceLiveVoice = selectedVoiceLiveVoice;
            _voiceLiveConfig = voiceLiveConfig;
            _freeSwitchService = freeSwitchService;
        }

        /// <summary>
        /// Main processing loop: connect to AI service, receive binary audio and relay.
        /// </summary>
        public async Task ProcessWebSocketAsync(string callContextPrompt)
        {
            if (_webSocket == null) return;

            if (_voiceApiMode == "VoiceLive" && _voiceLiveConfig != null && _voiceLiveConfig.IsConfigured)
            {
                _log.Info("Dispatching to VoiceLiveService");
                _vlServiceHandler = new VoiceLiveService(
                    this, callContextPrompt, _voiceLiveConfig, _logger, _hubContext,
                    _callConnectionId, _voiceLiveModel ?? "gpt-4o", _selectedVoiceLiveVoice,
                    _hangUpCallback, _sentimentService, _activeCalls, _emotionService);

                try
                {
                    _vlServiceHandler.StartConversation();
                    await ReceiveFromFreeSwitchAsync();
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "VoiceLive WebSocket error");
                }
                finally
                {
                    _vlServiceHandler.Close();
                    Close();
                    await InvokeHangUpCallback();
                }
            }
            else
            {
                _log.Info("Dispatching to AzureOpenAIService (voice={Voice})",
                    _selectedVoice);
                _aiServiceHandler = new AzureOpenAIService(
                    this, callContextPrompt, _configuration, _logger, _hubContext,
                    _callConnectionId, _hangUpCallback, _sentimentService, _activeCalls,
                    _emotionService, _selectedVoice);

                try
                {
                    _aiServiceHandler.StartConversation();
                    await ReceiveFromFreeSwitchAsync();
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "OpenAI WebSocket error");
                }
                finally
                {
                    _aiServiceHandler.Close();
                    Close();
                    await InvokeHangUpCallback();
                }
            }
        }

        /// <summary>
        /// Buffer resampled audio from AI, or handle StopAudio (barge-in).
        /// </summary>
        public async Task SendMessageAsync(string acsJsonMessage)
        {
            try
            {
                // Handle barge-in: stop current playback when user starts speaking
                if (IsStopAudioMessage(acsJsonMessage))
                {
                    // Ignore stale VAD events that arrive right after flush.
                    // OpenAI delivers SpeechStarted AFTER ResponseFinished for speech
                    // the model already processed. FlushAudioAsync enqueues audio, then
                    // StopAudio arrives 0-2ms later and clears the queue before
                    // ProcessPlaybackQueueAsync can even dequeue it.
                    var msSinceFlush = (DateTime.UtcNow - _lastFlushAt).TotalMilliseconds;
                    if (msSinceFlush < 600)
                    {
                        _log.Info("Ignoring stale barge-in ({Elapsed}ms after flush)",
                            (int)msSinceFlush);
                        return;
                    }

                    _isPlayingBack = false;
                    _audioBuffer.SetLength(0);
                    _eagerAccumulateStart = DateTime.MinValue;
                    _flushOnNextChunk = false;
                    // Clear any queued turns and stop current playback
                    while (_playbackQueue.TryDequeue(out _)) { }
                    // BreakAudioAsync also cancels the pending WaitForPlaybackStopAsync waiter
                    if (_freeSwitchService != null)
                    {
                        await _freeSwitchService.BreakAudioAsync(_callConnectionId);
                    }
                    _log.Info("Barge-in: stopped playback and cleared queue");
                    return;
                }

                var audioBytes = ExtractAudioFromAcsJson(acsJsonMessage);
                if (audioBytes != null && audioBytes.Length > 0)
                {
                    // Resample 24kHz → 16kHz for FreeSWITCH
                    var resampled = AudioResampler.Downsample24kTo16k(audioBytes);
                    _audioBuffer.Write(resampled, 0, resampled.Length);

                    // Auto-flush when buffer exceeds threshold for low-latency playback.
                    // Without this, long AI responses are fully buffered before any audio plays,
                    // causing 30+ seconds of silence until ResponseFinished.
                    //
                    // Eager-flush path: when playback queue emptied (_flushOnNextChunk),
                    // a 300ms accumulation window lets more audio collect before flushing.
                    // This converts many small (<0.5s) WAV files into fewer large (1-2s)
                    // segments, dramatically reducing ESL uuid_broadcast overhead and
                    // eliminating audible gaps between segments.
                    var shouldFlush = _audioBuffer.Length >= AutoFlushThresholdBytes;
                    if (!shouldFlush && _flushOnNextChunk && _audioBuffer.Length >= MinImmediateFlushBytes)
                    {
                        // Start the accumulation window on first eligible chunk
                        if (_eagerAccumulateStart == DateTime.MinValue)
                        {
                            _eagerAccumulateStart = DateTime.UtcNow;
                        }
                        // Flush only after the accumulation window expires
                        if ((DateTime.UtcNow - _eagerAccumulateStart).TotalMilliseconds >= EagerAccumulationMs)
                        {
                            shouldFlush = true;
                            _flushOnNextChunk = false;
                            _eagerAccumulateStart = DateTime.MinValue;
                        }
                    }
                    if (shouldFlush)
                    {
                        await FlushAudioAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to buffer audio");
            }
        }

        /// <summary>
        /// Write buffered audio to a WAV file and enqueue for sequential playback via ESL.
        /// </summary>
        public async Task FlushAudioAsync()
        {
            if (_audioBuffer.Length == 0) return;

            var pcmData = _audioBuffer.ToArray();
            _audioBuffer.SetLength(0);
            _turnCounter++;

            var fileName = $"ai_{_callConnectionId}_{_turnCounter}.wav";
            var filePath = Path.Combine(Path.GetTempPath(), fileName);

            try
            {
                await using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write))
                {
                    WriteWavHeader(fs, pcmData.Length, 16000, 1, 16);
                    await fs.WriteAsync(pcmData);
                }

                _log.Info("Wrote audio turn #{Turn} ({Bytes} bytes) to {Path}",
                    _turnCounter, pcmData.Length, filePath);

                // Enqueue for sequential playback
                _lastFlushAt = DateTime.UtcNow;
                _playbackQueue.Enqueue((filePath, pcmData.Length));
                if (Interlocked.CompareExchange(ref _playbackRunning, 1, 0) == 0)
                {
                    _ = Task.Run(ProcessPlaybackQueueAsync);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to flush audio turn #{Turn}", _turnCounter);
            }
        }

        /// <summary>
        /// Process queued audio files sequentially: play one, wait for PLAYBACK_STOP event, then play next.
        /// Mutes upstream mic audio during playback to prevent echo from mod_audio_stream
        /// feeding uuid_broadcast audio back to OpenAI (which would trigger false VAD → incomplete responses).
        /// </summary>
        private async Task ProcessPlaybackQueueAsync()
        {
            try
            {
                while (_playbackQueue.TryDequeue(out var item))
                {
                    _isPlayingBack = true;
                    await EslBroadcastAsync(item.filePath);
                    // Wait for FreeSWITCH PLAYBACK_STOP event instead of estimating duration.
                    // Fallback timeout = estimated duration + 2s safety margin in case event is lost.
                    var fallbackMs = (int)((double)item.pcmBytes / 32000 * 1000) + 2000;
                    _log.Info("Waiting for PLAYBACK_STOP (fallback {Fallback}ms)",
                        fallbackMs);
                    if (_freeSwitchService != null)
                    {
                        try
                        {
                            await _freeSwitchService.WaitForPlaybackStopAsync(_callConnectionId, item.filePath, fallbackMs, _cts.Token);
                        }
                        catch (OperationCanceledException) when (!_cts.IsCancellationRequested)
                        {
                            _log.Info("Playback wait cancelled by barge-in");
                        }
                    }

                    // Clear input buffer between turns to flush accumulated echo.
                    // mod_audio_stream mixes uuid_broadcast into the READ stream;
                    // even with silence-mute, residual echo may have leaked through
                    // the ~1ms transition window. Clearing here ensures each gap
                    // between turns starts with a clean slate.
                    try
                    {
                        if (_aiServiceHandler != null)
                            await _aiServiceHandler.ClearInputAudioBufferAsync();
                        else if (_vlServiceHandler != null)
                            await _vlServiceHandler.ClearInputAudioBufferAsync();
                    }
                    catch { /* best effort */ }
                }
                _isPlayingBack = false;
                _playbackEndedAt = DateTime.UtcNow;

                // Signal SendMessageAsync to flush immediately on the next audio chunk.
                // Without this, after a turn finishes playing, audio sits in the buffer
                // until it reaches AutoFlushThresholdBytes — causing silence gaps when
                // the AI generates audio slower than real-time.
                _flushOnNextChunk = true;

                _log.Info("Playback queue empty, upstream unmuted");
            }
            catch (OperationCanceledException) { }
            finally
            {
                _isPlayingBack = false;
                Interlocked.Exchange(ref _playbackRunning, 0);
                // Re-check in case items were enqueued while we were finishing
                if (!_playbackQueue.IsEmpty && Interlocked.CompareExchange(ref _playbackRunning, 1, 0) == 0)
                {
                    _ = Task.Run(ProcessPlaybackQueueAsync);
                }
            }
        }

        /// <summary>
        /// Play audio file on the call via FreeSwitchService ESL connection.
        /// </summary>
        private async Task EslBroadcastAsync(string filePath)
        {
            if (_freeSwitchService == null) return;

            _log.Info("ESL IsConnected={Connected}, calling PlayAudioAsync for {Path}",
                _freeSwitchService.IsConnected, filePath);

            try
            {
                var result = await _freeSwitchService.PlayAudioAsync(_callConnectionId, filePath);
                _log.Info("ESL uuid_broadcast result: {Result}", result);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "ESL uuid_broadcast failed");
            }
        }

        private static void WriteWavHeader(Stream stream, int dataLength, int sampleRate, int channels, int bitsPerSample)
        {
            var byteRate = sampleRate * channels * bitsPerSample / 8;
            var blockAlign = (short)(channels * bitsPerSample / 8);
            using var bw = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
            bw.Write(Encoding.ASCII.GetBytes("RIFF"));
            bw.Write(36 + dataLength);
            bw.Write(Encoding.ASCII.GetBytes("WAVE"));
            bw.Write(Encoding.ASCII.GetBytes("fmt "));
            bw.Write(16);
            bw.Write((short)1);
            bw.Write((short)channels);
            bw.Write(sampleRate);
            bw.Write(byteRate);
            bw.Write(blockAlign);
            bw.Write((short)bitsPerSample);
            bw.Write(Encoding.ASCII.GetBytes("data"));
            bw.Write(dataLength);
        }

        private static bool IsStopAudioMessage(string json)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(json);
                return doc.RootElement.TryGetProperty("kind", out var kind)
                    && kind.GetString() == "StopAudio";
            }
            catch { return false; }
        }

        /// <summary>
        /// Extract raw PCM audio bytes from ACS OutStreamingData JSON format.
        /// Format: {"kind":"AudioData","audioData":{"data":"base64..."},...}
        /// </summary>
        private static byte[]? ExtractAudioFromAcsJson(string json)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty("audioData", out var audioData) &&
                    audioData.TryGetProperty("data", out var data))
                {
                    return Convert.FromBase64String(data.GetString() ?? "");
                }
            }
            catch
            {
                // Not JSON or not ACS format — ignore
            }
            return null;
        }

        public async Task CloseWebSocketAsync(WebSocketReceiveResult result)
        {
            if (_webSocket?.State == WebSocketState.Open)
            {
                await _webSocket.CloseAsync(result.CloseStatus!.Value, result.CloseStatusDescription, CancellationToken.None);
            }
        }

        public async Task CloseNormalWebSocketAsync()
        {
            if (_webSocket?.State == WebSocketState.Open)
            {
                await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Stream completed", CancellationToken.None);
            }
        }

        public void Close()
        {
            _cts.Cancel();
            _cts.Dispose();
        }

        /// <summary>
        /// Receive binary PCM audio from FreeSWITCH WebSocket, resample and forward to AI.
        /// </summary>
        private async Task ReceiveFromFreeSwitchAsync()
        {
            if (_webSocket == null) return;

            try
            {
                _log.Info("Starting to receive audio from FreeSWITCH");
                var buffer = new byte[4096];
                var messageBuffer = new MemoryStream();
                int messageCount = 0;
                int forwardedCount = 0;
                int silenceCount = 0;

                while (_webSocket.State == WebSocketState.Open)
                {
                    var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _log.Info("WebSocket close received after {Count} messages",
                            messageCount);
                        break;
                    }

                    if (result.MessageType == WebSocketMessageType.Binary)
                    {
                        messageBuffer.Write(buffer, 0, result.Count);

                        if (result.EndOfMessage)
                        {
                            messageCount++;
                            var audioData = messageBuffer.ToArray();
                            messageBuffer.SetLength(0);

                            // During AI playback (or the post-playback cooldown), send silence
                            // to OpenAI instead of real mic audio.
                            // mod_audio_stream captures uuid_broadcast audio mixed into READ stream;
                            // sending that would trigger false VAD → phantom transcripts.
                            // The cooldown period covers residual echo that lingers in the READ
                            // stream after PLAYBACK_STOP, and gives input_audio_buffer.clear time
                            // to propagate to the OpenAI server before real audio resumes.
                            var inCooldown = !_isPlayingBack
                                && _playbackEndedAt != DateTime.MinValue
                                && (DateTime.UtcNow - _playbackEndedAt).TotalMilliseconds < PostPlaybackSilenceMs;
                            if (_isPlayingBack || inCooldown)
                            {
                                silenceCount++;
                                using var silenceMs = new MemoryStream(SilenceFrame24k);
                                if (_vlServiceHandler != null)
                                    await _vlServiceHandler.SendAudioToExternalAI(silenceMs);
                                else if (_aiServiceHandler != null)
                                    await _aiServiceHandler.SendAudioToExternalAI(silenceMs);
                                continue;
                            }

                            // Resample 16kHz → 24kHz for OpenAI
                            var resampled = AudioResampler.Upsample16kTo24k(audioData);
                            forwardedCount++;
                            if (forwardedCount == 1 || forwardedCount % 250 == 0)
                            {
                                _log.Info("Audio stats: forwarded={Forwarded}, silence={Silence}",
                                    forwardedCount, silenceCount);
                            }

                            using var ms = new MemoryStream(resampled);
                            if (_vlServiceHandler != null)
                            {
                                await _vlServiceHandler.SendAudioToExternalAI(ms);
                            }
                            else if (_aiServiceHandler != null)
                            {
                                await _aiServiceHandler.SendAudioToExternalAI(ms);
                            }
                        }
                    }
                    else if (result.MessageType == WebSocketMessageType.Text)
                    {
                        // FreeSWITCH might send text messages (events) — log and skip
                        var textData = System.Text.Encoding.UTF8.GetString(buffer, 0, result.Count);
                        _log.Info("Text message received: {Text}",
                            textData.Length > 200 ? textData[..200] : textData);
                    }
                }

                _log.Info("FreeSWITCH receive loop ended. Total messages: {Count}, forwarded: {Forwarded}, silence: {Silence}",
                    messageCount, forwardedCount, silenceCount);
            }
            catch (OperationCanceledException)
            {
                _log.Info("Receive loop cancelled");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error in FreeSWITCH receive loop");
            }
        }

        private async Task InvokeHangUpCallback()
        {
            if (_hangUpCallback != null)
            {
                try
                {
                    await _hangUpCallback(_callConnectionId);
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "Hang-up callback failed");
                }
            }
        }
    }
}
