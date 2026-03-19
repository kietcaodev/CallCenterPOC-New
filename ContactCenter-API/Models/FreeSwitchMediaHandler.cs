using ContactCenterPOC.Hubs;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace ContactCenterPOC.Models
{
    /// <summary>
    /// WebSocket handler for FreeSWITCH audio streaming via mod_audio_stream.
    /// Receives binary PCM 16kHz from FreeSWITCH, resamples to 24kHz, sends to OpenAI/VoiceLive.
    /// Receives AI audio at 24kHz, writes proper WAV files and plays via ESL uuid_broadcast.
    /// Audio is kept at 24kHz (no downsampling) — FreeSWITCH handles resampling to channel rate
    /// with its built-in high-quality resampler, avoiding quality loss from linear interpolation.
    /// </summary>
    public class FreeSwitchMediaHandler : IMediaStreamingHandler
    {
        private WebSocket _webSocket;
        private CancellationTokenSource _cts;
        private AzureOpenAIService? _aiServiceHandler;
        private VoiceLiveService? _vlServiceHandler;
        private GeminiLiveService? _geminiServiceHandler;
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
        private readonly GeminiLiveConfig? _geminiLiveConfig;
        private readonly string? _geminiLiveVoice;
        private readonly FreeSwitchService? _freeSwitchService;

        // Serialize WebSocket writes (reads and writes are independent but
        // concurrent writes on the same WebSocket are not thread-safe).
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        // --- Buffered playback via ESL uuid_broadcast ---
        private readonly object _bufferLock = new();
        private MemoryStream _audioBuffer = new();
        private readonly Channel<string> _playbackQueue = Channel.CreateUnbounded<string>();
        private Task? _playbackTask;
        private volatile bool _bargeIn = false;
        private int _segmentCounter = 0;
        private Timer? _flushTimer;
        // Flush buffered audio every 300ms — balances latency vs segment count.
        // Larger segments = fewer transitions = smoother playback.
        private const int FlushIntervalMs = 300;
        // Immediate flush if buffer exceeds ~2 seconds of 24kHz PCM16 mono (96000 = 2s * 24000 * 2)
        private const int MaxBufferBytes = 96000;
        // Bytes per ms for 24kHz 16-bit mono: 24000 samples/sec * 2 bytes = 48 bytes/ms
        private const double BytesPerMs24k = 48.0;
        // WAV header size
        private const int WavHeaderSize = 44;

        // --- Debug timing ---
        private DateTime _firstChunkTime = DateTime.MinValue;
        private DateTime _lastChunkTime = DateTime.MinValue;
        private int _totalChunksReceived = 0;
        private int _totalBytesBuffered = 0;
        private int _queuedSegments = 0;
        private int _playedSegments = 0;

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
            GeminiLiveConfig? geminiLiveConfig = null,
            string? geminiLiveVoice = null,
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
            _geminiLiveConfig = geminiLiveConfig;
            _geminiLiveVoice = geminiLiveVoice;
            _freeSwitchService = freeSwitchService;
        }

        /// <summary>
        /// Main processing loop: connect to AI service, receive binary audio and relay.
        /// </summary>
        public async Task ProcessWebSocketAsync(string callContextPrompt)
        {
            if (_webSocket == null) return;

            // Start background playback queue processor (plays temp audio files via ESL)
            _playbackTask = Task.Run(() => ProcessPlaybackQueueAsync());

            // Initialize flush timer (armed on first audio byte, fires every 150ms)
            _flushTimer = new Timer(FlushTimerCallback, null, Timeout.Infinite, Timeout.Infinite);

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
            else if (_voiceApiMode == "GeminiLive" && _geminiLiveConfig != null && _geminiLiveConfig.IsConfigured)
            {
                _log.Info("Dispatching to GeminiLiveService (voice={Voice})",
                    _geminiLiveVoice);
                _geminiServiceHandler = new GeminiLiveService(
                    this, callContextPrompt, _geminiLiveConfig, _logger, _hubContext,
                    _callConnectionId, _geminiLiveVoice,
                    _hangUpCallback, _sentimentService, _activeCalls, _emotionService);

                try
                {
                    _geminiServiceHandler.StartConversation();
                    await ReceiveFromFreeSwitchAsync();
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "GeminiLive WebSocket error");
                }
                finally
                {
                    _geminiServiceHandler.Close();
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
        /// Buffer AI audio chunks and queue them for playback via ESL uuid_broadcast.
        /// Uses timer-based flushing (150ms) for low-latency streaming.
        /// </summary>
        public async Task SendMessageAsync(string acsJsonMessage)
        {
            try
            {
                // Barge-in: stop current playback and clear queue via ESL uuid_break
                if (IsStopAudioMessage(acsJsonMessage))
                {
                    await HandleBargeInAsync();
                    return;
                }

                var audioBytes = ExtractAudioFromAcsJson(acsJsonMessage);
                if (audioBytes != null && audioBytes.Length > 0)
                {
                    var now = DateTime.UtcNow;
                    _totalChunksReceived++;
                    if (_firstChunkTime == DateTime.MinValue) _firstChunkTime = now;
                    var sinceLastChunk = _lastChunkTime == DateTime.MinValue ? 0 : (now - _lastChunkTime).TotalMilliseconds;
                    _lastChunkTime = now;

                    // Keep original 24kHz audio — let FreeSWITCH resample with its built-in
                    // high-quality resampler instead of our low-quality linear interpolation.
                    var audioMs = audioBytes.Length / BytesPerMs24k;

                    _log.Info("[PIPE] Chunk #{Num}: {Bytes}B@24k ({AudioMs:F0}ms audio), gap={Gap:F0}ms, queued={Queued}, played={Played}",
                        _totalChunksReceived, audioBytes.Length, audioMs,
                        sinceLastChunk, _queuedSegments - _playedSegments, _playedSegments);

                    lock (_bufferLock)
                    {
                        _audioBuffer.Write(audioBytes, 0, audioBytes.Length);
                        _totalBytesBuffered += audioBytes.Length;

                        // Immediate flush for large buffers (>2s of audio)
                        if (_audioBuffer.Length >= MaxBufferBytes)
                        {
                            _log.Info("[PIPE] Flushing buffer (max threshold): {BufLen}B ({BufMs:F0}ms audio)",
                                _audioBuffer.Length, _audioBuffer.Length / BytesPerMs24k);
                            FlushBufferToQueue();
                        }
                        else
                        {
                            // Arm the flush timer — fires after 150ms to collect nearby chunks
                            _flushTimer?.Change(FlushIntervalMs, Timeout.Infinite);
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to process audio for FreeSWITCH playback");
            }
        }

        /// <summary>
        /// Timer callback: flush whatever audio has accumulated in the buffer.
        /// </summary>
        private void FlushTimerCallback(object? state)
        {
            lock (_bufferLock)
            {
                if (_audioBuffer.Length > 0)
                {
                    _log.Info("[PIPE] Flushing buffer (timer {TimerMs}ms): {BufLen}B ({BufMs:F0}ms audio)",
                        FlushIntervalMs, _audioBuffer.Length, _audioBuffer.Length / BytesPerMs24k);
                    FlushBufferToQueue();
                }
            }
        }

        /// <summary>
        /// Flush any remaining buffered audio to a temp file and enqueue for playback.
        /// Called by AI service when model finishes generating audio for the current turn.
        /// </summary>
        public Task FlushAudioAsync()
        {
            // Disarm timer — AI turn is done, flush immediately
            _flushTimer?.Change(Timeout.Infinite, Timeout.Infinite);
            lock (_bufferLock)
            {
                if (_audioBuffer.Length > 0)
                {
                    _log.Info("[PIPE] Flushing buffer (AI turn done): {BufLen}B ({BufMs:F0}ms audio)",
                        _audioBuffer.Length, _audioBuffer.Length / BytesPerMs24k);
                    FlushBufferToQueue();
                }
            }
            _log.Info("[PIPE] AI turn complete. Total chunks={Chunks}, totalBuffered={TotalBytes}B, segments queued={Queued}, played={Played}",
                _totalChunksReceived, _totalBytesBuffered, _queuedSegments, _playedSegments);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called by AI service when model starts generating a response.
        /// Reset barge-in flag so new audio segments are played.
        /// </summary>
        public void NotifyAiResponseStarted()
        {
            _bargeIn = false;
        }

        /// <summary>
        /// Called by AI service when model finishes generating a response.
        /// </summary>
        public void NotifyAiResponseFinished() { }

        /// <summary>
        /// Write buffered 24kHz audio to a temp WAV file and enqueue for ESL playback.
        /// WAV format ensures FreeSWITCH correctly knows the sample rate (24kHz, 16-bit, mono).
        /// Must be called under _bufferLock.
        /// </summary>
        private void FlushBufferToQueue()
        {
            var data = _audioBuffer.ToArray();
            _audioBuffer.SetLength(0);

            if (data.Length == 0) return;

            var segNum = Interlocked.Increment(ref _segmentCounter);
            var tempDir = Path.GetTempPath();
            var fileName = Path.Combine(tempDir, $"{_callConnectionId}_{segNum}.wav");

            try
            {
                var writeStart = DateTime.UtcNow;
                WriteWavFile(fileName, data, 24000);
                var writeMs = (DateTime.UtcNow - writeStart).TotalMilliseconds;
                Interlocked.Increment(ref _queuedSegments);
                _playbackQueue.Writer.TryWrite(fileName);
                _log.Info("[PIPE] Seg#{SegNum} queued: {Bytes}B ({AudioMs:F0}ms audio), file write={WriteMs:F1}ms, pending={Pending}",
                    segNum, data.Length, data.Length / BytesPerMs24k, writeMs, _queuedSegments - _playedSegments);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to write audio segment to {File}", fileName);
            }
        }

        /// <summary>
        /// Write a WAV file with proper RIFF/WAVE header.
        /// Ensures FreeSWITCH correctly interprets sample rate, bit depth, and channels.
        /// </summary>
        private static void WriteWavFile(string path, byte[] pcmData, int sampleRate, int bitsPerSample = 16, int channels = 1)
        {
            int byteRate = sampleRate * channels * bitsPerSample / 8;
            int blockAlign = channels * bitsPerSample / 8;
            int dataSize = pcmData.Length;

            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
            using var bw = new BinaryWriter(fs);

            // RIFF header
            bw.Write(Encoding.ASCII.GetBytes("RIFF"));
            bw.Write(36 + dataSize); // file size - 8
            bw.Write(Encoding.ASCII.GetBytes("WAVE"));

            // fmt chunk
            bw.Write(Encoding.ASCII.GetBytes("fmt "));
            bw.Write(16);                   // chunk size
            bw.Write((short)1);             // PCM format
            bw.Write((short)channels);
            bw.Write(sampleRate);
            bw.Write(byteRate);
            bw.Write((short)blockAlign);
            bw.Write((short)bitsPerSample);

            // data chunk
            bw.Write(Encoding.ASCII.GetBytes("data"));
            bw.Write(dataSize);
            bw.Write(pcmData);
        }

        /// <summary>
        /// Background task: sequentially play queued audio segments via ESL uuid_broadcast.
        /// </summary>
        private async Task ProcessPlaybackQueueAsync()
        {
            try
            {
                await foreach (var filePath in _playbackQueue.Reader.ReadAllAsync(_cts.Token))
                {
                    if (_bargeIn)
                    {
                        _log.Info("[PLAY] Skipping {File} (barge-in)", Path.GetFileName(filePath));
                        TryDeleteFile(filePath);
                        continue;
                    }

                    try
                    {
                        if (_freeSwitchService != null)
                        {
                            var fileLen = new FileInfo(filePath).Length;
                            // WAV file: subtract 44-byte header to get PCM data length
                            var dataLen = Math.Max(0, fileLen - WavHeaderSize);
                            var durationMs = (int)(dataLen / BytesPerMs24k);
                            var playStart = DateTime.UtcNow;

                            _log.Info("[PLAY] Starting seg {File}: {Bytes}B ({DurMs}ms audio), pending after this={Pending}",
                                Path.GetFileName(filePath), dataLen, durationMs, _queuedSegments - _playedSegments - 1);

                            await _freeSwitchService.PlayAudioAsync(_callConnectionId, filePath);
                            var eslMs = (DateTime.UtcNow - playStart).TotalMilliseconds;
                            _log.Info("[PLAY] ESL command took {EslMs:F0}ms for {File}", eslMs, Path.GetFileName(filePath));

                            // Wait the audio duration. Add small padding (20ms) to avoid
                            // cutting off the tail when uuid_broadcast starts the next segment.
                            if (durationMs > 0)
                            {
                                await Task.Delay(durationMs + 20, _cts.Token);
                            }

                            var totalPlayMs = (DateTime.UtcNow - playStart).TotalMilliseconds;
                            Interlocked.Increment(ref _playedSegments);
                            _log.Info("[PLAY] Finished seg {File}: totalWait={TotalMs:F0}ms (audio={DurMs}ms), played={Played}",
                                Path.GetFileName(filePath), totalPlayMs, durationMs, _playedSegments);
                        }
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        _log.Warn(ex, "Playback failed for {File}", filePath);
                    }
                    finally
                    {
                        TryDeleteFile(filePath);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.Error(ex, "Playback queue processor error");
            }
        }

        /// <summary>
        /// Handle barge-in: stop current playback, clear buffer and queue.
        /// Uses ESL uuid_break instead of WebSocket clearAudio (not supported by open-source mod_audio_stream).
        /// </summary>
        private async Task HandleBargeInAsync()
        {
            _bargeIn = true;

            // Disarm flush timer
            _flushTimer?.Change(Timeout.Infinite, Timeout.Infinite);

            // Clear audio buffer
            lock (_bufferLock)
            {
                _audioBuffer.SetLength(0);
            }

            // Drain queued files
            while (_playbackQueue.Reader.TryRead(out var filePath))
            {
                TryDeleteFile(filePath);
            }

            // Stop current playback via ESL
            if (_freeSwitchService != null)
            {
                try
                {
                    await _freeSwitchService.BreakAudioAsync(_callConnectionId);
                }
                catch (Exception ex)
                {
                    _log.Warn(ex, "Barge-in ESL uuid_break failed");
                }
            }

            _log.Info("Barge-in: cleared queue and stopped playback via ESL");
        }

        private static void TryDeleteFile(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { /* best-effort cleanup */ }
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

            // Stop flush timer
            _flushTimer?.Dispose();

            // Complete playback queue and wait for processor to finish
            _playbackQueue.Writer.TryComplete();
            try { _playbackTask?.Wait(TimeSpan.FromSeconds(2)); } catch { /* ignore */ }

            // Clean up any remaining queued files
            while (_playbackQueue.Reader.TryRead(out var filePath))
            {
                TryDeleteFile(filePath);
            }

            _cts.Dispose();
            _sendLock.Dispose();
            _audioBuffer.Dispose();
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

                            // Always forward real mic audio to OpenAI — full-duplex mode.
                            // User can speak while bot is playing. OpenAI server-side VAD
                            // handles barge-in detection. Echo from telephone speaker→mic
                            // coupling may cause Whisper hallucinations, which are filtered
                            // at the transcript level (see WhisperHallucinationFilter).
                            var resampled = AudioResampler.Upsample16kTo24k(audioData);
                            forwardedCount++;
                            if (forwardedCount == 1 || forwardedCount % 250 == 0)
                            {
                                _log.Info("Audio stats: forwarded={Forwarded}",
                                    forwardedCount);
                            }

                            using var ms = new MemoryStream(resampled);
                            if (_vlServiceHandler != null)
                            {
                                await _vlServiceHandler.SendAudioToExternalAI(ms);
                            }
                            else if (_geminiServiceHandler != null)
                            {
                                await _geminiServiceHandler.SendAudioToExternalAI(ms);
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

                _log.Info("FreeSWITCH receive loop ended. Total messages: {Count}, forwarded: {Forwarded}",
                    messageCount, forwardedCount);
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
