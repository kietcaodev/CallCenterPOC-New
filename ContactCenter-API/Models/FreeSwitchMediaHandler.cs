using ContactCenterPOC.Hubs;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace ContactCenterPOC.Models
{
    /// <summary>
    /// WebSocket handler for FreeSWITCH audio streaming via mod_audio_fork.
    /// Receives binary PCM 16kHz from FreeSWITCH, resamples to 24kHz, sends to AI engine.
    /// Receives AI audio at 24kHz, downsamples to 16kHz, sends back as binary WebSocket frames.
    /// mod_audio_fork injects the returned audio into the channel's write stream for playback.
    /// Barge-in is handled by sending a {"type":"killAudio"} text frame.
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
        private readonly AzureTtsService? _ttsService;

        // Output sample rate for audio sent BACK to FreeSWITCH via WebSocket binary frames.
        // Must match the FreeSWITCH channel's write_rate:
        //   16000 = SIP wideband (default)
        //   8000  = PSTN/PCMU — mod_audio_fork does NOT resample return audio automatically.
        private readonly int _outputSampleRate;

        // Serialize WebSocket writes (reads and writes are independent but
        // concurrent writes on the same WebSocket are not thread-safe).
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        private volatile bool _bargeIn = false;

        // --- Jitter buffer: smooths OpenAI's bursty audio delivery ---
        // AI sends 400ms chunks unevenly; we slice to 20ms and send at fixed rate.
        private Channel<byte[]>? _audioChannel;
        private Task? _drainerTask;
        private volatile bool _aiIsSpeaking = false;

        // --- Debug counters ---
        private int _totalChunksSent = 0;
        private int _totalBytesSent = 0;
        private int _bargeInSkippedChunks = 0;
        private int _jitterSlicesSent = 0;

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
            FreeSwitchService? freeSwitchService = null,
            AzureTtsService? ttsService = null,
            int outputSampleRate = 16000)
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
            _ttsService = ttsService;
            // 16000 = SIP wideband (send as-is); 8000 = PSTN PCMU (must downsample 16k→8k).
            // mod_audio_fork always forks audio to us at 16kHz (per dialplan '16k'),
            // but it does NOT auto-resample audio we send BACK — we must match channel rate.
            _outputSampleRate = (outputSampleRate == 8000) ? 8000 : 16000;
            _log.Info("Output sample rate: {Rate}Hz", _outputSampleRate);
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
                    _emotionService, _selectedVoice, _ttsService);

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
        /// Receive AI audio, downsample to 16kHz, and send as binary WebSocket frame
        /// back to mod_audio_fork for direct injection into the channel's write stream.
        /// </summary>
        public async Task SendMessageAsync(string acsJsonMessage)
        {
            try
            {
                // Barge-in: send killAudio to clear mod_audio_fork's receive buffer
                if (IsStopAudioMessage(acsJsonMessage))
                {
                    await HandleBargeInAsync();
                    return;
                }

                if (_bargeIn)
                {
                    _bargeInSkippedChunks++;
                    if (_bargeInSkippedChunks == 1 || _bargeInSkippedChunks % 50 == 0)
                        _log.Info("[PLAY] BargeIn active — skipped {SkippedChunks} chunk(s)", _bargeInSkippedChunks);
                    return;
                }

                var audioBytes = ExtractAudioFromAcsJson(acsJsonMessage);
                if (audioBytes != null && audioBytes.Length > 0)
                {
                    // Downsample from 24kHz to target output rate
                    byte[] downsampled;
                    if (_outputSampleRate == 8000)
                    {
                        // PSTN: 24kHz → 8kHz (channel write_rate = PCMU 8kHz)
                        downsampled = AudioResampler.Resample(audioBytes, 24000, 8000);
                    }
                    else
                    {
                        // SIP wideband: 24kHz → 16kHz
                        downsampled = AudioResampler.Downsample24kTo16k(audioBytes);
                    }

                    _totalChunksSent++;
                    _totalBytesSent += downsampled.Length;

                    if (_totalChunksSent <= 10 || _totalChunksSent % 50 == 0)
                    {
                        _log.Info("[PLAY] Enqueue chunk#{Chunk}: 24k={RawB}B→{OutRate}k={DownB}B | total enqueued={TotalKB:F1}KB",
                            _totalChunksSent, audioBytes.Length, _outputSampleRate / 1000, downsampled.Length, _totalBytesSent / 1024.0);
                    }

                    // Write to jitter buffer channel; drainer sends at steady 20ms rate
                    _audioChannel?.Writer.TryWrite(downsampled);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to send audio to FreeSWITCH via WebSocket");
            }
        }

        /// <summary>
        /// Signal end-of-turn: complete the jitter buffer channel and wait for the drainer
        /// to finish flushing all remaining 20ms slices before signalling finished.
        /// </summary>
        public async Task FlushAudioAsync()
        {
            _log.Info("[PLAY] FlushAudioAsync: completing jitter channel. Enqueued chunks={Chunks}, enqueued bytes={TotalKB:F1}KB",
                _totalChunksSent, _totalBytesSent / 1024.0);

            _audioChannel?.Writer.TryComplete();

            if (_drainerTask != null)
            {
                try { await _drainerTask.ConfigureAwait(false); }
                catch (OperationCanceledException) { }
                catch (Exception ex) { _log.Warn(ex, "[PLAY] Drainer task faulted during flush"); }
            }

            _log.Info("[PLAY] FlushAudioAsync done. Jitter slices sent={Slices}", _jitterSlicesSent);
            _audioChannel = null;
            _drainerTask = null;
        }

        /// <summary>
        /// Called by AI service when model starts generating a response.
        /// Reset barge-in flag so new audio is sent.
        /// </summary>
        public void NotifyAiResponseStarted()
        {
            _log.Info("[PLAY] AI response started — resetting bargeIn (was skipping {Skipped} chunks). Creating jitter buffer.",
                _bargeInSkippedChunks);
            _bargeInSkippedChunks = 0;
            _totalChunksSent = 0;
            _totalBytesSent = 0;
            _jitterSlicesSent = 0;
            _bargeIn = false;
            _aiIsSpeaking = true;

            // Create a fresh channel for this response turn
            _audioChannel = Channel.CreateUnbounded<byte[]>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
            _drainerTask = Task.Run(() => SendJitteredAsync(_audioChannel.Reader, _cts.Token));
        }

        public void NotifyAiResponseFinished()
        {
            _aiIsSpeaking = false;
            _log.Info("[PLAY] AI response finished — mic unmuted");
        }

        /// <summary>
        /// Feeds a complete TTS audio blob (raw PCM16 mono at 16 kHz) directly into the
        /// jitter buffer that was started by NotifyAiResponseStarted().
        /// No resampling is applied — bytes go straight into the channel as-is.
        /// The pre-buffer phase will be satisfied immediately since the entire audio is
        /// enqueued at once; the drainer then paces out 20ms slices at the correct rate.
        /// </summary>
        public Task FeedPcm16AudioAsync(byte[] pcm16_16kHz)
        {
            if (_audioChannel == null || pcm16_16kHz == null || pcm16_16kHz.Length == 0)
                return Task.CompletedTask;

            // If PSTN (8kHz output), downsample the TTS audio (16kHz) before feeding the jitter buffer.
            // The jitter buffer slice size is already 320B (8kHz 20ms) for PSTN calls.
            byte[] pcmOut;
            if (_outputSampleRate == 8000)
            {
                pcmOut = AudioResampler.Resample(pcm16_16kHz, 16000, 8000);
            }
            else
            {
                pcmOut = pcm16_16kHz;
            }

            _totalChunksSent++;
            _totalBytesSent += pcmOut.Length;
            _log.Info("[PLAY] [TTS] FeedPcm16: {InBytes}B (16kHz) → {OutBytes}B ({OutRate}Hz, {Ms}ms audio) → jitter buffer",
                pcm16_16kHz.Length, pcmOut.Length, _outputSampleRate, pcmOut.Length / (_outputSampleRate / 1000 * 2));  // bytes ÷ (samples/ms * 2 bytes)

            _audioChannel.Writer.TryWrite(pcmOut);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Handle barge-in: send killAudio to mod_audio_fork to clear its receive buffer
        /// and stop current playback via CF_BREAK.
        /// </summary>
        private async Task HandleBargeInAsync()
        {
            _bargeIn = true;
            _aiIsSpeaking = false; // Unmute mic immediately so user speech is forwarded

            // Stop the jitter buffer drainer; discard remaining buffered audio
            _audioChannel?.Writer.TryComplete();

            // Send killAudio JSON text message to mod_audio_fork
            var killAudioJson = Encoding.UTF8.GetBytes("{\"type\":\"killAudio\"}");

            await _sendLock.WaitAsync(_cts.Token);
            try
            {
                if (_webSocket?.State == WebSocketState.Open)
                {
                    await _webSocket.SendAsync(
                        new ArraySegment<byte>(killAudioJson),
                        WebSocketMessageType.Text,
                        true,
                        _cts.Token);
                }
            }
            finally
            {
                _sendLock.Release();
            }

            _log.Info("[BARGE-IN] Sent killAudio to mod_audio_fork; jitter buffer aborted; mic unmuted");
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
            _sendLock.Dispose();
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

                            // Mute mic while AI is speaking to prevent echo from triggering
                            // false VAD and hallucinated transcriptions in Whisper.
                            if (_aiIsSpeaking)
                            {
                                // Count muted frames but don't forward
                                forwardedCount++;
                                if (forwardedCount <= 5 || forwardedCount % 500 == 0)
                                    _log.Info("[MIC] Muted during AI speech. frameCount={Count}", forwardedCount);
                                continue;
                            }

                            var resampled = AudioResampler.Upsample16kTo24k(audioData);
                            forwardedCount++;

                            // Log first 5 chunks and every 250 thereafter
                            if (forwardedCount <= 5 || forwardedCount % 250 == 0)
                            {
                                _log.Info("[MIC] chunk#{Count}: raw={RawB}B (16kHz) -> resampled={ResampledB}B (24kHz) | forwarded={Fwd}",
                                    messageCount, audioData.Length, resampled.Length, forwardedCount);
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

        /// <summary>
        /// Jitter buffer drainer: reads downsampled 16kHz PCM bytes from the channel,
        /// slices into 640-byte (20ms) pieces and sends each with a fixed 20ms cadence.
        /// When the accumulator runs dry (AI chunk gap), sends PCM silence so mod_audio_fork
        /// never under-flows its playout buffer — eliminating audio stuttering on slow chunks.
        /// </summary>
        private async Task SendJitteredAsync(ChannelReader<byte[]> reader, CancellationToken ct)
        {
            // Slice size: 20ms of PCM16 at the output sample rate.
            //   16kHz: 16000 samples/s × 0.020s × 2 bytes = 640 bytes
            //   8kHz:   8000 samples/s × 0.020s × 2 bytes = 320 bytes
            int kBytesPerSlice = (_outputSampleRate == 8000) ? 320 : 640;
            const int kIntervalMs    = 20;

            // PRE-BUFFER target: accumulate ~2s of audio before starting playback.
            // 16kHz PCM16: 16000*2*2s = 64000B; 8kHz PCM16: 8000*2*2s = 32000B.
            int kPreBufferTargetBytes = kBytesPerSlice * 100;  // 100 slices × 20ms = 2s
            // Max time to wait while pre-buffering (if AI is slow to respond)
            const int kPreBufferTimeoutMs   = 1200;
            // When underrun happens, wait this long for new data before sending silence
            const int kUnderrunWaitMs       = 60;

            var silence = new byte[kBytesPerSlice];
            var acc     = new List<byte>(kBytesPerSlice * 300);
            int accPos  = 0;
            int silencesSent     = 0;
            int totalSilenceMs   = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            void AppendChunk(byte[] chunk)
            {
                if (accPos > kBytesPerSlice * 50) { acc.RemoveRange(0, accPos); accPos = 0; }
                acc.AddRange(chunk);
            }

            try
            {
                // ── PHASE 1: PRE-BUFFER ──────────────────────────────────────────────────
                // Keep pulling chunks until we hit the target or the timeout expires.
                _log.Info("[JITTER] Pre-buffer start: target={TargetKB:F1}KB, timeout={TimeoutMs}ms",
                    kPreBufferTargetBytes / 1024.0, kPreBufferTimeoutMs);

                var preDeadline = DateTime.UtcNow.AddMilliseconds(kPreBufferTimeoutMs);
                int chunksReceived = 0;

                while (!ct.IsCancellationRequested && !_bargeIn)
                {
                    int available = acc.Count - accPos;
                    if (available >= kPreBufferTargetBytes)
                    {
                        _log.Info("[JITTER] Pre-buffer FULL: {Bytes}B in {Chunks} chunk(s) after {ElapsedMs}ms — starting playback",
                            available, chunksReceived, sw.ElapsedMilliseconds);
                        break;
                    }
                    if (DateTime.UtcNow >= preDeadline)
                    {
                        _log.Info("[JITTER] Pre-buffer TIMEOUT after {ElapsedMs}ms: {Bytes}B / {TargetBytes}B in {Chunks} chunk(s) — starting anyway",
                            sw.ElapsedMilliseconds, available, kPreBufferTargetBytes, chunksReceived);
                        break;
                    }
                    if (reader.Completion.IsCompleted)
                    {
                        _log.Info("[JITTER] Pre-buffer: channel completed early with {Bytes}B — starting flush",
                            available);
                        break;
                    }

                    // Wait for next chunk (up to remaining pre-buffer window)
                    var remaining = (int)(preDeadline - DateTime.UtcNow).TotalMilliseconds;
                    if (remaining <= 0) break;
                    using var preCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    preCts.CancelAfter(remaining);
                    try
                    {
                        if (await reader.WaitToReadAsync(preCts.Token))
                        {
                            while (reader.TryRead(out var seed)) { AppendChunk(seed); chunksReceived++; }
                        }
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested) { break; }
                }

                // ── PHASE 2: STEADY DRAIN ────────────────────────────────────────────────
                // Use an adaptive clock: track wall-clock since drain start,
                // compute the ideal next-slice deadline, and sleep only the remainder.
                // This compensates for Task.Delay imprecision (~1-5ms drift per slice on Windows)
                // which otherwise causes audio to play 5-15% slower than real-time.
                var drainSw = System.Diagnostics.Stopwatch.StartNew();
                long drainSliceCount = 0; // number of slices (real + silence) dispatched

                while (!ct.IsCancellationRequested && !_bargeIn)
                {
                    while (reader.TryRead(out var newChunk)) AppendChunk(newChunk);

                    int available = acc.Count - accPos;

                    if (available >= kBytesPerSlice)
                    {
                        if (silencesSent > 0)
                        {
                            _log.Info("[JITTER] Resumed real audio after {Count} silence packet(s) ({Ms}ms gap)",
                                silencesSent, totalSilenceMs);
                            silencesSent   = 0;
                            totalSilenceMs = 0;
                        }

                        var slice = new byte[kBytesPerSlice];
                        acc.CopyTo(accPos, slice, 0, kBytesPerSlice);
                        accPos += kBytesPerSlice;

                        _jitterSlicesSent++;
                        if (_jitterSlicesSent <= 10 || _jitterSlicesSent % 100 == 0)
                            _log.Info("[JITTER] slice#{Slice}: bufferAhead={AheadKB:F1}KB remaining",
                                _jitterSlicesSent, (acc.Count - accPos) / 1024.0);

                        await SendBinaryDirectAsync(slice, ct);

                        // Adaptive sleep: sleep only what is left until the next 20ms boundary
                        drainSliceCount++;
                        long expectedMs = drainSliceCount * kIntervalMs;
                        long actualMs   = drainSw.ElapsedMilliseconds;
                        int  delayMs    = (int)(expectedMs - actualMs);
                        if (delayMs > 1)
                            await Task.Delay(delayMs, ct);
                    }
                    else if (reader.Completion.IsCompleted)
                    {
                        if (available > 0 && !_bargeIn)
                        {
                            var tail = acc.Skip(accPos).ToArray();
                            _log.Info("[JITTER] Flushing tail {Bytes}B", tail.Length);
                            await SendBinaryDirectAsync(tail, ct);
                        }
                        break;
                    }
                    else
                    {
                        // Underrun: wait a bit for data before sending silence
                        using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        waitCts.CancelAfter(kUnderrunWaitMs);
                        try { await reader.WaitToReadAsync(waitCts.Token); }
                        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { }

                        // Check again after waiting
                        while (reader.TryRead(out var lateChunk)) AppendChunk(lateChunk);
                        if (acc.Count - accPos >= kBytesPerSlice) continue; // got data, no silence needed

                        // Still empty → send silence
                        silencesSent++;
                        totalSilenceMs += kIntervalMs;
                        if (silencesSent == 1 || silencesSent % 10 == 0)
                            _log.Warn("[JITTER] Underrun #{Count} ({TotalMs}ms gap) — sending silence; elapsed={ElapsedMs}ms",
                                silencesSent, totalSilenceMs, sw.ElapsedMilliseconds);

                        await SendBinaryDirectAsync(silence, ct);

                        // Adaptive sleep for silence slices too
                        drainSliceCount++;
                        long expectedMsS = drainSliceCount * kIntervalMs;
                        long actualMsS   = drainSw.ElapsedMilliseconds;
                        int  delaySMs    = (int)(expectedMsS - actualMsS);
                        if (delaySMs > 1)
                            await Task.Delay(delaySMs, ct);
                    }
                }

                long expectedTotalMs = drainSliceCount * kIntervalMs;
                long actualTotalMs   = drainSw.ElapsedMilliseconds;
                long driftMs         = actualTotalMs - expectedTotalMs;
                _log.Info("[JITTER] Drainer complete: slices={Slices}, barge-in={BargeIn}, total-silence={SilenceMs}ms, elapsed={ElapsedMs}ms, drift={DriftMs}ms",
                    _jitterSlicesSent, _bargeIn, totalSilenceMs, sw.ElapsedMilliseconds, driftMs);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.Error(ex, "[JITTER] Drainer exception");
            }
        }

        /// <summary>
        /// Low-level binary send to WebSocket (bypasses jitter queue, thread-safe via _sendLock).
        /// </summary>
        private async Task SendBinaryDirectAsync(byte[] data, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct);
            try
            {
                if (_webSocket?.State == WebSocketState.Open)
                {
                    var sendStart = Stopwatch.GetTimestamp();
                    await _webSocket.SendAsync(new ArraySegment<byte>(data), WebSocketMessageType.Binary, true, ct);
                    var sendMs = (Stopwatch.GetTimestamp() - sendStart) * 1000.0 / Stopwatch.Frequency;
                    if (sendMs > 30)
                        _log.Warn("[JITTER] SLOW WebSocket send: {SendMs:F1}ms for {Bytes}B slice", sendMs, data.Length);
                }
            }
            finally
            {
                _sendLock.Release();
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
