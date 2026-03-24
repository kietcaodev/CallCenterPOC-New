using ContactCenterPOC.Hubs;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

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

        // Serialize WebSocket writes (reads and writes are independent but
        // concurrent writes on the same WebSocket are not thread-safe).
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        private volatile bool _bargeIn = false;

        // --- Debug counters ---
        private int _totalChunksSent = 0;
        private int _totalBytesSent = 0;

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

                if (_bargeIn) return; // Skip audio while barge-in is active

                var audioBytes = ExtractAudioFromAcsJson(acsJsonMessage);
                if (audioBytes != null && audioBytes.Length > 0)
                {
                    // Downsample from 24kHz to 16kHz to match mod_audio_fork's desired sampling rate
                    var downsampled = AudioResampler.Downsample24kTo16k(audioBytes);

                    await _sendLock.WaitAsync(_cts.Token);
                    try
                    {
                        if (_webSocket?.State == WebSocketState.Open)
                        {
                            await _webSocket.SendAsync(
                                new ArraySegment<byte>(downsampled),
                                WebSocketMessageType.Binary,
                                true,
                                _cts.Token);

                            _totalChunksSent++;
                            _totalBytesSent += downsampled.Length;

                            if (_totalChunksSent == 1 || _totalChunksSent % 100 == 0)
                            {
                                _log.Info("[PLAY] Sent {Chunks} audio chunks ({TotalKB:F1}KB) to mod_audio_fork",
                                    _totalChunksSent, _totalBytesSent / 1024.0);
                            }
                        }
                    }
                    finally
                    {
                        _sendLock.Release();
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to send audio to FreeSWITCH via WebSocket");
            }
        }

        /// <summary>
        /// Flush any remaining audio. With direct binary streaming, this is a no-op
        /// since each chunk is sent immediately.
        /// </summary>
        public Task FlushAudioAsync()
        {
            _log.Info("[PLAY] AI turn complete. Total chunks sent={Chunks}, totalBytes={TotalKB:F1}KB",
                _totalChunksSent, _totalBytesSent / 1024.0);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called by AI service when model starts generating a response.
        /// Reset barge-in flag so new audio is sent.
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
        /// Handle barge-in: send killAudio to mod_audio_fork to clear its receive buffer
        /// and stop current playback via CF_BREAK.
        /// </summary>
        private async Task HandleBargeInAsync()
        {
            _bargeIn = true;

            // Send killAudio JSON text message to mod_audio_fork
            var killAudioJson = "{\"type\":\"killAudio\"}"u8;

            await _sendLock.WaitAsync(_cts.Token);
            try
            {
                if (_webSocket?.State == WebSocketState.Open)
                {
                    await _webSocket.SendAsync(
                        new ArraySegment<byte>(killAudioJson.ToArray()),
                        WebSocketMessageType.Text,
                        true,
                        _cts.Token);
                }
            }
            finally
            {
                _sendLock.Release();
            }

            _log.Info("Barge-in: sent killAudio to mod_audio_fork");
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

                            // Always forward real mic audio to AI — full-duplex mode.
                            // User can speak while bot is playing. AI server-side VAD
                            // handles barge-in detection.
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
