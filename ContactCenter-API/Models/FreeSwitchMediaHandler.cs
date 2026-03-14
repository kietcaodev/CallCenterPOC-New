using ContactCenterPOC.Hubs;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Net.WebSockets;

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

        // Audio format: FreeSWITCH sends 16kHz, OpenAI expects 24kHz
        private const int FreeSwitchSampleRate = 16000;
        private const int OpenAiSampleRate = 24000;

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
            VoiceLiveConfig? voiceLiveConfig = null)
        {
            _webSocket = webSocket;
            _configuration = configuration;
            _cts = new CancellationTokenSource();
            _logger = logger;
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
        }

        /// <summary>
        /// Main processing loop: connect to AI service, receive binary audio and relay.
        /// </summary>
        public async Task ProcessWebSocketAsync(string callContextPrompt)
        {
            if (_webSocket == null) return;

            if (_voiceApiMode == "VoiceLive" && _voiceLiveConfig != null && _voiceLiveConfig.IsConfigured)
            {
                _logger.LogInformation("[FS-{CallId}] Dispatching to VoiceLiveService", _callConnectionId);
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
                    _logger.LogError(ex, "[FS-{CallId}] VoiceLive WebSocket error", _callConnectionId);
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
                _logger.LogInformation("[FS-{CallId}] Dispatching to AzureOpenAIService (voice={Voice})",
                    _callConnectionId, _selectedVoice);
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
                    _logger.LogError(ex, "[FS-{CallId}] OpenAI WebSocket error", _callConnectionId);
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
        /// Send resampled audio message back to FreeSWITCH via WebSocket.
        /// mod_audio_stream expects a JSON text frame with type "streamAudio" and base64-encoded PCM.
        /// Called by AI services that produce 24kHz PCM wrapped in ACS JSON format.
        /// We intercept, extract audio, resample, and send as JSON text.
        /// </summary>
        public async Task SendMessageAsync(string acsJsonMessage)
        {
            if (_webSocket?.State != WebSocketState.Open) return;

            try
            {
                // The AI services use OutStreamingData.GetAudioDataForOutbound() which produces
                // an ACS-format JSON with base64 audio. We need to extract and resample.
                var audioBytes = ExtractAudioFromAcsJson(acsJsonMessage);
                if (audioBytes != null && audioBytes.Length > 0)
                {
                    // Resample 24kHz → 16kHz for FreeSWITCH
                    var resampled = AudioResampler.Downsample24kTo16k(audioBytes);

                    // mod_audio_stream expects JSON text: {"type":"streamAudio","data":{"audioDataType":"raw","sampleRate":16000,"audioData":"<base64>"}}
                    var base64Audio = Convert.ToBase64String(resampled);
                    var json = $"{{\"type\":\"streamAudio\",\"data\":{{\"audioDataType\":\"raw\",\"sampleRate\":16000,\"audioData\":\"{base64Audio}\"}}}}";
                    var jsonBytes = System.Text.Encoding.UTF8.GetBytes(json);

                    await _webSocket.SendAsync(
                        new ArraySegment<byte>(jsonBytes),
                        WebSocketMessageType.Text,
                        endOfMessage: true,
                        CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FS-{CallId}] Failed to send audio to FreeSWITCH", _callConnectionId);
            }
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
                _logger.LogInformation("[FS-{CallId}] Starting to receive audio from FreeSWITCH", _callConnectionId);
                var buffer = new byte[4096];
                var messageBuffer = new MemoryStream();
                int messageCount = 0;

                while (_webSocket.State == WebSocketState.Open)
                {
                    var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("[FS-{CallId}] WebSocket close received after {Count} messages",
                            _callConnectionId, messageCount);
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

                            if (messageCount <= 3 || messageCount % 100 == 0)
                            {
                                _logger.LogInformation("[FS-{CallId}] Audio message #{Count} ({Bytes} bytes)",
                                    _callConnectionId, messageCount, audioData.Length);
                            }

                            // Resample 16kHz → 24kHz for OpenAI
                            var resampled = AudioResampler.Upsample16kTo24k(audioData);

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
                        _logger.LogInformation("[FS-{CallId}] Text message received: {Text}",
                            _callConnectionId, textData.Length > 200 ? textData[..200] : textData);
                    }
                }

                _logger.LogInformation("[FS-{CallId}] FreeSWITCH receive loop ended. Total messages: {Count}",
                    _callConnectionId, messageCount);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("[FS-{CallId}] Receive loop cancelled", _callConnectionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FS-{CallId}] Error in FreeSWITCH receive loop", _callConnectionId);
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
                    _logger.LogError(ex, "[FS-{CallId}] Hang-up callback failed", _callConnectionId);
                }
            }
        }
    }
}
