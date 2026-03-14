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

        // Buffer to accumulate audio chunks per AI turn, then play via ESL
        private readonly MemoryStream _audioBuffer = new();
        private int _turnCounter;

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
        /// Buffer resampled audio from AI for later playback via ESL uuid_broadcast.
        /// Called by AI services per audio chunk. Audio is accumulated until FlushAudioAsync.
        /// </summary>
        public async Task SendMessageAsync(string acsJsonMessage)
        {
            try
            {
                var audioBytes = ExtractAudioFromAcsJson(acsJsonMessage);
                if (audioBytes != null && audioBytes.Length > 0)
                {
                    // Resample 24kHz → 16kHz for FreeSWITCH
                    var resampled = AudioResampler.Downsample24kTo16k(audioBytes);
                    _audioBuffer.Write(resampled, 0, resampled.Length);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FS-{CallId}] Failed to buffer audio", _callConnectionId);
            }
            await Task.CompletedTask;
        }

        /// <summary>
        /// Write buffered audio to a WAV file and play it back to the caller via ESL uuid_broadcast.
        /// Called when AI finishes generating one turn of audio.
        /// </summary>
        public async Task FlushAudioAsync()
        {
            if (_audioBuffer.Length == 0) return;

            var pcmData = _audioBuffer.ToArray();
            _audioBuffer.SetLength(0);
            _turnCounter++;

            // Write WAV file to /tmp/
            var fileName = $"ai_{_callConnectionId}_{_turnCounter}.wav";
            var filePath = Path.Combine(Path.GetTempPath(), fileName);

            try
            {
                await using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write))
                {
                    WriteWavHeader(fs, pcmData.Length, 16000, 1, 16);
                    await fs.WriteAsync(pcmData);
                }

                _logger.LogInformation("[FS-{CallId}] Wrote audio turn #{Turn} ({Bytes} bytes) to {Path}",
                    _callConnectionId, _turnCounter, pcmData.Length, filePath);

                // Play via ESL uuid_broadcast
                await EslBroadcastAsync(filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FS-{CallId}] Failed to flush audio turn #{Turn}", _callConnectionId, _turnCounter);
            }
        }

        /// <summary>
        /// Play audio file on the call via FreeSwitchService ESL connection.
        /// </summary>
        private async Task EslBroadcastAsync(string filePath)
        {
            if (_freeSwitchService == null || !_freeSwitchService.IsConnected)
            {
                _logger.LogWarning("[FS-{CallId}] FreeSwitchService not available, cannot play audio", _callConnectionId);
                return;
            }

            try
            {
                _logger.LogInformation("[FS-{CallId}] ESL: uuid_broadcast {UUID} {Path} aleg",
                    _callConnectionId, _callConnectionId, filePath);
                await _freeSwitchService.PlayAudioAsync(_callConnectionId, filePath);
                _logger.LogInformation("[FS-{CallId}] ESL uuid_broadcast sent successfully", _callConnectionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FS-{CallId}] ESL uuid_broadcast failed", _callConnectionId);
            }
        }

        /// <summary>
        /// Write a standard PCM WAV header.
        /// </summary>
        private static void WriteWavHeader(Stream stream, int dataLength, int sampleRate, int channels, int bitsPerSample)
        {
            var byteRate = sampleRate * channels * bitsPerSample / 8;
            var blockAlign = (short)(channels * bitsPerSample / 8);
            using var bw = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
            bw.Write(Encoding.ASCII.GetBytes("RIFF"));
            bw.Write(36 + dataLength); // file size - 8
            bw.Write(Encoding.ASCII.GetBytes("WAVE"));
            bw.Write(Encoding.ASCII.GetBytes("fmt "));
            bw.Write(16); // PCM chunk size
            bw.Write((short)1); // PCM format
            bw.Write((short)channels);
            bw.Write(sampleRate);
            bw.Write(byteRate);
            bw.Write(blockAlign);
            bw.Write((short)bitsPerSample);
            bw.Write(Encoding.ASCII.GetBytes("data"));
            bw.Write(dataLength);
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
