using ContactCenterPOC.Hubs;
using ContactCenterPOC.Models;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace ContactCenterPOC.Services
{
    /// <summary>
    /// Gemini Live API service — connects to Google Gemini via raw WebSocket.
    /// Protocol: wss://generativelanguage.googleapis.com
    /// Input audio: PCM 16-bit 24kHz (resampled from 16kHz FreeSWITCH, base64 via JSON)
    /// Output audio: PCM 16-bit 24kHz (base64 via JSON)
    /// </summary>
    public class GeminiLiveService
    {
        private CancellationTokenSource _cts;
        private ClientWebSocket? _webSocket;
        private IMediaStreamingHandler _mediaStreaming;
        private readonly ILogger<CallService> _logger;
        private readonly CallLogger _log;
        private readonly IHubContext<TranscriptHub> _hubContext;
        private readonly string _callConnectionId;
        private readonly Func<string, Task>? _hangUpCallback;
        private readonly SentimentAnalysisService? _sentimentService;
        private readonly EmotionAnalysisService? _emotionService;
        private readonly ConcurrentDictionary<string, ActiveCall>? _activeCalls;
        private readonly GeminiLiveConfig _config;
        private readonly string _prompt;
        private readonly string _voice;
        private bool _sessionReady = false;
        private int _audioChunkCount = 0;

        // Gemini Live protocol constants
        private const string GeminiWssBaseUrl = "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent";

        public GeminiLiveService(
            IMediaStreamingHandler mediaStreaming,
            string prompt,
            GeminiLiveConfig config,
            ILogger<CallService> logger,
            IHubContext<TranscriptHub> hubContext,
            string callConnectionId,
            string? voice = null,
            Func<string, Task>? hangUpCallback = null,
            SentimentAnalysisService? sentimentService = null,
            ConcurrentDictionary<string, ActiveCall>? activeCalls = null,
            EmotionAnalysisService? emotionService = null)
        {
            _mediaStreaming = mediaStreaming;
            _cts = new CancellationTokenSource();
            _logger = logger;
            _log = new CallLogger(logger, callConnectionId, "Gemini");
            _hubContext = hubContext;
            _callConnectionId = callConnectionId;
            _hangUpCallback = hangUpCallback;
            _sentimentService = sentimentService;
            _emotionService = emotionService;
            _activeCalls = activeCalls;
            _config = config;
            _prompt = prompt;
            _voice = voice ?? config.Voice ?? "Puck";

            _log.Info("GeminiLiveService created (model={Model}, voice={Voice})", config.Model, _voice);
        }

        /// <summary>
        /// Connect to Gemini Live API and send setup message.
        /// </summary>
        private async Task ConnectAsync()
        {
            _webSocket = new ClientWebSocket();
            var uri = new Uri($"{GeminiWssBaseUrl}?key={_config.ApiKey}");

            _log.Info("Connecting to Gemini Live API (model={Model})...", _config.Model);
            await _webSocket.ConnectAsync(uri, _cts.Token);
            _log.Info("WebSocket connected to Gemini");

            // Send setup message
            var setupMessage = new
            {
                setup = new
                {
                    model = $"models/{_config.Model}",
                    generationConfig = new
                    {
                        responseModalities = new[] { "AUDIO" },
                        speechConfig = new
                        {
                            voiceConfig = new
                            {
                                prebuiltVoiceConfig = new
                                {
                                    voiceName = _voice
                                }
                            }
                        }
                    },
                    systemInstruction = new
                    {
                        parts = new[] { new { text = _prompt } }
                    },
                    realtimeInputConfig = new
                    {
                        automaticActivityDetection = new
                        {
                            disabled = false
                        }
                    },
                    inputAudioTranscription = new { },
                    outputAudioTranscription = new { }
                }
            };

            var setupJson = JsonSerializer.Serialize(setupMessage);
            await SendTextAsync(setupJson);
            _log.Info("Setup message sent ({Len} bytes)", setupJson.Length);
        }

        /// <summary>
        /// Send a JSON text message to Gemini.
        /// </summary>
        private async Task SendTextAsync(string json)
        {
            if (_webSocket?.State != WebSocketState.Open) return;

            var bytes = Encoding.UTF8.GetBytes(json);
            await _webSocket.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                true,
                _cts.Token);
        }

        /// <summary>
        /// Send PCM audio (already at 24kHz) to Gemini as base64 realtime input.
        /// </summary>
        public async Task SendAudioToExternalAI(MemoryStream audioStream)
        {
            if (_webSocket?.State != WebSocketState.Open || !_sessionReady) return;

            try
            {
                var audioBytes = audioStream.ToArray();
                var base64Audio = Convert.ToBase64String(audioBytes);

                var message = new
                {
                    realtimeInput = new
                    {
                        audio = new
                        {
                            mimeType = "audio/pcm;rate=24000",
                            data = base64Audio
                        }
                    }
                };

                var json = JsonSerializer.Serialize(message);
                await SendTextAsync(json);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to send audio to Gemini");
            }
        }

        /// <summary>
        /// Main receive loop — process messages from Gemini.
        /// </summary>
        private async Task ReceiveFromGeminiAsync()
        {
            if (_webSocket == null) return;

            var buffer = new byte[64 * 1024]; // 64KB buffer
            var messageBuffer = new MemoryStream();
            _audioChunkCount = 0;

            try
            {
                _log.Info("Starting Gemini receive loop...");

                while (_webSocket.State == WebSocketState.Open && !_cts.IsCancellationRequested)
                {
                    var result = await _webSocket.ReceiveAsync(
                        new ArraySegment<byte>(buffer), _cts.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _log.Info("Gemini WebSocket closed by server: status={Status}, description={Desc}",
                            result.CloseStatus, result.CloseStatusDescription);
                        break;
                    }

                    messageBuffer.Write(buffer, 0, result.Count);

                    if (result.EndOfMessage)
                    {
                        var jsonText = Encoding.UTF8.GetString(
                            messageBuffer.GetBuffer(), 0, (int)messageBuffer.Length);
                        messageBuffer.SetLength(0);

                        await ProcessGeminiMessage(jsonText);
                    }
                }

                _log.Info("Gemini receive loop ended. Total audio chunks: {Count}", _audioChunkCount);
            }
            catch (OperationCanceledException)
            {
                _log.Info("Gemini receive loop cancelled");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error in Gemini receive loop");
                if (_hangUpCallback != null)
                {
                    try { await _hangUpCallback(_callConnectionId); }
                    catch (Exception cbEx) { _log.Warn(cbEx, "Hang-up callback failed"); }
                }
            }
        }

        /// <summary>
        /// Parse and dispatch a single Gemini response message.
        /// </summary>
        private async Task ProcessGeminiMessage(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Setup complete
                if (root.TryGetProperty("setupComplete", out _))
                {
                    _sessionReady = true;
                    _log.Info("Gemini session setup complete");
                    return;
                }

                // Server content
                if (root.TryGetProperty("serverContent", out var serverContent))
                {
                    // Interrupted (barge-in)
                    if (serverContent.TryGetProperty("interrupted", out var interrupted) &&
                        interrupted.GetBoolean())
                    {
                        _log.Info("Gemini: barge-in detected");
                        var stopJson = MediaStreamingData.GetStopAudioForOutbound();
                        await _mediaStreaming.SendMessageAsync(stopJson);
                        return;
                    }

                    // Turn complete
                    if (serverContent.TryGetProperty("turnComplete", out var turnComplete) &&
                        turnComplete.GetBoolean())
                    {
                        _log.Info("Gemini: turn complete. Audio chunks: {Count}", _audioChunkCount);
                        await _mediaStreaming.FlushAudioAsync();
                        _mediaStreaming.NotifyAiResponseFinished();
                        return;
                    }

                    // Input transcription (user speech)
                    if (serverContent.TryGetProperty("inputTranscription", out var inputTranscription))
                    {
                        var text = "";
                        if (inputTranscription.TryGetProperty("text", out var inputText))
                            text = inputText.GetString() ?? "";

                        if (!string.IsNullOrWhiteSpace(text))
                        {
                            _log.Info("User transcript: {Text}", text);

                            // Filter Whisper-like hallucinations
                            if (WhisperHallucinationFilter.IsHallucination(text))
                            {
                                _log.Info("Filtered hallucinated transcript: {Text}", text);
                                return;
                            }

                            var recipientEntry = new TranscriptEntry
                            {
                                CallConnectionId = _callConnectionId,
                                Speaker = SpeakerType.Recipient,
                                Text = text,
                                Timestamp = DateTimeOffset.UtcNow
                            };

                            if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var call))
                            {
                                call.TranscriptEntries.Add(recipientEntry);
                            }

                            await _hubContext.Clients.Group(_callConnectionId)
                                .SendAsync("TranscriptUpdate", recipientEntry);

                            FireAndForgetSentiment(recipientEntry);
                            FireAndForgetEmotion(recipientEntry);
                        }
                        return;
                    }

                    // Output transcription (AI speech)
                    if (serverContent.TryGetProperty("outputTranscription", out var outputTranscription))
                    {
                        var text = "";
                        if (outputTranscription.TryGetProperty("text", out var outputText))
                            text = outputText.GetString() ?? "";

                        if (!string.IsNullOrWhiteSpace(text))
                        {
                            _log.Info("AI transcript: {Text}", text);

                            var aiEntry = new TranscriptEntry
                            {
                                CallConnectionId = _callConnectionId,
                                Speaker = SpeakerType.AI,
                                Text = text,
                                Timestamp = DateTimeOffset.UtcNow
                            };

                            if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var call))
                            {
                                call.TranscriptEntries.Add(aiEntry);
                            }

                            await _hubContext.Clients.Group(_callConnectionId)
                                .SendAsync("TranscriptUpdate", aiEntry);

                            FireAndForgetSentiment(aiEntry);
                            FireAndForgetEmotion(aiEntry);
                        }
                        return;
                    }

                    // Model turn — audio data
                    if (serverContent.TryGetProperty("modelTurn", out var modelTurn) &&
                        modelTurn.TryGetProperty("parts", out var parts))
                    {
                        foreach (var part in parts.EnumerateArray())
                        {
                            if (part.TryGetProperty("inlineData", out var inlineData) &&
                                inlineData.TryGetProperty("data", out var audioData))
                            {
                                var base64 = audioData.GetString();
                                if (!string.IsNullOrEmpty(base64))
                                {
                                    _audioChunkCount++;
                                    if (_audioChunkCount <= 3 || _audioChunkCount % 50 == 0)
                                    {
                                        _log.Info("Audio chunk #{Num} received", _audioChunkCount);
                                    }

                                    // Gemini outputs 24kHz PCM — wrap as ACS-format JSON
                                    // so FreeSwitchMediaHandler can process it the same way
                                    var audioBytes = Convert.FromBase64String(base64);
                                    var acsJson = MediaStreamingData.GetAudioDataForOutbound(audioBytes);
                                    _mediaStreaming.NotifyAiResponseStarted();
                                    await _mediaStreaming.SendMessageAsync(acsJson);
                                }
                            }
                        }
                    }
                }

                // Tool call (future extension)
                if (root.TryGetProperty("toolCall", out _))
                {
                    _log.Info("Gemini tool call received (not implemented)");
                }
            }
            catch (JsonException jex)
            {
                _log.Warn(jex, "Failed to parse Gemini message");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error processing Gemini message");
            }
        }

        /// <summary>
        /// Start the conversation: connect and begin listening.
        /// </summary>
        public void StartConversation()
        {
            _log.Info("StartConversation called");
            _ = Task.Run(async () =>
            {
                try
                {
                    await ConnectAsync();
                    await ReceiveFromGeminiAsync();
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "Unhandled exception in Gemini response task");
                }
            });
        }

        public void Close()
        {
            _log.Info("Closing Gemini connection");
            _cts.Cancel();
            try
            {
                if (_webSocket?.State == WebSocketState.Open)
                {
                    _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure,
                        "Call ended", CancellationToken.None).GetAwaiter().GetResult();
                }
            }
            catch { }
            _webSocket?.Dispose();
            _cts.Dispose();
        }

        private void FireAndForgetSentiment(TranscriptEntry entry)
        {
            if (_sentimentService == null) return;

            _ = Task.Run(async () =>
            {
                try
                {
                    string textToAnalyze = entry.Text;
                    if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var call))
                    {
                        var cutoff = entry.Timestamp.AddSeconds(-5);
                        var recentEntries = call.TranscriptEntries
                            .Where(e => e.Timestamp >= cutoff && e.Timestamp <= entry.Timestamp)
                            .OrderBy(e => e.Timestamp)
                            .ToList();

                        if (recentEntries.Count > 1)
                        {
                            textToAnalyze = string.Join(" ", recentEntries.Select(e => e.Text));
                        }
                    }

                    var sentiment = await _sentimentService.AnalyzeAsync(textToAnalyze);
                    entry.Sentiment = sentiment;

                    await _hubContext.Clients.Group(_callConnectionId)
                        .SendAsync("SentimentUpdate", new
                        {
                            callConnectionId = _callConnectionId,
                            entryTimestamp = entry.Timestamp,
                            sentiment = sentiment
                        });
                }
                catch (Exception ex)
                {
                    _log.Warn(ex, "Sentiment analysis failed");
                }
            });
        }

        private void FireAndForgetEmotion(TranscriptEntry entry)
        {
            if (_emotionService == null) return;

            _ = Task.Run(async () =>
            {
                try
                {
                    string textToAnalyze = entry.Text;
                    if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var call))
                    {
                        var cutoff = entry.Timestamp.AddSeconds(-5);
                        var recentEntries = call.TranscriptEntries
                            .Where(e => e.Timestamp >= cutoff && e.Timestamp <= entry.Timestamp)
                            .OrderBy(e => e.Timestamp)
                            .ToList();

                        if (recentEntries.Count > 1)
                        {
                            textToAnalyze = string.Join(" ", recentEntries.Select(e => e.Text));
                        }
                    }

                    var emotion = await _emotionService.AnalyzeAsync(textToAnalyze);
                    entry.Emotion = emotion;

                    await _hubContext.Clients.Group(_callConnectionId)
                        .SendAsync("EmotionUpdate", new
                        {
                            callConnectionId = _callConnectionId,
                            entryTimestamp = entry.Timestamp,
                            emotion = emotion
                        });
                }
                catch (Exception ex)
                {
                    _log.Warn(ex, "Emotion analysis failed");
                }
            });
        }
    }
}
