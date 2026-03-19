using Azure.AI.OpenAI;
using Azure.Identity;
using ContactCenterPOC.Hubs;
using ContactCenterPOC.Models;
using Microsoft.AspNetCore.SignalR;
using OpenAI.RealtimeConversation;
using System.ClientModel;
using System.Collections.Concurrent;


namespace ContactCenterPOC.Services
{
    public class AzureOpenAIService
    {
        private CancellationTokenSource m_cts;
        private RealtimeConversationSession m_aiSession;
        private IMediaStreamingHandler m_mediaStreaming;
        private MemoryStream m_memoryStream;
        private ILogger<CallService> _logger;
        private readonly CallLogger _log;
        private readonly IHubContext<TranscriptHub> _hubContext;
        private readonly string _callConnectionId;
        private readonly Func<string, Task>? _hangUpCallback;
        private readonly SentimentAnalysisService? _sentimentService;
        private readonly EmotionAnalysisService? _emotionService;
        private readonly ConcurrentDictionary<string, ActiveCall>? _activeCalls;
        private readonly string _selectedVoice;
        private readonly IConfiguration _configuration;
        private readonly string? _prompt;
        private volatile bool _sessionReady = false;
        private volatile bool _sessionAlive = false;
        private readonly SemaphoreSlim _reconnectLock = new(1, 1);
        private int _reconnectAttempts = 0;
        private const int MaxReconnectAttempts = 3;
        private const int ReconnectDelayMs = 2000;
        private int _sendErrorCount = 0;
        private DateTime _lastSendErrorLog = DateTime.MinValue;

        public AzureOpenAIService(
            IMediaStreamingHandler mediaStreaming,
            IConfiguration configuration,
            ILogger<CallService> logger,
            IHubContext<TranscriptHub> hubContext,
            string callConnectionId,
            Func<string, Task>? hangUpCallback = null,
            SentimentAnalysisService? sentimentService = null,
            ConcurrentDictionary<string, ActiveCall>? activeCalls = null,
            EmotionAnalysisService? emotionService = null,
            string? selectedVoice = null)
        {
            m_mediaStreaming = mediaStreaming;
            m_cts = new CancellationTokenSource();
            m_memoryStream = new MemoryStream();
            _logger = logger;
            _log = new CallLogger(logger, callConnectionId, "AI");
            _hubContext = hubContext;
            _callConnectionId = callConnectionId;
            _hangUpCallback = hangUpCallback;
            _sentimentService = sentimentService;
            _emotionService = emotionService;
            _activeCalls = activeCalls;
            _selectedVoice = selectedVoice ?? "alloy";
            _configuration = configuration;
            _prompt = null;

            try
            {
                _log.Info("Creating AI session (no prompt)...");
                m_aiSession = CreateAISessionAsync(configuration, null!).GetAwaiter().GetResult();
                _sessionReady = true;
                _sessionAlive = true;
                _log.Info("AI session created successfully");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "FAILED to create AI session");
                throw;
            }
        }

        public AzureOpenAIService(
            IMediaStreamingHandler mediaStreaming,
            string prompt,
            IConfiguration configuration,
            ILogger<CallService> logger,
            IHubContext<TranscriptHub> hubContext,
            string callConnectionId,
            Func<string, Task>? hangUpCallback = null,
            SentimentAnalysisService? sentimentService = null,
            ConcurrentDictionary<string, ActiveCall>? activeCalls = null,
            EmotionAnalysisService? emotionService = null,
            string? selectedVoice = null)
        {
            m_mediaStreaming = mediaStreaming;
            m_cts = new CancellationTokenSource();
            m_memoryStream = new MemoryStream();
            _logger = logger;
            _log = new CallLogger(logger, callConnectionId, "AI");
            _hubContext = hubContext;
            _callConnectionId = callConnectionId;
            _hangUpCallback = hangUpCallback;
            _sentimentService = sentimentService;
            _emotionService = emotionService;
            _activeCalls = activeCalls;
            _selectedVoice = selectedVoice ?? "alloy";
            _configuration = configuration;
            _prompt = prompt;

            try
            {
                _log.Info("Creating AI session with prompt ({PromptLen} chars)...", prompt?.Length ?? 0);
                m_aiSession = CreateAISessionAsync(configuration, prompt!).GetAwaiter().GetResult();
                _sessionReady = true;
                _sessionAlive = true;
                _log.Info("AI session created successfully");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "FAILED to create AI session");
                throw;
            }
        }



        private async Task<RealtimeConversationSession> CreateAISessionAsync(IConfiguration configuration, string prompt)
        {
            var openAiUri = configuration["AzureOpenAI:EndpointUri"];
            ArgumentNullException.ThrowIfNullOrEmpty(openAiUri);

            var openAiModelName = configuration["AzureOpenAI:DeploymentName"];
            ArgumentNullException.ThrowIfNullOrEmpty(openAiModelName);

            string? systemPrompt = prompt;
            if (systemPrompt == null)
            {
                systemPrompt = configuration["AzureOpenAI:SystemPrompt"];
                ArgumentNullException.ThrowIfNullOrEmpty(systemPrompt);
            }

            _log.Info("Connecting to OpenAI Realtime: endpoint={Endpoint}, deployment={Deployment}",
                openAiUri, openAiModelName);

            // Use API key if provided, otherwise fall back to DefaultAzureCredential (Managed Identity)
            var apiKey = configuration["AzureOpenAI:Key"];
            AzureOpenAIClient aiClient;
            if (!string.IsNullOrEmpty(apiKey))
            {
                _log.Info("Using API Key authentication");
                aiClient = new AzureOpenAIClient(new Uri(openAiUri), new System.ClientModel.ApiKeyCredential(apiKey));
            }
            else
            {
                _log.Info("Using DefaultAzureCredential (Managed Identity / Entra ID)");
                aiClient = new AzureOpenAIClient(new Uri(openAiUri), new DefaultAzureCredential());
            }
            var realtimeClient = aiClient.GetRealtimeConversationClient(openAiModelName);
            
            _log.Info("Starting conversation session...");
            var session = await realtimeClient.StartConversationSessionAsync();
            _log.Info("Conversation session started, configuring...");

            // Session options control connection-wide behavior shared across all conversations,
            // including audio input format and voice activity detection settings.
            ConversationSessionOptions sessionOptions = new()
            {
                Instructions = systemPrompt,
                Voice = MapVoice(_selectedVoice),
                InputAudioFormat = ConversationAudioFormat.Pcm16,
                OutputAudioFormat = ConversationAudioFormat.Pcm16,
                InputTranscriptionOptions = new()
                {
                    Model = "whisper-1",
                },
                TurnDetectionOptions = ConversationTurnDetectionOptions.CreateServerVoiceActivityTurnDetectionOptions(0.7f, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(300)),
            };

            await session.ConfigureSessionAsync(sessionOptions);

            // Set Whisper language to Vietnamese via raw session.update
            // (Language property not available in this SDK version)
            var langUpdate = BinaryData.FromString("""
                {
                    "type": "session.update",
                    "session": {
                        "input_audio_transcription": {
                            "model": "whisper-1",
                            "language": "vi"
                        }
                    }
                }
                """);
            await session.SendCommandAsync(langUpdate, null);

            _log.Info("Session configured (voice={Voice}, format=PCM16, VAD enabled, lang=vi)", _selectedVoice);
            return session;
        }

        private static ConversationVoice MapVoice(string voiceName)
        {
            return voiceName?.ToLowerInvariant() switch
            {
                "echo" => ConversationVoice.Echo,
                "fable" => new ConversationVoice("fable"),
                "onyx" => new ConversationVoice("onyx"),
                "nova" => new ConversationVoice("nova"),
                "shimmer" => ConversationVoice.Shimmer,
                _ => ConversationVoice.Alloy
            };
        }

        // Loop and wait for the AI response
        private async Task GetOpenAiStreamResponseAsync()
        {
            try
            {
                _log.Info("Starting initial AI response...");
                await m_aiSession.StartResponseAsync();
                _log.Info("Listening for AI updates...");
                int audioChunkCount = 0;
                
                await foreach (ConversationUpdate update in m_aiSession.ReceiveUpdatesAsync(m_cts.Token))
                {
                    if (update is ConversationSessionStartedUpdate sessionStartedUpdate)
                    {
                        _log.Info("Session started. ID: {SessionId}", sessionStartedUpdate.SessionId);
                    }

                    if (update is ConversationInputSpeechStartedUpdate speechStartedUpdate)
                    {
                        _log.Info("Voice activity detection started at {AudioStartTime} ms", speechStartedUpdate.AudioStartTime);
                        // Barge-in, send stop audio
                        var jsonString = MediaStreamingData.GetStopAudioForOutbound();
                        await m_mediaStreaming.SendMessageAsync(jsonString);
                    }

                    if (update is ConversationInputSpeechFinishedUpdate speechFinishedUpdate)
                    {
                        _log.Info("Voice activity detection ended at {AudioEndTime} ms", speechFinishedUpdate.AudioEndTime);
                    }

                    if (update is ConversationItemStreamingStartedUpdate itemStartedUpdate)
                    {
                        _log.Info("Begin streaming of new item");
                        m_mediaStreaming.NotifyAiResponseStarted();
                    }

                    // Audio transcript updates contain the incremental text matching the generated
                    // output audio.
                    if (update is ConversationItemStreamingAudioTranscriptionFinishedUpdate outputTranscriptDeltaUpdate)
                    {
                        _log.Info("AI transcript: {Transcript}", outputTranscriptDeltaUpdate.Transcript);
                        var aiEntry = new TranscriptEntry
                        {
                            CallConnectionId = _callConnectionId,
                            Speaker = SpeakerType.AI,
                            Text = outputTranscriptDeltaUpdate.Transcript,
                            Timestamp = DateTimeOffset.UtcNow
                        };

                        // Accumulate on ActiveCall for persistence
                        if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var activeCall))
                        {
                            activeCall.TranscriptEntries.Add(aiEntry);
                        }

                        await _hubContext.Clients.Group(_callConnectionId)
                            .SendAsync("TranscriptUpdate", aiEntry);

                        // Fire-and-forget sentiment analysis
                        FireAndForgetSentiment(aiEntry);
                        // Fire-and-forget emotion analysis
                        FireAndForgetEmotion(aiEntry);
                    }

                    // Audio delta updates contain the incremental binary audio data of the generated output
                    // audio, matching the output audio format configured for the session.
                    if (update is ConversationItemStreamingPartDeltaUpdate deltaUpdate)
                    {
                        if (deltaUpdate.AudioBytes != null)
                        {
                            audioChunkCount++;
                            if (audioChunkCount <= 3 || audioChunkCount % 50 == 0)
                            {
                                _log.Info("Sending audio chunk #{ChunkNum} ({ByteCount} bytes) to ACS", 
                                    audioChunkCount, deltaUpdate.AudioBytes.ToArray().Length);
                            }
                            var jsonString = MediaStreamingData.GetAudioDataForOutbound(deltaUpdate.AudioBytes.ToArray());
                            await m_mediaStreaming.SendMessageAsync(jsonString);
                        }
                        else
                        {
                            _log.Debug("Received text-only delta (no audio bytes)");
                        }
                    }

                    if (update is ConversationItemStreamingTextFinishedUpdate itemFinishedUpdate)
                    {
                        _log.Info("Item streaming finished, response_id={ResponseId}", itemFinishedUpdate.ResponseId);
                    }

                    if (update is ConversationInputTranscriptionFinishedUpdate transcriptionCompletedUpdate)
                    {
                        _log.Info("User audio transcript: {Transcript}", transcriptionCompletedUpdate.Transcript);

                        // Filter out Whisper hallucinations (YouTube outro phrases etc.)
                        if (WhisperHallucinationFilter.IsHallucination(transcriptionCompletedUpdate.Transcript))
                        {
                            _log.Info("Filtered hallucinated transcript: {Transcript}", transcriptionCompletedUpdate.Transcript);
                            continue;
                        }

                        var recipientEntry = new TranscriptEntry
                        {
                            CallConnectionId = _callConnectionId,
                            Speaker = SpeakerType.Recipient,
                            Text = transcriptionCompletedUpdate.Transcript,
                            Timestamp = DateTimeOffset.UtcNow
                        };

                        // Accumulate on ActiveCall for persistence
                        if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var activeCall2))
                        {
                            activeCall2.TranscriptEntries.Add(recipientEntry);
                        }

                        await _hubContext.Clients.Group(_callConnectionId)
                            .SendAsync("TranscriptUpdate", recipientEntry);

                        // Fire-and-forget sentiment analysis
                        FireAndForgetSentiment(recipientEntry);
                        // Fire-and-forget emotion analysis
                        FireAndForgetEmotion(recipientEntry);
                    }

                    if (update is ConversationResponseFinishedUpdate turnFinishedUpdate)
                    {
                        _log.Info("Model turn generation finished. Status: {Status}. Total audio chunks sent: {ChunkCount}", 
                            turnFinishedUpdate.Status, audioChunkCount);
                        // Flush remaining buffered audio BEFORE signaling finished,
                        // otherwise ProcessPlaybackQueue wakes up, sees empty queue + !_aiGenerating,
                        // and unmutes mic before the last segment is enqueued.
                        await m_mediaStreaming.FlushAudioAsync();
                        m_mediaStreaming.NotifyAiResponseFinished();
                    }

                    if (update is ConversationErrorUpdate errorUpdate)
                    {
                        _log.Error("OpenAI Realtime error: {ErrorMessage}", errorUpdate.Message);
                        if (_hangUpCallback != null)
                        {
                            await _hangUpCallback(_callConnectionId);
                        }
                        break;
                    }
                }
                _log.Info("AI response loop ended. Total audio chunks: {ChunkCount}", audioChunkCount);
            }
            catch (OperationCanceledException e)
            {
                _log.Info("AI response loop cancelled: {Message}", e.Message);
                _sessionAlive = false;
            }
            catch (Exception ex)
            {
                _sessionAlive = false;
                _log.Error(ex, "Exception during AI streaming");

                // Attempt auto-reconnect if not cancelled
                if (!m_cts.IsCancellationRequested)
                {
                    var reconnected = await TryReconnectAsync();
                    if (reconnected)
                    {
                        _log.Info("Reconnected successfully, restarting AI response loop");
                        // Restart the response loop recursively
                        await GetOpenAiStreamResponseAsync();
                        return;
                    }
                    else
                    {
                        _log.Error("Failed to reconnect after {Attempts} attempts, hanging up", _reconnectAttempts);
                        if (_hangUpCallback != null)
                        {
                            try { await _hangUpCallback(_callConnectionId); }
                            catch (Exception cbEx) { _log.Warn(cbEx, "Hang-up callback failed after AI error"); }
                        }
                    }
                }
            }
        }

        private async Task<bool> TryReconnectAsync()
        {
            if (!await _reconnectLock.WaitAsync(0))
            {
                _log.Info("Reconnect already in progress, skipping");
                return false;
            }

            try
            {
                while (_reconnectAttempts < MaxReconnectAttempts && !m_cts.IsCancellationRequested)
                {
                    _reconnectAttempts++;
                    _log.Info("Reconnect attempt {Attempt}/{Max}...", _reconnectAttempts, MaxReconnectAttempts);

                    try
                    {
                        await Task.Delay(ReconnectDelayMs * _reconnectAttempts, m_cts.Token);

                        // Dispose old session safely
                        try { m_aiSession?.Dispose(); } catch { /* ignore */ }

                        // Re-create session
                        m_aiSession = await CreateAISessionAsync(_configuration, _prompt!);
                        _sessionAlive = true;
                        _reconnectAttempts = 0;
                        _sendErrorCount = 0;
                        _log.Info("AI session reconnected successfully");
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _log.Warn(ex, "Reconnect attempt {Attempt} failed", _reconnectAttempts);
                    }
                }
                return false;
            }
            finally
            {
                _reconnectLock.Release();
            }
        }

        private void FireAndForgetSentiment(TranscriptEntry entry)
        {
            if (_sentimentService == null) return;

            _ = Task.Run(async () =>
            {
                try
                {
                    // Build rolling 5-second context: aggregate recent transcript for better sentiment
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

                    // Send SentimentUpdate event to the frontend
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
                    _log.Warn(ex, "Sentiment analysis failed for entry");
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
                    // Build rolling 5-second context (same as sentiment)
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

                    // Send EmotionUpdate event to the frontend
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
                    _log.Warn(ex, "Emotion analysis failed for entry");
                }
            });
        }

        public void StartConversation()
        {
            _log.Info("StartConversation called, sessionReady={Ready}", _sessionReady);
            if (!_sessionReady)
            {
                _log.Error("Cannot start conversation - session not ready");
                return;
            }
            _ = Task.Run(async () =>
            {
                try
                {
                    await GetOpenAiStreamResponseAsync();
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "Unhandled exception in AI response task");
                }
            });
        }

        public async Task SendAudioToExternalAI(MemoryStream memoryStream)
        {
            if (!_sessionAlive)
            {
                // Session is dead, silently drop audio (avoid error spam)
                return;
            }

            try
            {
                await m_aiSession.SendInputAudioAsync(memoryStream);
                // Reset error counter on success
                _sendErrorCount = 0;
            }
            catch (ObjectDisposedException)
            {
                _sessionAlive = false;
                var count = Interlocked.Increment(ref _sendErrorCount);
                if (count == 1)
                {
                    _log.Error("AI session WebSocket disposed - stopping audio send. Will attempt reconnect.");
                }
            }
            catch (Exception ex)
            {
                var count = Interlocked.Increment(ref _sendErrorCount);
                var now = DateTime.UtcNow;
                // Throttle error logging: log first error, then at most once per 5 seconds
                if (count == 1 || (now - _lastSendErrorLog).TotalSeconds >= 5)
                {
                    _log.Error(ex, "Failed to send audio to OpenAI ({ByteCount} bytes, errorCount={ErrorCount})",
                        memoryStream.Length, count);
                    _lastSendErrorLog = now;
                }
            }
        }

        /// <summary>
        /// Clear the server-side input audio buffer.
        /// Called after AI playback finishes to give VAD a clean slate
        /// (accumulated silence/echo can prevent speech detection onset).
        /// </summary>
        public async Task ClearInputAudioBufferAsync()
        {
            try
            {
                var clearCmd = BinaryData.FromString("{\"type\":\"input_audio_buffer.clear\"}");
                await m_aiSession.SendCommandAsync(clearCmd, null);
                _log.Info("Sent input_audio_buffer.clear");
            }
            catch (Exception ex)
            {
                _log.Warn(ex, "Failed to send input_audio_buffer.clear");
            }
        }

        public void Close()
        {
            _sessionAlive = false;
            m_cts.Cancel();
            m_cts.Dispose();
            try { m_aiSession?.Dispose(); } catch { /* ignore */ }
        }
    }
}