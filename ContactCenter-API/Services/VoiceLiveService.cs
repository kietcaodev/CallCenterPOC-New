using Azure.AI.VoiceLive;
using Azure.Identity;
using ContactCenterPOC.Hubs;
using ContactCenterPOC.Models;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace ContactCenterPOC.Services
{
    public class VoiceLiveService
    {
        private CancellationTokenSource m_cts;
        private VoiceLiveSession? m_session;
        private IMediaStreamingHandler m_mediaStreaming;
        private readonly ILogger<CallService> _logger;
        private readonly CallLogger _log;
        private readonly IHubContext<TranscriptHub> _hubContext;
        private readonly string _callConnectionId;
        private readonly Func<string, Task>? _hangUpCallback;
        private readonly SentimentAnalysisService? _sentimentService;
        private readonly EmotionAnalysisService? _emotionService;
        private readonly ConcurrentDictionary<string, ActiveCall>? _activeCalls;
        private readonly VoiceLiveConfig _voiceLiveConfig;
        private readonly string _selectedVoice;
        private readonly string _model;
        private readonly string _prompt;
        private bool _sessionReady = false;

        // Reconnection constants (FR-018: exponential backoff 1s, 2s, 4s — max 3)
        private const int MaxReconnectAttempts = 3;
        private static readonly int[] ReconnectDelaysMs = { 1000, 2000, 4000 };

        public VoiceLiveService(
            IMediaStreamingHandler mediaStreaming,
            string prompt,
            VoiceLiveConfig voiceLiveConfig,
            ILogger<CallService> logger,
            IHubContext<TranscriptHub> hubContext,
            string callConnectionId,
            string model = "gpt-4o",
            string? selectedVoice = null,
            Func<string, Task>? hangUpCallback = null,
            SentimentAnalysisService? sentimentService = null,
            ConcurrentDictionary<string, ActiveCall>? activeCalls = null,
            EmotionAnalysisService? emotionService = null)
        {
            m_mediaStreaming = mediaStreaming;
            m_cts = new CancellationTokenSource();
            _logger = logger;
            _log = new CallLogger(logger, callConnectionId, "VL");
            _hubContext = hubContext;
            _callConnectionId = callConnectionId;
            _hangUpCallback = hangUpCallback;
            _sentimentService = sentimentService;
            _emotionService = emotionService;
            _activeCalls = activeCalls;
            _voiceLiveConfig = voiceLiveConfig;
            _selectedVoice = selectedVoice ?? "en-US-Ava:DragonHDLatestNeural";
            _model = model;
            _prompt = prompt;

            try
            {
                _log.Info("Creating VoiceLive session (model={Model}, voice={Voice})...",
                    _model, _selectedVoice);
                m_session = CreateSessionAsync().GetAwaiter().GetResult();
                _sessionReady = true;
                _log.Info("VoiceLive session created successfully");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "FAILED to create VoiceLive session");
                throw;
            }
        }

        private async Task<VoiceLiveSession> CreateSessionAsync()
        {
            var endpoint = new Uri(_voiceLiveConfig.EndpointUri);

            VoiceLiveClient client;
            if (!string.IsNullOrWhiteSpace(_voiceLiveConfig.Key))
            {
                client = new VoiceLiveClient(endpoint, new Azure.AzureKeyCredential(_voiceLiveConfig.Key));
                _log.Info("Using API key authentication");
            }
            else
            {
                client = new VoiceLiveClient(endpoint, new DefaultAzureCredential());
                _log.Info("Using DefaultAzureCredential (Managed Identity)");
            }

            _log.Info("Starting session with model '{Model}'...", _model);
            var session = await client.StartSessionAsync(_model);

            // Configure session options
            var options = new VoiceLiveSessionOptions
            {
                Instructions = _prompt,
                Voice = new AzureStandardVoice(_selectedVoice),
                InputAudioFormat = InputAudioFormat.Pcm16,
                OutputAudioFormat = OutputAudioFormat.Pcm16,
                InputAudioNoiseReduction = new AudioNoiseReduction(AudioNoiseReductionType.AzureDeepNoiseSuppression),
                InputAudioEchoCancellation = new AudioEchoCancellation(),
                TurnDetection = new AzureSemanticVadTurnDetection(),
                InputAudioTranscription = new AudioInputTranscriptionOptions(AudioInputTranscriptionOptionsModel.Whisper1)
            };

            await session.ConfigureSessionAsync(options);
            _log.Info("Session configured (voice={Voice}, format=PCM16, echo cancel + noise reduction + semantic VAD)",
                _selectedVoice);

            return session;
        }

        public void StartConversation()
        {
            _log.Info("StartConversation called, sessionReady={Ready}", _sessionReady);
            if (!_sessionReady || m_session == null)
            {
                _log.Error("Cannot start conversation - session not ready");
                return;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await GetVoiceLiveStreamResponseAsync();
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "Unhandled exception in VoiceLive response task");
                }
            });
        }

        private async Task GetVoiceLiveStreamResponseAsync()
        {
          bool shouldRetry = true;
          while (shouldRetry)
          {
            shouldRetry = false;
            try
            {
                if (m_session == null) return;

                _log.Info("Starting VoiceLive initial response...");
                await m_session.StartResponseAsync();
                _log.Info("Listening for VoiceLive updates...");
                int audioChunkCount = 0;

                await foreach (SessionUpdate update in m_session.GetUpdatesAsync(m_cts.Token))
                {
                    if (update is SessionUpdateSessionCreated sessionCreated)
                    {
                        _log.Info("Session created");
                    }

                    // User barge-in: speech started
                    if (update is SessionUpdateInputAudioBufferSpeechStarted speechStarted)
                    {
                        _log.Info("Voice activity detection started");
                        var jsonString = MediaStreamingData.GetStopAudioForOutbound();
                        await m_mediaStreaming.SendMessageAsync(jsonString);
                    }

                    // AI audio output delta
                    if (update is SessionUpdateResponseAudioDelta audioDelta)
                    {
                        if (audioDelta.Delta != null)
                        {
                            audioChunkCount++;
                            if (audioChunkCount == 1)
                            {
                                m_mediaStreaming.NotifyAiResponseStarted();
                            }
                            if (audioChunkCount <= 3 || audioChunkCount % 50 == 0)
                            {
                                _log.Info("Sending audio chunk #{ChunkNum} to ACS",
                                    audioChunkCount);
                            }
                            var audioBytes = audioDelta.Delta.ToArray();
                            var jsonString = MediaStreamingData.GetAudioDataForOutbound(audioBytes);
                            await m_mediaStreaming.SendMessageAsync(jsonString);
                        }
                    }

                    // AI transcript delta (incremental)
                    if (update is SessionUpdateResponseAudioTranscriptDelta transcriptDelta)
                    {
                        // Deltas are incremental; we accumulate and emit on "done"
                    }

                    // AI transcript done (complete)
                    if (update is SessionUpdateResponseAudioTranscriptDone transcriptDone)
                    {
                        _log.Info("AI transcript: {Transcript}", transcriptDone.Transcript);
                        var aiEntry = new TranscriptEntry
                        {
                            CallConnectionId = _callConnectionId,
                            Speaker = SpeakerType.AI,
                            Text = transcriptDone.Transcript,
                            Timestamp = DateTimeOffset.UtcNow
                        };

                        if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var activeCall))
                        {
                            lock (activeCall.TranscriptEntries)
                            {
                                activeCall.TranscriptEntries.Add(aiEntry);
                            }
                        }

                        await _hubContext.Clients.Group(_callConnectionId)
                            .SendAsync("TranscriptUpdate", aiEntry);

                        FireAndForgetSentiment(aiEntry);
                        FireAndForgetEmotion(aiEntry);
                    }

                    // User transcript completed
                    if (update is SessionUpdateConversationItemInputAudioTranscriptionCompleted userTranscript)
                    {
                        _log.Info("User audio transcript: {Transcript}", userTranscript.Transcript);

                        // Filter out Whisper hallucinations (YouTube outro phrases etc.)
                        if (WhisperHallucinationFilter.IsHallucination(userTranscript.Transcript))
                        {
                            _log.Info("Filtered hallucinated transcript: {Transcript}", userTranscript.Transcript);
                            continue;
                        }

                        var recipientEntry = new TranscriptEntry
                        {
                            CallConnectionId = _callConnectionId,
                            Speaker = SpeakerType.Recipient,
                            Text = userTranscript.Transcript,
                            Timestamp = DateTimeOffset.UtcNow
                        };

                        if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var activeCall2))
                        {
                            lock (activeCall2.TranscriptEntries)
                            {
                                activeCall2.TranscriptEntries.Add(recipientEntry);
                            }
                        }

                        await _hubContext.Clients.Group(_callConnectionId)
                            .SendAsync("TranscriptUpdate", recipientEntry);

                        FireAndForgetSentiment(recipientEntry);
                        FireAndForgetEmotion(recipientEntry);
                    }

                    // Response done
                    if (update is SessionUpdateResponseDone responseDone)
                    {
                        _log.Info("Response turn finished. Total audio chunks: {ChunkCount}",
                            audioChunkCount);
                        // Flush remaining buffered audio BEFORE signaling finished,
                        // to avoid race where ProcessPlaybackQueue unmutes before last segment.
                        await m_mediaStreaming.FlushAudioAsync();
                        m_mediaStreaming.NotifyAiResponseFinished();
                    }

                    // Error handling
                    if (update is SessionUpdateError errorUpdate)
                    {
                        var errorCode = errorUpdate.Error?.Code;
                        var errorMessage = errorUpdate.Error?.Message;
                        _log.Error("VoiceLive error: code={Code}, message={Message}",
                            errorCode, errorMessage);

                        // FR-024: Rate-limit / quota errors — notify operator, don't retry
                        if (IsRateLimitOrQuotaError(errorCode))
                        {
                            await _hubContext.Clients.Group(_callConnectionId)
                                .SendAsync("CallStatusChanged", new
                                {
                                    callConnectionId = _callConnectionId,
                                    status = "Error",
                                    message = $"VoiceLive service error: {errorMessage ?? "Rate limit or quota exceeded"}"
                                });
                            if (_hangUpCallback != null)
                            {
                                await _hangUpCallback(_callConnectionId);
                            }
                            break;
                        }

                        // Attempt reconnection for other errors
                        var reconnected = await AttemptReconnectionAsync();
                        if (!reconnected)
                        {
                            if (_hangUpCallback != null)
                            {
                                await _hangUpCallback(_callConnectionId);
                            }
                            break;
                        }
                        // If reconnected, the loop will continue with the new session
                        audioChunkCount = 0;
                        continue;
                    }
                }

                _log.Info("VoiceLive response loop ended. Total audio chunks: {ChunkCount}",
                    audioChunkCount);
            }
            catch (OperationCanceledException)
            {
                _log.Info("VoiceLive response loop cancelled");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Exception during VoiceLive streaming");

                // Attempt reconnection on unexpected exceptions
                var reconnected = await AttemptReconnectionAsync();
                if (!reconnected)
                {
                    if (_hangUpCallback != null)
                    {
                        try { await _hangUpCallback(_callConnectionId); }
                        catch (Exception cbEx) { _log.Warn(cbEx, "Hang-up callback failed after VoiceLive error"); }
                    }
                }
                // If reconnected, set shouldRetry to re-enter the while loop
                    if (reconnected) shouldRetry = true;
            }
          } // end while
        }

        /// <summary>
        /// Attempt reconnection with exponential backoff (1s, 2s, 4s — max 3 attempts).
        /// FR-018/019/020: Reconnection with status indicators and operator abort support.
        /// </summary>
        private async Task<bool> AttemptReconnectionAsync()
        {
            for (int attempt = 1; attempt <= MaxReconnectAttempts; attempt++)
            {
                // Update ActiveCall reconnect tracking
                if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var activeCall))
                {
                    activeCall.Status = CallStatus.Reconnecting;
                    activeCall.ReconnectAttempts = attempt;

                    // Check if operator has aborted (CancellationToken cancelled = hang up)
                    if (activeCall.CancellationTokenSource.IsCancellationRequested)
                    {
                        _log.Info("Operator aborted during reconnection");
                        return false;
                    }
                }

                // FR-019: Emit Reconnecting status via SignalR
                _log.Warn("Reconnection attempt {Attempt}/{Max}...", attempt, MaxReconnectAttempts);
                await _hubContext.Clients.Group(_callConnectionId)
                    .SendAsync("CallStatusChanged", new
                    {
                        callConnectionId = _callConnectionId,
                        status = "Reconnecting",
                        message = $"Reconnecting\u2026 attempt {attempt}/{MaxReconnectAttempts}"
                    });

                // Exponential backoff delay
                var delay = ReconnectDelaysMs[attempt - 1];
                await Task.Delay(delay);

                try
                {
                    // Dispose old session
                    if (m_session != null)
                    {
                        try { m_session.Dispose(); } catch { }
                    }

                    // Create new session with same options
                    m_session = await CreateSessionAsync();
                    _sessionReady = true;
                    await m_session.StartResponseAsync();

                    _log.Info("Reconnection successful on attempt {Attempt}", attempt);

                    // Update status back to Connected
                    if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var callAfterReconnect))
                    {
                        callAfterReconnect.Status = CallStatus.Connected;
                    }

                    await _hubContext.Clients.Group(_callConnectionId)
                        .SendAsync("CallStatusChanged", new
                        {
                            callConnectionId = _callConnectionId,
                            status = "Connected",
                            message = "Reconnected successfully"
                        });

                    return true;
                }
                catch (Exception ex)
                {
                    _log.Warn(ex, "Reconnection attempt {Attempt} failed", attempt);
                }
            }

            // All attempts exhausted
            _log.Error("All {Max} reconnection attempts failed", MaxReconnectAttempts);

            // FR-019: Emit ReconnectFailed status
            await _hubContext.Clients.Group(_callConnectionId)
                .SendAsync("CallStatusChanged", new
                {
                    callConnectionId = _callConnectionId,
                    status = "ReconnectFailed",
                    message = "All reconnection attempts failed"
                });

            if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var failedCall))
            {
                failedCall.Status = CallStatus.Disconnected;
            }

            return false;
        }

        private static bool IsRateLimitOrQuotaError(string? errorCode)
        {
            if (string.IsNullOrEmpty(errorCode)) return false;
            return errorCode.Contains("rate_limit", StringComparison.OrdinalIgnoreCase)
                || errorCode.Contains("quota", StringComparison.OrdinalIgnoreCase)
                || errorCode.Contains("throttl", StringComparison.OrdinalIgnoreCase);
        }

        public async Task SendAudioToExternalAI(MemoryStream memoryStream)
        {
            try
            {
                if (m_session != null)
                {
                    memoryStream.Position = 0;
                    await m_session.SendInputAudioAsync(memoryStream);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to send audio to VoiceLive ({ByteCount} bytes)",
                    memoryStream.Length);
            }
        }

        /// <summary>
        /// Clear the server-side input audio buffer.
        /// Called after AI playback finishes to give VAD a clean slate.
        /// </summary>
        public async Task ClearInputAudioBufferAsync()
        {
            try
            {
                if (m_session != null)
                {
                    var clearCmd = BinaryData.FromString("{\"type\":\"input_audio_buffer.clear\"}");
                    await m_session.SendCommandAsync(clearCmd);
                    _log.Info("Sent input_audio_buffer.clear");
                }
            }
            catch (Exception ex)
            {
                _log.Warn(ex, "Failed to send input_audio_buffer.clear (VoiceLive may not support it)");
            }
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
                        List<TranscriptEntry> snapshot;
                        lock (call.TranscriptEntries)
                        {
                            snapshot = call.TranscriptEntries.ToList();
                        }
                        var recentEntries = snapshot
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
                    string textToAnalyze = entry.Text;
                    if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var call))
                    {
                        var cutoff = entry.Timestamp.AddSeconds(-5);
                        List<TranscriptEntry> snapshot;
                        lock (call.TranscriptEntries)
                        {
                            snapshot = call.TranscriptEntries.ToList();
                        }
                        var recentEntries = snapshot
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
                    _log.Warn(ex, "Emotion analysis failed for entry");
                }
            });
        }

        public void Close()
        {
            m_cts.Cancel();
            m_cts.Dispose();
            if (m_session != null)
            {
                try { m_session.Dispose(); } catch { }
            }
        }
    }
}
