using Azure.AI.OpenAI;
using Azure.Identity;
using ContactCenterPOC.Hubs;
using ContactCenterPOC.Models;
using Microsoft.AspNetCore.SignalR;
using OpenAI.RealtimeConversation;
using System.ClientModel;
using System.Collections.Concurrent;
using System.Threading.Channels;


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

        // TTS mode: when set, GPT audio output is ignored and text is fed to Azure TTS instead.
        private readonly AzureTtsService? _ttsService;

        // Streaming TTS pipeline:
        //   AI text deltas → sentence splitter → TTS tasks (parallel) → ordered playback
        // Each turn gets a fresh pipeline; sentences are fired as soon as a boundary is detected
        // so TTS latency overlaps with GPT generation rather than waiting for the full response.
        private readonly System.Text.StringBuilder _ttsSentenceBuffer = new();
        private Channel<Task<byte[]?>>? _ttsChunkChannel;  // tasks enqueued in order
        private Task? _ttsPlaybackTask;                    // consumer: awaits each task, feeds PCM
        private int _ttsChunkIndex = 0;

        // Tracks whether the model currently has an active response in-flight.
        // Used to guard CancelResponseAsync so we never call it after a response finishes.
        private volatile bool _responseInProgress = false;

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
            string? selectedVoice = null,
            AzureTtsService? ttsService = null)
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
            _ttsService = ttsService;

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

            // TTS mode: request text-only output so GPT skips its audio encoder.
            // Text tokens arrive token-by-token (~300-500ms sooner than audio-transcript deltas),
            // letting the sentence pipeline fire TTS requests earlier → lower TTFT.
            if (_ttsService != null)
            {
                var textOnlyUpdate = BinaryData.FromString("""
                    {
                        "type": "session.update",
                        "session": {
                            "modalities": ["text"]
                        }
                    }
                    """);
                await session.SendCommandAsync(textOnlyUpdate, null);
                _log.Info("Session configured (TTS mode: modalities=text, Azure TTS handles audio output)");
            }
            else
            {
                _log.Info("Session configured (voice={Voice}, format=PCM16, VAD enabled, lang=vi)", _selectedVoice);
            }
            return session;
        }

        private static ConversationVoice MapVoice(string voiceName)
        {
            return voiceName?.ToLowerInvariant() switch
            {
                "echo" => ConversationVoice.Echo,
                "shimmer" => ConversationVoice.Shimmer,
                "ash" => new ConversationVoice("ash"),
                "ballad" => new ConversationVoice("ballad"),
                "coral" => new ConversationVoice("coral"),
                "sage" => new ConversationVoice("sage"),
                "verse" => new ConversationVoice("verse"),
                "marin" => new ConversationVoice("marin"),
                "cedar" => new ConversationVoice("cedar"),
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
                        _responseInProgress = true;
                        if (_ttsService != null)
                        {
                            // TTS mode: start fresh pipeline for this turn
                            _ttsSentenceBuffer.Clear();
                            _ttsChunkIndex = 0;
                            _ttsChunkChannel = Channel.CreateUnbounded<Task<byte[]?>>(
                                new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
                            _ttsPlaybackTask = PlayTtsChunksInOrderAsync(_ttsChunkChannel.Reader, m_cts.Token);
                        }
                        else
                        {
                            m_mediaStreaming.NotifyAiResponseStarted(); // immediate for native audio mode
                        }
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
                        if (_ttsService != null && _ttsChunkChannel != null)
                        {
                            // TTS mode: use deltaUpdate.Text — text-only modality streams token-by-token
                            // (much earlier & more granular than AudioTranscript which aligns to audio frames)
                            var transcriptDelta = deltaUpdate.Text ?? deltaUpdate.AudioTranscript;
                            if (!string.IsNullOrEmpty(transcriptDelta))
                            {
                                _ttsSentenceBuffer.Append(transcriptDelta);
                                audioChunkCount++;

                                // Fire TTS for each complete sentence found in the buffer
                                while (TryExtractSentence(_ttsSentenceBuffer, out var sentence))
                                {
                                    var idx = ++_ttsChunkIndex;
                                    var capturedSentence = sentence;
                                    _log.Info("[TTS] chunk#{Idx} queued: '{Text}'", idx, capturedSentence);
                                    // Fire TTS call immediately (runs in parallel)
                                    var ttsTask = _ttsService.SynthesizeAsync(capturedSentence, null, m_cts.Token);
                                    _ttsChunkChannel.Writer.TryWrite(ttsTask);
                                }
                            }
                        }
                        else if (deltaUpdate.AudioBytes != null)
                        {
                            // Native audio mode
                            audioChunkCount++;
                            if (audioChunkCount <= 3 || audioChunkCount % 50 == 0)
                            {
                                _log.Info("Sending audio chunk #{ChunkNum} ({ByteCount} bytes) to ACS", 
                                    audioChunkCount, deltaUpdate.AudioBytes.ToArray().Length);
                            }
                            var jsonString = MediaStreamingData.GetAudioDataForOutbound(deltaUpdate.AudioBytes.ToArray());
                            await m_mediaStreaming.SendMessageAsync(jsonString);
                        }
                    }

                    if (update is ConversationItemStreamingTextFinishedUpdate itemFinishedUpdate)
                    {
                        _log.Info("Item streaming text finished, response_id={ResponseId}", itemFinishedUpdate.ResponseId);
                        // In TTS (text-only) mode, ConversationItemStreamingAudioTranscriptionFinishedUpdate
                        // never fires. Use this event to log the AI transcript and update the UI/analysis.
                        if (_ttsService != null && !string.IsNullOrWhiteSpace(itemFinishedUpdate.Text))
                        {
                            _log.Info("AI transcript (text-only mode): {Transcript}", itemFinishedUpdate.Text);
                            var aiEntry = new TranscriptEntry
                            {
                                CallConnectionId = _callConnectionId,
                                Speaker = SpeakerType.AI,
                                Text = itemFinishedUpdate.Text,
                                Timestamp = DateTimeOffset.UtcNow
                            };
                            if (_activeCalls != null && _activeCalls.TryGetValue(_callConnectionId, out var activeCall3))
                                activeCall3.TranscriptEntries.Add(aiEntry);
                            await _hubContext.Clients.Group(_callConnectionId)
                                .SendAsync("TranscriptUpdate", aiEntry);
                            FireAndForgetSentiment(aiEntry);
                            FireAndForgetEmotion(aiEntry);
                        }
                    }

                    if (update is ConversationInputTranscriptionFinishedUpdate transcriptionCompletedUpdate)
                    {
                        _log.Info("User audio transcript: {Transcript}", transcriptionCompletedUpdate.Transcript);

                        // Filter out Whisper hallucinations (YouTube outro phrases etc.)
                        if (WhisperHallucinationFilter.IsHallucination(transcriptionCompletedUpdate.Transcript))
                        {
                            _log.Info("Filtered hallucinated transcript: {Transcript}", transcriptionCompletedUpdate.Transcript);
                            // Cancel any in-progress AI response triggered by this hallucinated audio.
                            // Guard with _responseInProgress: Whisper transcription arrives AFTER
                            // ConversationResponseFinishedUpdate in TTS mode (TTS synthesis takes ~1s),
                            // so by the time we get here the response may already be complete.
                            // Calling CancelResponseAsync with no active response causes Azure to
                            // send a ConversationErrorUpdate that was previously killing the session.
                            if (_responseInProgress)
                            {
                                try
                                {
                                    _responseInProgress = false;
                                    await m_aiSession.CancelResponseAsync();
                                    _log.Info("Cancelled AI response triggered by hallucinated audio");
                                    // TTS mode: ConversationResponseFinishedUpdate(cancelled) will
                                    // close the TTS channel + await playback + call NotifyAiResponseFinished.
                                    // Native audio mode: notify immediately.
                                    if (_ttsService == null)
                                        m_mediaStreaming.NotifyAiResponseFinished();
                                }
                                catch (Exception ex)
                                {
                                    _log.Warn(ex, "Failed to cancel response for hallucinated transcript (may already be finished)");
                                }
                            }
                            else
                            {
                                _log.Info("Hallucination detected but no active response to cancel — ignoring");
                            }
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
                        _log.Info("Model turn generation finished. Status: {Status}. Total audio chunks (GPT): {ChunkCount}", 
                            turnFinishedUpdate.Status, audioChunkCount);

                        if (_ttsService != null && _ttsChunkChannel != null)
                        {
                            if (turnFinishedUpdate.Status != ConversationStatus.Cancelled)
                            {
                                // Flush any remaining partial sentence (no sentence boundary at end)
                                var tail = _ttsSentenceBuffer.ToString().Trim();
                                if (!string.IsNullOrWhiteSpace(tail))
                                {
                                    var idx = ++_ttsChunkIndex;
                                    _log.Info("[TTS] chunk#{Idx} tail queued: '{Text}'", idx, tail);
                                    var ttsTask = _ttsService.SynthesizeAsync(tail, null, m_cts.Token);
                                    _ttsChunkChannel.Writer.TryWrite(ttsTask);
                                }
                                _ttsSentenceBuffer.Clear();
                            }

                            // Signal no more chunks; wait for ordered playback to finish
                            _ttsChunkChannel.Writer.TryComplete();
                            if (_ttsPlaybackTask != null)
                            {
                                try { await _ttsPlaybackTask; }
                                catch (OperationCanceledException) { }
                                catch (Exception ex) { _log.Warn(ex, "[TTS] Playback task faulted"); }
                            }

                            // Drain jitter buffer and unmute
                            await m_mediaStreaming.FlushAudioAsync();
                        }
                        else if (_ttsService == null)
                        {
                            // Native audio mode: flush jitter buffer and unmute
                            await m_mediaStreaming.FlushAudioAsync();
                        }
                        m_mediaStreaming.NotifyAiResponseFinished();
                        _responseInProgress = false; // response fully complete
                    }

                    if (update is ConversationErrorUpdate errorUpdate)
                    {
                        // Non-fatal errors (e.g. "Cancellation failed: no active response found")
                        // must NOT kill the session or hang up — just log as warning and continue.
                        var msg = errorUpdate.Message ?? "";
                        var isNonFatal =
                            msg.Contains("no active response", StringComparison.OrdinalIgnoreCase) ||
                            msg.Contains("Cancellation failed", StringComparison.OrdinalIgnoreCase) ||
                            msg.Contains("no response", StringComparison.OrdinalIgnoreCase);

                        if (isNonFatal)
                        {
                            _log.Warn("Non-fatal OpenAI Realtime error (ignored): {ErrorMessage}", msg);
                            continue;
                        }

                        _log.Error("Fatal OpenAI Realtime error: {ErrorMessage}", msg);
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

        /// <summary>
        /// Consumes TTS tasks from the channel in order, awaiting each result and feeding
        /// PCM bytes into the jitter buffer sequentially.
        /// Calls NotifyAiResponseStarted() before the first non-empty chunk so the jitter
        /// buffer is created at the right moment (not speculatively before TTS returns).
        /// </summary>
        private async Task PlayTtsChunksInOrderAsync(
            ChannelReader<Task<byte[]?>> reader,
            CancellationToken ct)
        {
            bool audioStarted = false;
            try
            {
                await foreach (var ttsTask in reader.ReadAllAsync(ct))
                {
                    byte[]? pcm;
                    try { pcm = await ttsTask; }
                    catch (Exception ex)
                    {
                        _log.Warn(ex, "[TTS] Chunk synthesis failed — skipping");
                        continue;
                    }

                    if (pcm == null || pcm.Length == 0)
                    {
                        _log.Warn("[TTS] Chunk returned empty PCM — skipping");
                        continue;
                    }

                    if (!audioStarted)
                    {
                        m_mediaStreaming.NotifyAiResponseStarted();
                        audioStarted = true;
                    }

                    await m_mediaStreaming.FeedPcm16AudioAsync(pcm);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.Error(ex, "[TTS] PlayTtsChunksInOrderAsync exception");
            }
        }

        /// <summary>
        /// Extracts the next complete sentence from <paramref name="sb"/>.
        /// A sentence ends at `. `, `! `, `? ` (or the same punctuation before \n/\r).
        /// Minimum 6 characters to avoid false splits on abbreviations like "Mr. ".
        /// Returns false when no complete sentence is found yet.
        /// </summary>
        private static bool TryExtractSentence(System.Text.StringBuilder sb, out string sentence)
        {
            sentence = "";
            if (sb.Length < 6) return false;

            var text = sb.ToString();
            for (int i = 5; i < text.Length - 1; i++)
            {
                char c    = text[i];
                char next = text[i + 1];
                if ((c == '.' || c == '!' || c == '?') &&
                    (next == ' ' || next == '\n' || next == '\r'))
                {
                    sentence = text[..(i + 1)].Trim();
                    sb.Remove(0, i + 2);
                    return !string.IsNullOrWhiteSpace(sentence);
                }
            }
            return false;
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