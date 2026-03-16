using ContactCenterPOC.Hubs;
using ContactCenterPOC.Models;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.RegularExpressions;


namespace ContactCenterPOC.Services
{
    public class CallService
    {
        private readonly FreeSwitchService _freeSwitchService;
        private readonly string _callbackUri;
        private readonly ILogger<CallService> _logger;
        private readonly IConfiguration _configuration;
        private readonly IHubContext<TranscriptHub> _hubContext;
        private readonly CampaignService _campaignService;
        private readonly CallHistoryService _callHistoryService;
        private readonly SentimentAnalysisService? _sentimentService;
        private readonly EmotionAnalysisService? _emotionService;
        private readonly OperatorStyleAnalysisService? _operatorStyleService;
        private readonly CallSummaryService? _callSummaryService;
        private readonly SettingsService? _settingsService;
        private readonly VoiceLiveConfig _voiceLiveConfig;

        // Thread-safe dictionaries for concurrent call handling
        private readonly ConcurrentDictionary<string, ActiveCall> _activeCalls = new();
        private readonly ConcurrentDictionary<string, FreeSwitchMediaHandler> _mediaHandlers = new();

        public ConcurrentDictionary<string, ActiveCall> ActiveCalls => _activeCalls;

        public CallService(IConfiguration configuration, ILogger<CallService> logger, IHubContext<TranscriptHub> hubContext, CampaignService campaignService, CallHistoryService callHistoryService, VoiceLiveConfig voiceLiveConfig, FreeSwitchService freeSwitchService, SentimentAnalysisService? sentimentService = null, EmotionAnalysisService? emotionService = null, OperatorStyleAnalysisService? operatorStyleService = null, CallSummaryService? callSummaryService = null, SettingsService? settingsService = null)
        {
            _logger = logger;
            _configuration = configuration;
            _hubContext = hubContext;
            _campaignService = campaignService;
            _callHistoryService = callHistoryService;
            _sentimentService = sentimentService;
            _emotionService = emotionService;
            _operatorStyleService = operatorStyleService;
            _callSummaryService = callSummaryService;
            _settingsService = settingsService;
            _voiceLiveConfig = voiceLiveConfig;
            _freeSwitchService = freeSwitchService;
            _callbackUri = configuration["CallbackUrl"] ?? throw new InvalidOperationException("CallbackUrl not configured");

            // Wire up FreeSWITCH ESL events for call lifecycle
            _freeSwitchService.CallAnswered += OnFreeSwitchCallAnswered;
            _freeSwitchService.CallHangup += OnFreeSwitchCallHangup;
            _freeSwitchService.CallFailed += OnFreeSwitchCallFailed;
        }

        private void OnFreeSwitchCallAnswered(string uuid)
        {
            _ = HandleCallAnsweredAsync(uuid);
        }

        private async Task HandleCallAnsweredAsync(string uuid)
        {
            try
            {
                if (!_activeCalls.TryGetValue(uuid, out var activeCall)) return;

                // Guard: skip if already connected (StartCallInteraction also sets Connected)
                if (activeCall.Status == CallStatus.Connected) return;

                activeCall.Status = CallStatus.Connected;

                // No transfer needed here — originate uses &transfer() to auto-route on answer.
                // Just update UI status.
                await _hubContext.Clients.All.SendAsync("CallStatusChanged", new
                {
                    callConnectionId = uuid,
                    status = "Connected",
                    phoneNumber = activeCall.TargetPhoneNumber,
                    contactName = activeCall.ContactName,
                    campaignTitle = activeCall.CampaignTitle
                });

                _logger.LogInformation("[{UUID}] Call status updated to Connected", uuid);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{UUID}] Error handling call answered event", uuid);
            }
        }

        private void OnFreeSwitchCallHangup(string uuid, string cause)
        {
            _logger.LogInformation("[{UUID}] Call hangup event, cause: {Cause}", uuid, cause);
            _ = CleanupCall(uuid);
        }

        private void OnFreeSwitchCallFailed(string uuid)
        {
            if (_activeCalls.TryGetValue(uuid, out var activeCall))
            {
                activeCall.Status = CallStatus.Failed;
            }
            _ = CleanupCall(uuid);
        }

        private const int MaxConcurrentCalls = 5;
        private static readonly Regex E164Regex = new(@"^\+[1-9]\d{1,14}$");

        public async Task<List<(string callConnectionId, string phoneNumber)>> InitiateCall(string[] phoneNumbers, string? callContextPrompt, string? campaignId, string[]? contactNames, HttpContext httpContext)
        {
            // Validate and normalize phone numbers
            if (phoneNumbers == null || phoneNumbers.Length == 0)
                throw new ArgumentException("At least one phone number is required.");
            if (phoneNumbers.Length > 2)
                throw new ArgumentException("Maximum of 2 phone numbers allowed.");
            for (int i = 0; i < phoneNumbers.Length; i++)
            {
                // Convert local VN format (0xxx) to E.164 (+84xxx)
                var pn = phoneNumbers[i].Trim();
                if (pn.StartsWith("0") && pn.Length >= 9 && pn.Length <= 11)
                    pn = "+84" + pn.Substring(1);
                phoneNumbers[i] = pn;

                if (!E164Regex.IsMatch(pn))
                    throw new ArgumentException($"Phone number '{pn}' is not valid.");
            }

            // Check concurrent limit
            if (_activeCalls.Count + phoneNumbers.Length > MaxConcurrentCalls)
            {
                var available = MaxConcurrentCalls - _activeCalls.Count;
                throw new InvalidOperationException($"Maximum of {MaxConcurrentCalls} concurrent calls allowed. {available} slot(s) available.");
            }

            // Resolve prompt from campaign or direct prompt
            string effectivePrompt;
            string? resolvedCampaignId = null;
            string? resolvedCampaignTitle = null;

            if (!string.IsNullOrWhiteSpace(callContextPrompt))
            {
                // Direct prompt takes precedence
                effectivePrompt = callContextPrompt;
            }
            else if (!string.IsNullOrWhiteSpace(campaignId))
            {
                // Look up campaign
                var campaign = await _campaignService.GetByIdAsync(campaignId);
                if (campaign != null)
                {
                    effectivePrompt = campaign.AiBehaviorInstructions;
                    resolvedCampaignId = campaign.Id;
                    resolvedCampaignTitle = campaign.Title;
                }
                else
                {
                    _logger.LogWarning("Campaign {CampaignId} not found, using default prompt", campaignId);
                    effectivePrompt = _configuration["AzureOpenAI:SystemPrompt"] ?? "You are an AI assistant that helps people find information.";
                }
            }
            else
            {
                // Default prompt fallback (FR-008)
                effectivePrompt = _configuration["AzureOpenAI:SystemPrompt"] ?? "You are an AI assistant that helps people find information.";
            }

            var results = new List<(string callConnectionId, string phoneNumber)>();

            for (int i = 0; i < phoneNumbers.Length; i++)
            {
                var targetPhoneNumber = phoneNumbers[i];
                var contactName = (contactNames != null && i < contactNames.Length && !string.IsNullOrWhiteSpace(contactNames[i]))
                    ? contactNames[i].Trim()
                    : null;

                // Prepend contact name instruction to prompt if provided
                var callPrompt = effectivePrompt;
                if (contactName != null)
                {
                    callPrompt = $"IMPORTANT: The person you are calling is named {contactName}. You MUST greet them by name at the start of the conversation, for example: 'Hello {contactName}'. " + callPrompt;
                }

                // Originate call via FreeSWITCH ESL (transfer to mod_audio_stream on answer)
                var callConnectionId = await _freeSwitchService.OriginateOutboundCallAsync(targetPhoneNumber);

                if (contactName != null)
                {
                    _logger.LogInformation("[{CallConnectionId}] Call to {PhoneNumber} will greet contact as '{ContactName}'", callConnectionId, targetPhoneNumber, contactName);
                }

                var activeCall = new ActiveCall
                {
                    CallConnectionId = callConnectionId,
                    TargetPhoneNumber = targetPhoneNumber,
                    CampaignId = resolvedCampaignId,
                    CampaignTitle = resolvedCampaignTitle,
                    ContactName = contactName,
                    Prompt = callPrompt,
                    Status = CallStatus.Initiating,
                    StartedAt = DateTimeOffset.UtcNow
                };

                // Set up auto-terminate timeout from settings (default 2 minutes)
                var maxCallMinutes = 2.0;
                // Freeze VoiceApiMode and VoiceLiveModel from current settings (FR-014)
                var frozenVoiceApiMode = "ChatGPT";
                var frozenVoiceLiveModel = "gpt-4o";
                var frozenVoiceLiveVoice = "en-US-Ava:DragonHDLatestNeural";
                var frozenSelectedVoice = "alloy";
                if (_settingsService != null)
                {
                    try
                    {
                        var settings = await _settingsService.GetSettingsAsync();
                        maxCallMinutes = settings.MaxCallTimeMinutes;
                        frozenVoiceApiMode = settings.VoiceApiMode;
                        frozenVoiceLiveModel = settings.VoiceLiveModel;
                        frozenVoiceLiveVoice = settings.SelectedVoiceLiveVoice;
                        frozenSelectedVoice = settings.SelectedVoice;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "[{CallConnectionId}] Failed to load settings for max call time, using default {Default}min", callConnectionId, maxCallMinutes);
                    }
                }

                // Freeze engine settings on the ActiveCall for the call's duration
                activeCall.VoiceApiMode = frozenVoiceApiMode;
                activeCall.VoiceLiveModel = frozenVoiceApiMode == "VoiceLive" ? frozenVoiceLiveModel : null;
                activeCall.VoiceLiveVoice = frozenVoiceApiMode == "VoiceLive" ? frozenVoiceLiveVoice : null;

                // FR-015: Log engine type, voice, and model when call starts
                _logger.LogInformation("[{CallConnectionId}] Call initiated: engine={Engine}, voice={Voice}, model={Model}",
                    callConnectionId, frozenVoiceApiMode,
                    frozenVoiceApiMode == "VoiceLive" ? frozenVoiceLiveVoice : frozenSelectedVoice,
                    frozenVoiceApiMode == "VoiceLive" ? frozenVoiceLiveModel : "N/A");
                activeCall.CancellationTokenSource.CancelAfter(TimeSpan.FromMinutes(maxCallMinutes));
                activeCall.CancellationTokenSource.Token.Register(async () =>
                {
                    _logger.LogInformation("[{CallConnectionId}] Auto-terminated after {MaxMinutes} minutes", callConnectionId, maxCallMinutes);
                    await HangUpCall(callConnectionId);
                });

                _activeCalls[callConnectionId] = activeCall;
                results.Add((callConnectionId, targetPhoneNumber));

                // Persist an initial record immediately so the call appears in history even if
                // the app restarts or callbacks are handled by a different instance.
                await _callHistoryService.SaveCallRecordAsync(new CallRecord
                {
                    CallConnectionId = callConnectionId,
                    PhoneNumber = targetPhoneNumber,
                    CampaignId = resolvedCampaignId,
                    CampaignTitle = resolvedCampaignTitle,
                    ContactName = contactName,
                    Prompt = callPrompt,
                    RecordingId = null,
                    Duration = TimeSpan.Zero,
                    OverallSentiment = SentimentLabel.Neutral,
                    SentimentBreakdown = new SentimentBreakdown(),
                    TalkTimeRatio = new TalkTimeRatio(),
                    TranscriptEntries = new List<TranscriptEntry>(),
                    StartedAt = activeCall.StartedAt,
                    EndedAt = activeCall.StartedAt
                });
            }

            return results;
        }

        /// <summary>
        /// Handle a FreeSWITCH WebSocket connection.
        /// If no ActiveCall exists yet (FreeSWITCH connects directly), one is created automatically.
        /// </summary>
        public async Task StartCallInteraction(
            HttpContext httpContext, string callId,
            string? callerNumber = null, string? calledNumber = null,
            string? campaignId = null, string? contactName = null)
        {
            if (!httpContext.WebSockets.IsWebSocketRequest)
            {
                _logger.LogWarning("Non-WebSocket request received on WS endpoint");
                return;
            }

            var log = new CallLogger(_logger, callId);
            var ws = await httpContext.WebSockets.AcceptWebSocketAsync();
            log.Info("FreeSWITCH WebSocket connected");

            if (ws.State != WebSocketState.Open) return;

            // Look up existing ActiveCall (created by InitiateCall for outbound)
            // If not found, create one on-the-fly (FreeSWITCH connects directly for inbound)
            if (!_activeCalls.TryGetValue(callId, out var activeCall))
            {
                activeCall = await CreateActiveCallFromWebSocket(
                    callId, callerNumber, calledNumber, campaignId, contactName);
            }

            var callConnectionId = activeCall.CallConnectionId;

            // FR-010/FR-013: Check VoiceLive configuration before proceeding
            if (activeCall.VoiceApiMode == "VoiceLive" && !_voiceLiveConfig.IsConfigured)
            {
                log.Warn("VoiceLive call attempted but endpoint not configured");
                await _hubContext.Clients.Group(callConnectionId)
                    .SendAsync("CallStatusChanged", new
                    {
                        callConnectionId = callConnectionId,
                        status = "Failed",
                        message = "VoiceLive is not configured. Please contact your administrator."
                    });
                return;
            }

            // Read voice settings
            var selectedVoice = "alloy";
            var voiceApiMode = activeCall.VoiceApiMode;
            var voiceLiveModel = activeCall.VoiceLiveModel ?? "gpt-4o";
            var voiceLiveVoice = activeCall.VoiceLiveVoice ?? "en-US-Ava:DragonHDLatestNeural";
            if (_settingsService != null)
            {
                try
                {
                    var settings = await _settingsService.GetSettingsAsync();
                    selectedVoice = settings.SelectedVoice ?? "alloy";
                }
                catch (Exception ex)
                {
                    log.Warn(ex, "Failed to read voice setting, using defaults");
                }
            }

            // Push status update (Ringing until ESL ChannelAnswer confirms actual answer)
            if (activeCall.Status != CallStatus.Connected)
            {
                activeCall.Status = CallStatus.Ringing;
                await _hubContext.Clients.All.SendAsync("CallStatusChanged", new
                {
                    callConnectionId = callConnectionId,
                    status = "Ringing",
                    phoneNumber = activeCall.TargetPhoneNumber,
                    contactName = activeCall.ContactName,
                    campaignTitle = activeCall.CampaignTitle
                });
            }

            var handler = new FreeSwitchMediaHandler(
                ws, _configuration, _logger, _hubContext,
                callConnectionId, null,
                _sentimentService, _activeCalls, _emotionService, selectedVoice,
                voiceApiMode, voiceLiveModel, voiceLiveVoice, _voiceLiveConfig,
                _freeSwitchService);
            _mediaHandlers[callConnectionId] = handler;

            try
            {
                await handler.ProcessWebSocketAsync(activeCall.Prompt);
            }
            finally
            {
                // WebSocket closed — cleanup
                log.Info("WebSocket disconnected");

                // Try to hang up via ESL if connected (graceful — won't error if not connected)
                if (_freeSwitchService.IsConnected)
                {
                    try { await _freeSwitchService.HangUpAsync(callConnectionId); }
                    catch (Exception ex) { log.Warn(ex, "ESL hangup failed"); }
                }

                await CleanupCall(callConnectionId);
            }
        }

        /// <summary>
        /// Create an ActiveCall on-the-fly when FreeSWITCH connects directly via WebSocket
        /// (no prior InitiateCall from UI). Used for inbound calls or external origination.
        /// </summary>
        private async Task<ActiveCall> CreateActiveCallFromWebSocket(
            string callId, string? callerNumber, string? calledNumber,
            string? campaignId, string? contactName)
        {
            // Resolve prompt
            string effectivePrompt;
            string? resolvedCampaignId = null;
            string? resolvedCampaignTitle = null;

            if (!string.IsNullOrWhiteSpace(campaignId))
            {
                var campaign = await _campaignService.GetByIdAsync(campaignId);
                if (campaign != null)
                {
                    effectivePrompt = campaign.AiBehaviorInstructions;
                    resolvedCampaignId = campaign.Id;
                    resolvedCampaignTitle = campaign.Title;
                }
                else
                {
                    effectivePrompt = _configuration["AzureOpenAI:SystemPrompt"]
                        ?? "You are an AI assistant that helps people find information.";
                }
            }
            else
            {
                effectivePrompt = _configuration["AzureOpenAI:SystemPrompt"]
                    ?? "You are an AI assistant that helps people find information.";
            }

            if (!string.IsNullOrWhiteSpace(contactName))
            {
                effectivePrompt = $"IMPORTANT: The person you are talking to is named {contactName}. Greet them by name. " + effectivePrompt;
            }

            // Freeze voice settings from current SettingsService state
            var frozenVoiceApiMode = "ChatGPT";
            var frozenVoiceLiveModel = "gpt-4o";
            var frozenVoiceLiveVoice = "en-US-Ava:DragonHDLatestNeural";
            var maxCallMinutes = 2.0;
            if (_settingsService != null)
            {
                try
                {
                    var settings = await _settingsService.GetSettingsAsync();
                    maxCallMinutes = settings.MaxCallTimeMinutes;
                    frozenVoiceApiMode = settings.VoiceApiMode;
                    frozenVoiceLiveModel = settings.VoiceLiveModel;
                    frozenVoiceLiveVoice = settings.SelectedVoiceLiveVoice;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[{CallId}] Failed to load settings for WebSocket call, using defaults", callId);
                }
            }

            var phoneNumber = callerNumber ?? calledNumber ?? "unknown";

            var activeCall = new ActiveCall
            {
                CallConnectionId = callId,
                TargetPhoneNumber = phoneNumber,
                CampaignId = resolvedCampaignId,
                CampaignTitle = resolvedCampaignTitle,
                ContactName = contactName,
                Prompt = effectivePrompt,
                Status = CallStatus.Connected,
                StartedAt = DateTimeOffset.UtcNow,
                VoiceApiMode = frozenVoiceApiMode,
                VoiceLiveModel = frozenVoiceApiMode == "VoiceLive" ? frozenVoiceLiveModel : null,
                VoiceLiveVoice = frozenVoiceApiMode == "VoiceLive" ? frozenVoiceLiveVoice : null
            };

            // Auto-terminate timer
            activeCall.CancellationTokenSource.CancelAfter(TimeSpan.FromMinutes(maxCallMinutes));
            activeCall.CancellationTokenSource.Token.Register(async () =>
            {
                _logger.LogInformation("[{CallId}] Auto-terminated after {MaxMinutes} minutes", callId, maxCallMinutes);
                await HangUpCall(callId);
            });

            _activeCalls[callId] = activeCall;

            _logger.LogInformation(
                "[{CallId}] Created ActiveCall from WebSocket: phone={Phone}, engine={Engine}, campaign={Campaign}",
                callId, phoneNumber, frozenVoiceApiMode, resolvedCampaignId);

            // Persist initial record
            await _callHistoryService.SaveCallRecordAsync(new CallRecord
            {
                CallConnectionId = callId,
                PhoneNumber = phoneNumber,
                CampaignId = resolvedCampaignId,
                CampaignTitle = resolvedCampaignTitle,
                ContactName = contactName,
                Prompt = effectivePrompt,
                Duration = TimeSpan.Zero,
                OverallSentiment = SentimentLabel.Neutral,
                SentimentBreakdown = new SentimentBreakdown(),
                TalkTimeRatio = new TalkTimeRatio(),
                TranscriptEntries = new List<TranscriptEntry>(),
                StartedAt = activeCall.StartedAt,
                EndedAt = activeCall.StartedAt
            });

            // Notify UI
            await _hubContext.Clients.All.SendAsync("CallStatusChanged", new
            {
                callConnectionId = callId,
                status = "Initiating",
                phoneNumber = phoneNumber,
                contactName = contactName,
                campaignTitle = resolvedCampaignTitle
            });

            return activeCall;
        }

        public async Task HangUpCall(string callConnectionId)
        {
            try
            {
                // Only try ESL hangup if FreeSWITCH ESL is connected
                if (_freeSwitchService.IsConnected)
                {
                    await _freeSwitchService.HangUpAsync(callConnectionId);
                }
                else
                {
                    _logger.LogInformation("[{CallConnectionId}] ESL not connected, skipping hangup", callConnectionId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{CallConnectionId}] Error hanging up call", callConnectionId);
            }
            finally
            {
                await CleanupCall(callConnectionId);
            }
        }

        public async Task CleanupCall(string callConnectionId)
        {
            _logger.LogInformation("[{CallConnectionId}] Cleaning up call", callConnectionId);

            if (_activeCalls.TryRemove(callConnectionId, out var activeCall))
            {
                // Mark as disconnected so PersistHistoryIfNeededAsync will save it
                if (activeCall.Status == CallStatus.Connected)
                {
                    activeCall.Status = CallStatus.Disconnected;
                }

                await PersistHistoryIfNeededAsync(activeCall);

                // Stop recording if active
                if (!string.IsNullOrEmpty(activeCall.RecordingId))
                {
                    await StopRecordingAsync(activeCall.RecordingId);
                }

                // Dispose CTS
                activeCall.CancellationTokenSource.Dispose();
            }

            // Remove media handler
            _mediaHandlers.TryRemove(callConnectionId, out _);
        }

        private async Task PersistHistoryIfNeededAsync(ActiveCall activeCall)
        {
            try
            {
                // Only persist completed calls (matches original behavior which saved on CallDisconnected)
                if (activeCall.Status != CallStatus.Connected && activeCall.Status != CallStatus.Disconnected)
                {
                    return;
                }

                var endedAt = DateTimeOffset.UtcNow;
                var duration = endedAt - activeCall.StartedAt;
                var entries = activeCall.TranscriptEntries;

                // Overall sentiment = majority label among entries
                var overallSentiment = SentimentLabel.Neutral;
                if (entries.Count > 0)
                {
                    overallSentiment = entries
                        .GroupBy(e => e.Sentiment.Label)
                        .OrderByDescending(g => g.Count())
                        .First().Key;
                }

                // Sentiment breakdown percentages
                var breakdown = new SentimentBreakdown();
                if (entries.Count > 0)
                {
                    float total = entries.Count;
                    breakdown.PositivePercent = entries.Count(e => e.Sentiment.Label == SentimentLabel.Positive) / total * 100f;
                    breakdown.NeutralPercent = entries.Count(e => e.Sentiment.Label == SentimentLabel.Neutral) / total * 100f;
                    breakdown.NegativePercent = entries.Count(e => e.Sentiment.Label == SentimentLabel.Negative) / total * 100f;
                }

                // Talk time ratio (count of entries per speaker as proxy)
                var talkTime = new TalkTimeRatio();
                if (entries.Count > 0)
                {
                    float total = entries.Count;
                    var aiCount = entries.Count(e => e.Speaker == SpeakerType.AI);
                    var recipientCount = entries.Count(e => e.Speaker == SpeakerType.Recipient);
                    talkTime.AiPercent = aiCount / total * 100f;
                    talkTime.RecipientPercent = recipientCount / total * 100f;
                }

                // Compute operator style traits if service is available
                OperatorStyleTraits? operatorTraits = null;
                if (_operatorStyleService != null)
                {
                    var operatorEntries = entries.Where(e => e.Speaker == SpeakerType.AI).ToList();
                    if (operatorEntries.Count > 0)
                    {
                        try
                        {
                            operatorTraits = await _operatorStyleService.AnalyzeAsync(operatorEntries);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "[{CallConnectionId}] Failed to compute operator style traits", activeCall.CallConnectionId);
                        }
                    }
                }

                var callRecord = new CallRecord
                {
                    CallConnectionId = activeCall.CallConnectionId,
                    ServerCallId = activeCall.ServerCallId,
                    PhoneNumber = activeCall.TargetPhoneNumber,
                    CampaignId = activeCall.CampaignId,
                    CampaignTitle = activeCall.CampaignTitle,
                    ContactName = activeCall.ContactName,
                    Prompt = activeCall.Prompt,
                    RecordingId = activeCall.RecordingId,
                    Duration = duration,
                    OverallSentiment = overallSentiment,
                    SentimentBreakdown = breakdown,
                    TalkTimeRatio = talkTime,
                    OperatorStyleTraits = operatorTraits,
                    TranscriptEntries = entries,
                    StartedAt = activeCall.StartedAt,
                    EndedAt = endedAt,
                    VoiceApiMode = activeCall.VoiceApiMode,
                    VoiceLiveModel = activeCall.VoiceLiveModel,
                    VoiceLiveVoice = activeCall.VoiceLiveVoice
                };

                await _callHistoryService.SaveCallRecordAsync(callRecord);

                // Fire-and-forget: generate post-call summary in the background
                if (_callSummaryService != null && entries.Count > 0)
                {
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var summary = await _callSummaryService.GenerateSummaryAsync(entries);
                            if (!string.IsNullOrWhiteSpace(summary))
                            {
                                callRecord.CallSummary = summary;
                                callRecord.SummarizedAt = DateTimeOffset.UtcNow;
                                await _callHistoryService.SaveCallRecordAsync(callRecord);
                                _logger.LogInformation("[{CallConnectionId}] Post-call summary generated", activeCall.CallConnectionId);
                            }
                        }
                        catch (Exception summaryEx)
                        {
                            _logger.LogWarning(summaryEx, "[{CallConnectionId}] Failed to generate post-call summary", activeCall.CallConnectionId);
                        }
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[{CallConnectionId}] Failed to persist call history during cleanup", activeCall.CallConnectionId);
            }
        }

        public Task StartRecordingAsync(string serverCallId, string callConnectionId)
        {
            // Recording via FreeSWITCH is handled separately (uuid_record command)
            // Not yet implemented - stub for compatibility
            _logger.LogInformation("[{CallConnectionId}] Recording not yet implemented for FreeSWITCH", callConnectionId);
            return Task.CompletedTask;
        }

        public Task StopRecordingAsync(string recordingId)
        {
            _logger.LogInformation("Stop recording not yet implemented for FreeSWITCH");
            return Task.CompletedTask;
        }
    }
}
