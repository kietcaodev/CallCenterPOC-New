# Contact Center POC - GitHub Copilot Instructions

## Project Overview

**Contact Center POC** - AI-powered outbound call center system with FreeSWITCH ESL, real-time audio streaming, transcription, and AI analysis.

## Tech Stack
- **Backend**: ASP.NET Core (.NET 10), FreeSWITCH ESL (NEventSocket), SignalR
- **Frontend**: Razor Pages (ContactCenter-APP), JavaScript + SignalR client
- **AI Voice**: Azure OpenAI Realtime (`gpt-4o-mini-realtime-preview`) or Azure VoiceLive (switchable)
- **AI Analysis**: Azure OpenAI Chat (`gpt-4o-mini`) for sentiment, emotion, operator style, call summary
- **Storage**: Azure Blob Storage (call recordings, call history JSON)
- **Audio**: mod_audio_stream (FreeSWITCH ↔ WebSocket binary PCM16), AudioResampler (16kHz↔24kHz)

## Architecture Overview

**Call Flow:**
1. Frontend → `POST /api/Call/initiate` → CallService → FreeSwitchService.OriginateWithAudioForkAsync()
2. FreeSWITCH ESL originate → SIP gateway → phone
3. ChannelAnswer → audio fork via `socket:{CallbackUrl}/ws?callId={uuid}`
4. CallbackController WebSocket receives binary PCM16 16kHz from FreeSWITCH
5. FreeSwitchMediaHandler resamples 16kHz→24kHz → AI service (Azure OpenAI or VoiceLive)
6. AI response 24kHz→16kHz → WebSocket back to FreeSWITCH
7. Transcript + sentiment/emotion updates → SignalR → frontend

**Key Services (all Singleton):**
- `CallService` — Call orchestration, active calls (`ConcurrentDictionary`), max 5 concurrent
- `FreeSwitchService` — ESL connection (command + event), originate, uuid_kill, event subscriptions
- `AzureOpenAIService` — Realtime audio AI session per call
- `VoiceLiveService` — Alternative AI voice platform per call
- `SentimentAnalysisService`, `EmotionAnalysisService`, `OperatorStyleAnalysisService` — Real-time AI analysis
- `CallSummaryService` — Post-call AI summarization
- `CallHistoryService` — Blob Storage persistence (JSON + recordings)
- `CampaignService` — Campaign CRUD (Blob Storage)
- `SettingsService` — Operator settings persistence

**Controllers:**
- `CallController` (`/api/Call`) — initiate, hangup, active calls
- `CallbackController` (`/api/Callback`) — WebSocket endpoint `/ws` for FreeSWITCH audio
- `CallHistoryController` (`/api/CallHistory`) — Paged history, call detail, recording stream
- `CampaignController` (`/api/Campaign`) — CRUD campaigns
- `SettingsController` (`/api/Settings`) — Operator settings

**SignalR Hub:**
- `TranscriptHub` at `/transcriptHub` — JoinCall/LeaveCall groups, pushes CallStatusChanged + transcript updates

**Key Models:**
- `ActiveCall` — In-memory call state (UUID, status, transcript, cancellation token)
- `CallRecord` — Persisted call data (sentiment breakdown, talk time ratio, recording, summary)
- `Campaign` — AI behavior instructions template
- `TranscriptEntry` — Speaker (AI/Recipient), text, sentiment, emotion, timestamp
- `FreeSwitchMediaHandler` — Binary PCM handler with resampling
- `OperatorSettings` — Voice API mode (ChatGPT/VoiceLive), voice selection, max call time

---

## 🔧 Bug Fix Process (MANDATORY)

When fixing bugs, **always use the `bug-fix-process` skill**. Follow these rules:

1. **Use `Logger.LogToFile`** for all debug logging - no exceptions
2. **Never assume root cause** without log evidence
3. **Create todos list** for tracking - DO NOT create summary markdown files
4. **Wait for developer confirmation** before cleanup

```csharp
// ✅ Always use Logger.LogToFile for debugging
Logger.LogToFile($"[DEBUG] ServiceName.Method - key={value}, context={context}");
```

---

## 🔍 Logging

- Use `Logger.LogToFile` for temporary debug logging during bug fixes
- Production logging uses `ILogger<T>` with custom `FileLoggerProvider` (daily rotating files)
- Log prefixes: `[AI-{CallId}]` for Azure OpenAI, `[VL-{CallId}]` for VoiceLive
- Logs must include context (e.g., `callId`, `uuid`, `campaignId`)
- Remove all `Logger.LogToFile` debug logging after developer confirms fix

---

**Always follow the `bug-fix-process` skill when debugging or fixing bugs.**
