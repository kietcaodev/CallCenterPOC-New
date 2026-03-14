# Contact Center POC - GitHub Copilot Instructions

## Project Overview

**Contact Center POC** - Call center system with FreeSWITCH, ASP.NET Core, real-time transcription, and AI analysis.

## Tech Stack
- **Backend**: ASP.NET Core (.NET 10), FreeSWITCH (ESL), SignalR
- **Frontend**: Razor Pages (ContactCenter-APP)
- **AI**: Azure OpenAI (sentiment, emotion, operator style analysis, call summary)
- **Voice**: VoiceLive TTS, Google STT, mod_audio_stream

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

- Use `Logger.LogToFile` for debugging
- Logs must include context (e.g., `callId`, `sessionId`)
- Remove all debug logging after developer confirms fix

---

**Always follow the `bug-fix-process` skill when debugging or fixing bugs.**
