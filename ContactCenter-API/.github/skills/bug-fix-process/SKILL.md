---
name: bug-fix-process
description: Mandatory bug fix workflow requiring log-based root cause analysis before any code changes. Enforces using Logger.LogToFile for debugging, prohibits assumptions without log evidence, and requires developer confirmation before cleanup. Use when fixing bugs, debugging issues, or investigating unexpected behavior in the application.
---

# Bug Fix Process

## Golden Rule
**NEVER fix a bug without first confirming root cause from actual log output. No assumptions.**

## Mandatory Principles

1. **Trace root cause with logs** - REQUIRED before any fix
2. **Do not assume** or infer root cause without clear confirmation from debug logs
3. **All conclusions must be based on actual log evidence**, not assumptions or intuition
4. **If existing logs are insufficient**, add more logging BEFORE any code changes

## Bug Fix Steps

### Step 1: Add Logging
100% of cases must add logging with `Logger.LogToFile` at key points in the code/test to help with debugging.

```csharp
// ✅ REQUIRED - Add context-rich logging
Logger.LogToFile($"[DEBUG] WorkflowService.Execute - workflowId={workflowId}, tenantId={tenantId}, nodeCount={nodes.Count}");
Logger.LogToFile($"[DEBUG] NodeExecutor - nodeType={node.Type}, input={JsonSerializer.Serialize(input)}");
```

### Step 2: Run Tests & Review Logs
- Run/re-run the related test files
- Review the logs to identify the bug
- Update logging or test files to better capture the issue

### Step 3: Confirm Root Cause
- Update the test file to reflect the bug scenario
- **If root cause is not clear from logs → add more logs, do NOT guess**
- Repeat Step 2 until root cause is confirmed

### Step 4: Fix the Code
- Only after root cause is confirmed by log evidence
- Make targeted fix based on confirmed root cause

### Step 5: Verify Fix
- Run the test again to verify the fix
- Check logs frequently to confirm the fix works
- If test fails or logs don't confirm → go back to Step 4

### Step 6: Developer Confirms
- Wait for developer to manually verify the fix works
- Do NOT proceed to cleanup until developer confirms

### Step 7: Cleanup
- Remove all temporary `Logger.LogToFile` debug logging
- Run tests and lint one final time
- Confirm everything is clean and working

## Task Tracking
- **Create todos list** for tracking progress through these steps
- **Do NOT create summary markdown files** for bug fixes

## Anti-Patterns

```
❌ "I think the bug is caused by..." (without log evidence)
❌ Fixing code before adding debug logging
❌ Removing debug logs before developer confirms fix
❌ Skipping steps or assuming root cause from code reading alone
```
