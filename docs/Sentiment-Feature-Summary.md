# Tính năng Sentiment Analysis — Tổng hợp chi tiết

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Kiến trúc tổng thể](#2-kiến-trúc-tổng-thể)
3. [Models (Cấu trúc dữ liệu)](#3-models-cấu-trúc-dữ-liệu)
4. [Service phân tích — SentimentAnalysisService](#4-service-phân-tích--sentimentanalysisservice)
5. [Base class — AzureOpenAIAnalysisBase](#5-base-class--azureopenaianalysisbase)
6. [Phân tích real-time trong cuộc gọi — AzureOpenAIService](#6-phân-tích-real-time-trong-cuộc-gọi--azureopenaiservice)
7. [Tổng hợp khi kết thúc cuộc gọi — CallService](#7-tổng-hợp-khi-kết-thúc-cuộc-gọi--callservice)
8. [Batch Processing — CallHistoryController](#8-batch-processing--callhistorycontroller)
9. [API Endpoints trả về Sentiment](#9-api-endpoints-trả-về-sentiment)
10. [SignalR Real-time Communication](#10-signalr-real-time-communication)
11. [Frontend — JavaScript (site.js)](#11-frontend--javascript-sitejs)
12. [Frontend — HTML (Index.cshtml)](#12-frontend--html-indexcshtml)
13. [Frontend — CSS (site.css)](#13-frontend--css-sitecss)
14. [Cấu hình (Configuration)](#14-cấu-hình-configuration)
15. [Dependency Injection](#15-dependency-injection)
16. [Sơ đồ luồng dữ liệu End-to-End](#16-sơ-đồ-luồng-dữ-liệu-end-to-end)
17. [Xử lý lỗi & Fallback](#17-xử-lý-lỗi--fallback)

---

## 1. Tổng quan

Tính năng Sentiment Analysis phân tích cảm xúc (tích cực / trung lập / tiêu cực) của từng câu nói trong cuộc gọi call center, hoạt động **real-time** trong cuộc gọi và tổng hợp lại sau khi cuộc gọi kết thúc. Tính năng sử dụng **Azure OpenAI** (LLM) để phân loại sentiment qua prompt engineering.

**Các khả năng chính:**

- Phân tích sentiment real-time cho từng câu nói (cả AI lẫn người gọi)
- Cửa sổ ngữ cảnh 5 giây (rolling context window) để đánh giá chính xác hơn
- Push kết quả real-time qua SignalR đến frontend
- Hiển thị chấm màu (dot) trên mỗi bubble chat
- Đồ thị timeline sentiment dạng canvas rolling chart
- KPI widget tổng hợp sentiment trung bình
- Tổng hợp OverallSentiment (majority voting) và SentimentBreakdown (%) khi kết thúc cuộc gọi
- Batch processing cho các cuộc gọi lịch sử chưa có dữ liệu sentiment

---

## 2. Kiến trúc tổng thể

```
┌─────────────────────────────────────────────────────────────┐
│                     FRONTEND (Razor + JS)                   │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ KPI Card │  │ Sentiment    │  │ Call Detail Panel       │ │
│  │ (#kpi    │  │ Timeline     │  │ - Badge (Overall)      │ │
│  │ Sentiment│  │ (Canvas)     │  │ - Breakdown bars (%)   │ │
│  └──────────┘  └──────────────┘  └────────────────────────┘ │
│        ▲              ▲                    ▲                 │
│        └──────────────┼────────────────────┘                 │
│                       │ SignalR "SentimentUpdate"            │
└───────────────────────┼─────────────────────────────────────┘
                        │
┌───────────────────────┼─────────────────────────────────────┐
│                    BACKEND (ASP.NET Core)                    │
│                       │                                      │
│  ┌────────────────────┴──────────────────────────────────┐  │
│  │ AzureOpenAIService (Realtime conversation handler)    │  │
│  │  - FireAndForgetSentiment(entry)                      │  │
│  │  - Rolling 5s context window                          │  │
│  │  - Push via IHubContext<TranscriptHub>                 │  │
│  └────────────────────┬──────────────────────────────────┘  │
│                       │                                      │
│  ┌────────────────────▼──────────────────────────────────┐  │
│  │ SentimentAnalysisService : AzureOpenAIAnalysisBase    │  │
│  │  - AnalyzeAsync(text) → SentimentResult               │  │
│  │  - Azure OpenAI Chat Completion API                   │  │
│  └────────────────────┬──────────────────────────────────┘  │
│                       │                                      │
│  ┌────────────────────▼──────────────────────────────────┐  │
│  │ CallService.PersistHistoryIfNeededAsync()             │  │
│  │  - Majority voting → OverallSentiment                 │  │
│  │  - Count-based → SentimentBreakdown (%)               │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Models (Cấu trúc dữ liệu)

### 3.1 SentimentLabel (Enum)

**File:** `ContactCenter-API/Models/SentimentResult.cs`

```csharp
public enum SentimentLabel
{
    Positive,   // Giá trị 0
    Neutral,    // Giá trị 1
    Negative    // Giá trị 2
}
```

### 3.2 SentimentResult

**File:** `ContactCenter-API/Models/SentimentResult.cs`

```csharp
public class SentimentResult
{
    public SentimentLabel Label { get; set; } = SentimentLabel.Neutral;
    public float Confidence { get; set; } = 0f;   // 0.0 – 1.0
}
```

- **Label**: Phân loại sentiment (Positive / Neutral / Negative)
- **Confidence**: Độ tin cậy do LLM trả về (0.0 – 1.0)

### 3.3 TranscriptEntry (chứa Sentiment per-entry)

**File:** `ContactCenter-API/Models/TranscriptEntry.cs`

```csharp
public class TranscriptEntry
{
    public string CallConnectionId { get; set; } = string.Empty;
    public SpeakerType Speaker { get; set; }          // AI hoặc Recipient
    public string Text { get; set; } = string.Empty;
    public SentimentResult Sentiment { get; set; } = new SentimentResult();  // ← Sentiment gắn per-entry
    public EmotionResult? Emotion { get; set; }
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}
```

### 3.4 CallRecord (chứa Sentiment tổng hợp)

**File:** `ContactCenter-API/Models/CallRecord.cs`

```csharp
public class CallRecord
{
    // ... các field khác ...
    public SentimentLabel OverallSentiment { get; set; } = SentimentLabel.Neutral;  // ← Majority label
    public SentimentBreakdown SentimentBreakdown { get; set; } = new();             // ← % breakdown
    public List<TranscriptEntry> TranscriptEntries { get; set; } = new();
}
```

### 3.5 SentimentBreakdown

**File:** `ContactCenter-API/Models/CallRecord.cs`

```csharp
public class SentimentBreakdown
{
    public float PositivePercent { get; set; }    // 0 – 100
    public float NeutralPercent { get; set; }     // 0 – 100
    public float NegativePercent { get; set; }    // 0 – 100
}
```

### 3.6 CallHistorySummary (chứa OverallSentiment dạng string)

**File:** `ContactCenter-API/Models/CallRecord.cs`

```csharp
public class CallHistorySummary
{
    // ... các field khác ...
    public string OverallSentiment { get; set; } = "Neutral";
}
```

---

## 4. Service phân tích — SentimentAnalysisService

**File:** `ContactCenter-API/Services/SentimentAnalysisService.cs`

### 4.1 Kế thừa

Kế thừa từ `AzureOpenAIAnalysisBase`, sử dụng deployment config key `AzureOpenAI:SentimentDeployment` (nếu có, nếu không fallback sang `AzureOpenAI:ChatDeployment` hoặc `AzureOpenAI:DeploymentName`).

### 4.2 Các hằng số

| Hằng số | Giá trị | Mô tả |
|---------|---------|-------|
| `SentimentMaxCompletionTokens` | `200` | Max tokens cho model mới (reasoning model) |
| `SentimentReasoningEffort` | `"low"` | Mức reasoning effort (tiết kiệm token) |
| `SentimentLegacyMaxTokens` | `50` | Max tokens cho model legacy |
| `SentimentLegacyTemperature` | `0f` | Temperature = 0 cho kết quả deterministic |

### 4.3 System Prompt

```
You are a sentiment analysis service. Classify the sentiment of the given text as
Positive, Neutral, or Negative. Respond ONLY with a JSON object:
{"label":"Positive|Neutral|Negative","confidence":0.0-1.0}. No other text or explanation.
```

### 4.4 Method `AnalyzeAsync(string? text)`

**Luồng xử lý:**

1. Nếu `text` null/empty → trả `Neutral / 0`
2. Nếu service chưa cấu hình (`!IsConfigured`) → trả `Neutral / 0`
3. Gọi `CallChatCompletionAsync()` với system prompt + user text
4. Nếu response empty → trả `Neutral / 0`
5. Parse JSON response qua `ParseSentimentJson()`
6. Nếu exception → log warning, trả `Neutral / 0`

### 4.5 Method `ParseSentimentJson(string? json)` (static)

**Chiến lược parse 2 lớp:**

1. **Lần 1**: Parse trực tiếp JSON → đọc `label` và `confidence`
2. **Lần 2 (fallback)**: Nếu parse lỗi, dùng `ExtractJsonObject()` để trích xuất JSON object từ text có chứa text thừa (LLM đôi khi trả thêm giải thích)
3. Nếu cả 2 lần đều lỗi → trả `Neutral / 0`

**Mapping label:**
- `"positive"` → `SentimentLabel.Positive`
- `"negative"` → `SentimentLabel.Negative`
- Bất kỳ giá trị khác → `SentimentLabel.Neutral`

---

## 5. Base class — AzureOpenAIAnalysisBase

**File:** `ContactCenter-API/Services/AzureOpenAIAnalysisBase.cs`

### 5.1 Vai trò

Lớp abstract dùng chung cho Sentiment, Emotion, Operator Style, Summary analysis services. Quản lý:

- HTTP client construction
- Authentication (API key hoặc DefaultAzureCredential / Managed Identity)
- Chat completion request với cơ chế **primary/legacy parameter retry**
- JSON response extraction

### 5.2 Cấu hình Endpoint

Ưu tiên config theo thứ tự:
1. `AzureOpenAI:ChatEndpointUri` → fallback `AzureOpenAI:EndpointUri`
2. `AzureOpenAI:ChatKey` → fallback `AzureOpenAI:Key`
3. Deployment: `AzureOpenAI:SentimentDeployment` (dedicated) → `AzureOpenAI:ChatDeployment` → `AzureOpenAI:DeploymentName`

### 5.3 Authentication

- **Ưu tiên 1**: API Key (`api-key` header)
- **Ưu tiên 2**: `DefaultAzureCredential` (Managed Identity / Entra ID) + Bearer token

### 5.4 Method `CallChatCompletionAsync()`

**Cơ chế retry thông minh:**

1. **Attempt 1**: Gửi với tham số model mới (`max_completion_tokens`, `reasoning_effort`)
2. Nếu nhận HTTP 400 + message chứa "Unsupported parameter" / "does not support" / "Unrecognized request argument" → **Attempt 2**: Gửi với tham số legacy (`max_tokens`, `temperature`)
3. Parse response: `choices[0].message.content`

### 5.5 Các utility methods

| Method | Mô tả |
|--------|-------|
| `ExtractAssistantContent()` | Trích xuất `content` từ response JSON |
| `ExtractJsonObject()` | Tìm JSON object đầu tiên `{...}` từ chuỗi có text thừa |
| `LooksLikeUnsupportedParam()` | Phát hiện lỗi unsupported parameter để trigger retry |
| `TruncateForLog()` | Cắt ngắn string cho logging |

---

## 6. Phân tích real-time trong cuộc gọi — AzureOpenAIService

**File:** `ContactCenter-API/Services/AzureOpenAIService.cs`

### 6.1 Dependency

`AzureOpenAIService` nhận `SentimentAnalysisService?` qua constructor (nullable, optional).

### 6.2 Khi nào sentiment được phân tích?

Sentiment được phân tích **cho mỗi transcript entry** khi:

1. **AI nói xong** (`ConversationItemStreamingAudioTranscriptionFinishedUpdate`): Tạo `TranscriptEntry` (Speaker = AI) → `FireAndForgetSentiment(aiEntry)`
2. **Người gọi nói xong** (`ConversationInputTranscriptionFinishedUpdate`): Tạo `TranscriptEntry` (Speaker = Recipient) → `FireAndForgetSentiment(recipientEntry)`

### 6.3 Method `FireAndForgetSentiment(TranscriptEntry entry)`

**Đặc điểm:**
- Chạy trên background thread (`Task.Run`) — không block luồng audio chính
- Fire-and-forget pattern

**Luồng xử lý chi tiết:**

```
1. Kiểm tra _sentimentService != null (nếu null → bỏ qua)
2. Task.Run (async):
   a. Xây dựng rolling 5-second context window:
      - Lấy cutoff = entry.Timestamp - 5 giây
      - Lọc các entries từ ActiveCall có Timestamp >= cutoff && <= entry.Timestamp
      - Nếu có > 1 entry → nối tất cả text lại (separated by space)
      - Nếu chỉ có 1 entry → dùng entry.Text gốc
   b. Gọi _sentimentService.AnalyzeAsync(textToAnalyze)
   c. Gán entry.Sentiment = sentiment result
   d. Push SignalR event "SentimentUpdate" đến group của cuộc gọi:
      {
        callConnectionId: "...",
        entryTimestamp: "2026-03-16T10:30:00Z",
        sentiment: { label: "Positive", confidence: 0.92 }
      }
3. Catch exception → log warning, không throw
```

### 6.4 Tại sao dùng rolling 5-second context?

Một câu nói đơn lẻ có thể không đủ ngữ cảnh để đánh giá chính xác sentiment. Bằng cách nối các câu trong 5 giây gần nhất, LLM có thêm context để phân tích chính xác hơn (ví dụ: "Tôi gọi để hỏi về..." → "Cảm ơn bạn rất nhiều!" — câu thứ 2 rõ ràng hơn khi có câu thứ 1).

---

## 7. Tổng hợp khi kết thúc cuộc gọi — CallService

**File:** `ContactCenter-API/Services/CallService.cs`

### 7.1 Method `PersistHistoryIfNeededAsync(ActiveCall activeCall)`

Được gọi khi cuộc gọi kết thúc (status = Connected hoặc Disconnected).

### 7.2 Tính OverallSentiment (Majority Voting)

```csharp
if (entries.Count > 0)
{
    overallSentiment = entries
        .GroupBy(e => e.Sentiment.Label)      // Nhóm theo label
        .OrderByDescending(g => g.Count())    // Sắp xếp theo số lượng giảm dần
        .First().Key;                          // Lấy label xuất hiện nhiều nhất
}
```

**Ví dụ:** Nếu 10 entries: 6 Positive + 3 Neutral + 1 Negative → OverallSentiment = `Positive`

### 7.3 Tính SentimentBreakdown (Phần trăm)

```csharp
float total = entries.Count;
breakdown.PositivePercent = entries.Count(e => e.Sentiment.Label == SentimentLabel.Positive) / total * 100f;
breakdown.NeutralPercent  = entries.Count(e => e.Sentiment.Label == SentimentLabel.Neutral)  / total * 100f;
breakdown.NegativePercent = entries.Count(e => e.Sentiment.Label == SentimentLabel.Negative) / total * 100f;
```

**Ví dụ:** 10 entries: 6P + 3N + 1Neg → `{ PositivePercent: 60, NeutralPercent: 30, NegativePercent: 10 }`

### 7.4 Khởi tạo cuộc gọi ban đầu

Khi cuộc gọi mới được tạo (`InitiateCall`), CallRecord được lưu ngay với giá trị mặc định:
```csharp
OverallSentiment = SentimentLabel.Neutral,
SentimentBreakdown = new SentimentBreakdown()   // { 0, 0, 0 }
```

---

## 8. Batch Processing — CallHistoryController

**File:** `ContactCenter-API/Controllers/CallHistoryController.cs`

### 8.1 Endpoint

```
POST /api/CallHistory/batch-process?force=false
```

### 8.2 Luồng xử lý

Duyệt qua **tất cả** call records trong lịch sử:

1. **Transcribe** (nếu có recording nhưng chưa có transcript)
2. **Analyze Sentiment** (nếu có transcript nhưng chưa có sentiment):
   - Kiểm tra: `OverallSentiment != Neutral` hoặc `SentimentBreakdown` có giá trị > 0 → **đã có sentiment**
   - Nếu `force=true` → phân tích lại bất kể
   - Gọi `AnalyzeAsync(record.RecordingTranscript)` trên **toàn bộ transcript text**

### 8.3 Mapping kết quả cho single-text analysis

```csharp
record.SentimentBreakdown = new SentimentBreakdown
{
    PositivePercent = (label == Positive) ? confidence * 100 : 0,
    NeutralPercent  = (label == Neutral)  ? confidence * 100 : 0,
    NegativePercent = (label == Negative) ? confidence * 100 : 0
};
```

Sau đó **normalize** để tổng = 100%:
- Phần còn lại (100 - positive - negative) được gán cho `NeutralPercent`

### 8.4 Khác biệt so với real-time

| Aspect | Real-time (trong cuộc gọi) | Batch (lịch sử) |
|--------|---------------------------|------------------|
| Input | Từng câu nói + 5s context | Toàn bộ transcript text |
| Granularity | Per-entry | Per-call |
| Breakdown | Count-based từ nhiều entries | Single-result mapped |
| Trigger | Tự động (fire-and-forget) | Manual (API call) |

---

## 9. API Endpoints trả về Sentiment

### 9.1 GET /api/Call/active

**File:** `ContactCenter-API/Controllers/CallController.cs`

Trả về danh sách cuộc gọi đang hoạt động, mỗi transcript entry kèm sentiment:

```json
{
  "count": 2,
  "maxConcurrent": 5,
  "calls": [
    {
      "callConnectionId": "abc-123",
      "transcriptEntries": [
        {
          "speaker": "AI",
          "text": "Xin chào, tôi có thể giúp gì cho bạn?",
          "timestamp": "2026-03-16T10:30:00Z",
          "sentiment": { "label": "Neutral", "confidence": 0.85 },
          "emotion": null
        }
      ]
    }
  ]
}
```

### 9.2 GET /api/CallHistory/{id}

Trả về chi tiết cuộc gọi lịch sử với `overallSentiment` và `sentimentBreakdown`.

### 9.3 GET /api/CallHistory

Trả về danh sách tóm tắt, mỗi item có `overallSentiment` (string).

### 9.4 POST /api/CallHistory/batch-process

Batch transcribe + analyze sentiment (xem mục 8).

---

## 10. SignalR Real-time Communication

**File:** `ContactCenter-API/Hubs/TranscriptHub.cs`

### 10.1 Hub

Hub rất đơn giản, chỉ quản lý group:
- `JoinCall(callConnectionId)` — client join group theo cuộc gọi
- `LeaveCall(callConnectionId)` — client leave group

### 10.2 Server → Client Events

Sentiment được push từ `AzureOpenAIService` thông qua `IHubContext<TranscriptHub>`:

**Event name:** `"SentimentUpdate"`

**Payload:**
```json
{
  "callConnectionId": "abc-123",
  "entryTimestamp": "2026-03-16T10:30:05.000Z",
  "sentiment": {
    "label": "Positive",    // hoặc 0 (enum value)
    "confidence": 0.92
  }
}
```

**Gửi đến:** Group cụ thể của cuộc gọi (`_callConnectionId`)

---

## 11. Frontend — JavaScript (site.js)

**File:** `ContactCenter-APP/wwwroot/js/site.js`

### 11.1 Cấu trúc dữ liệu per-call

```javascript
calls[callId] = {
    // ... các field khác ...
    sentimentData: [],           // Mảng số [-1, 0, +1] tương ứng từng entry
    transcriptEntries: [         // Mảng entry, mỗi entry có .sentiment
        { speaker, text, timestamp, sentiment: { label, confidence } }
    ]
};
```

### 11.2 SignalR Listener

```javascript
connection.on("SentimentUpdate", function (data) {
    handleSentimentUpdate(data.callConnectionId, data.entryTimestamp, data.sentiment);
});
```

### 11.3 Function `handleSentimentUpdate(callConnectionId, entryTimestamp, sentiment)`

1. Tìm entry trong `calls[callConnectionId].transcriptEntries` theo `timestamp` (so sánh ISO string)
2. Cập nhật `transcriptEntries[i].sentiment = sentiment`
3. Cập nhật `sentimentData[i] = sentimentToNumber(sentiment.label)`
4. Cập nhật DOM: tìm `.chat-bubble` có `data-timestamp` khớp → đổi class `.sentiment-dot`
5. Vẽ lại `drawSentimentGraph(callConnectionId)`

### 11.4 Function `sentimentToNumber(label)`

Chuyển đổi label → giá trị số để vẽ graph:

| Input (enum hoặc string) | Output |
|--------------------------|--------|
| `0` / `"Positive"` | `+1` |
| `1` / `"Neutral"` | `0` |
| `2` / `"Negative"` | `-1` |

### 11.5 Function `getSentimentDotClass(label)`

| Input | CSS Class | Màu |
|-------|-----------|-----|
| `0` / `"Positive"` | `sentiment-dot-positive` | Xanh lá |
| `2` / `"Negative"` | `sentiment-dot-negative` | Đỏ |
| Khác | `sentiment-dot-neutral` | Xám |

### 11.6 Function `handleTranscriptUpdate(entry)`

Khi nhận transcript mới:
1. Push entry vào `calls[id].transcriptEntries`
2. Nếu entry có sentiment → `sentimentData.push(sentimentToNumber(label))`
3. Nếu không → `sentimentData.push(0)` (giữ chỗ, sẽ cập nhật khi SentimentUpdate đến)
4. Tạo chat bubble với sentiment dot
5. Vẽ lại sentiment graph

### 11.7 Function `drawSentimentGraph(callConnectionId)`

**Canvas-based rolling line chart:**

- **Kích thước**: Lấy từ parent element, chiều cao 80px
- **DPR-aware**: Nhân với `devicePixelRatio` cho Retina display
- **Max points**: 40 điểm (nếu data > 40 → slice lấy 40 điểm cuối)
- **Trục Y**: +1 (Positive) ↔ 0 (Neutral) ↔ -1 (Negative)
- **Grid lines**: 3 đường ngang (top, middle, bottom)
- **Labels**: `+`, `0`, `−` ở bên trái
- **Area fill**: Vùng giữa đường line và trục 0, màu xanh mờ `rgba(59,130,246,0.08)`
- **Line**: Đường nối các điểm dữ liệu
- **Dots**: Chấm tròn tại mỗi điểm, màu theo sentiment (xanh/xám/đỏ)

### 11.8 KPI Widget — renderKPIs()

```javascript
if (kpiState.sentimentScores.length > 0) {
    var avgSent = kpiState.sentimentScores.reduce((a, b) => a + b, 0)
                  / kpiState.sentimentScores.length;
    var pctPos = Math.round(((avgSent + 1) / 2) * 100);   // Map [-1,+1] → [0%,100%]
    sentimentEl.textContent = pctPos + "%";
}
```

**Công thức:** `pctPos = ((avg + 1) / 2) * 100`

| `avg` score | `pctPos` | Ý nghĩa |
|------------|---------|---------|
| +1.0 | 100% | Hoàn toàn tích cực |
| 0.0 | 50% | Trung lập |
| -1.0 | 0% | Hoàn toàn tiêu cực |

Khi cuộc gọi kết thúc, average score của cuộc gọi đó được push vào `kpiState.sentimentScores`, sau đó tính trung bình các cuộc gọi.

### 11.9 Call History Detail — Sentiment Badge & Breakdown

**Badge:**
```javascript
var badge = document.getElementById("detailSentimentBadge");
badge.textContent = getSentimentLabel(record.overallSentiment);  // "Positive", "Neutral", "Negative"
badge.className = "sentiment-badge-large " + getSentimentClass(record.overallSentiment);
```

**Breakdown bars:**
```javascript
if (record.sentimentBreakdown) {
    setBar("positiveBar", record.sentimentBreakdown.positivePercent);
    setBar("neutralBar",  record.sentimentBreakdown.neutralPercent);
    setBar("negativeBar", record.sentimentBreakdown.negativePercent);
}
```

### 11.10 Helper Functions

| Function | Mô tả |
|----------|-------|
| `getSentimentClass(sentiment)` | Trả về CSS class: `"positive"`, `"neutral"`, `"negative"` |
| `getSentimentLabel(sentiment)` | Normalize label thành string: `"Positive"`, `"Neutral"`, `"Negative"` — hỗ trợ cả enum value (0/1/2) và string |
| `setBar(elementId, value)` | Set width + text cho progress bar |

---

## 12. Frontend — HTML (Index.cshtml)

**File:** `ContactCenter-APP/Pages/Index.cshtml`

### 12.1 KPI Card (dòng 231–239)

```html
<div class="kpi-card">
    <div class="kpi-icon kpi-icon-sentiment">
        <!-- Smiley face SVG icon -->
    </div>
    <div class="kpi-value" id="kpiSentiment">—</div>
    <div class="kpi-label">Sentiment</div>
</div>
```

### 12.2 Sentiment Timeline Canvas (dòng 312–323)

```html
<div class="sentiment-graph-container">
    <div class="sentiment-graph-header">
        <span class="small fw-bold">Sentiment Timeline</span>
        <span class="sentiment-legend">
            <span class="legend-dot legend-positive"></span> Positive
            <span class="legend-dot legend-neutral"></span> Neutral
            <span class="legend-dot legend-negative"></span> Negative
        </span>
    </div>
    <canvas id="sentimentCanvas" class="sentiment-canvas" height="80"></canvas>
</div>
```

### 12.3 Call Detail — Sentiment Badge (dòng 383)

```html
<span id="detailSentimentBadge" class="sentiment-badge-large"></span>
```

### 12.4 Call Detail — Sentiment Breakdown Bars (dòng 426–440)

```html
<h6 class="detail-section-title">Sentiment</h6>
<div class="breakdown-bar-row">
    <span class="bar-label">Positive</span>
    <div class="progress" style="height: 16px;">
        <div class="progress-bar bg-success" id="positiveBar" style="width: 0%">0%</div>
    </div>
</div>
<div class="breakdown-bar-row">
    <span class="bar-label">Neutral</span>
    <div class="progress" style="height: 16px;">
        <div class="progress-bar bg-secondary" id="neutralBar" style="width: 0%">0%</div>
    </div>
</div>
<div class="breakdown-bar-row">
    <span class="bar-label">Negative</span>
    <div class="progress" style="height: 16px;">
        <div class="progress-bar bg-danger" id="negativeBar" style="width: 0%">0%</div>
    </div>
</div>
```

### 12.5 Batch Process Button (dòng 542)

```html
<button id="batchProcessBtn" type="button"
        class="btn btn-sm btn-outline-primary"
        title="Transcribe & analyze sentiment for all calls">
```

---

## 13. Frontend — CSS (site.css)

**File:** `ContactCenter-APP/wwwroot/css/site.css`

### 13.1 Sentiment Dot (trên chat bubble)

```css
.sentiment-dot {
    width: 8px; height: 8px; border-radius: 50%;
    display: inline-block; margin-left: 6px;
}
.sentiment-dot-positive { background: var(--accent-green); }
.sentiment-dot-neutral  { background: var(--neutral-gray); }
.sentiment-dot-negative { background: var(--accent-red); }
.sentiment-dot-loading  { /* animation pulse khi đang chờ kết quả */ }
```

### 13.2 Sentiment Graph Container

```css
.sentiment-graph-container { /* container cho canvas chart */ }
.sentiment-graph-header    { /* header với title + legend */ }
.sentiment-legend          { /* legend items */ }
.sentiment-canvas          { /* canvas element styling */ }
```

### 13.3 Sentiment Badge (History)

```css
.sentiment-badge-large {
    /* Badge hiển thị trong history list và detail panel */
}
.sentiment-badge-large.positive { background: #dcfce7; color: #166534; }
.sentiment-badge-large.neutral  { background: #f1f5f9; color: #475569; }
.sentiment-badge-large.negative { background: #fef2f2; color: #991b1b; }
```

### 13.4 KPI Icon

```css
.kpi-icon-sentiment { background: rgba(34,197,94,0.1); color: var(--accent-green); }
```

---

## 14. Cấu hình (Configuration)

**File:** `ContactCenter-API/appsettings.json`

```json
{
  "AzureOpenAI": {
    "EndpointUri": "",                    // Azure OpenAI endpoint (Realtime API)
    "Key": "",                            // API Key (Realtime API)
    "DeploymentName": "gpt-realtime-mini", // Realtime model
    "ChatEndpointUri": "",                // Chat endpoint (ưu tiên cho Sentiment)
    "ChatKey": "",                        // Chat API key (ưu tiên cho Sentiment)
    "ChatDeployment": "gpt-4o-mini"       // Chat model mặc định
  }
}
```

### Config key tra cứu (Sentiment-specific)

| Config Key | Mô tả | Fallback |
|-----------|-------|---------|
| `AzureOpenAI:SentimentDeployment` | Deployment riêng cho sentiment (nếu muốn dùng model khác) | `AzureOpenAI:ChatDeployment` → `AzureOpenAI:DeploymentName` |
| `AzureOpenAI:ChatEndpointUri` | Endpoint cho Chat API | `AzureOpenAI:EndpointUri` |
| `AzureOpenAI:ChatKey` | API key cho Chat API | `AzureOpenAI:Key` |

---

## 15. Dependency Injection

**File:** `ContactCenter-API/Program.cs`

```csharp
builder.Services.AddSingleton<SentimentAnalysisService>();
```

- Đăng ký là **Singleton** — một instance duy nhất cho toàn bộ ứng dụng
- Được inject vào:
  - `CallService` → truyền cho `AzureOpenAIService` (real-time analysis)
  - `CallHistoryController` → batch processing
  - `acsMediaStreamingHandler` → truyền cho `AzureOpenAIService`
  - `FreeSwitchMediaHandler` → truyền cho `AzureOpenAIService`

---

## 16. Sơ đồ luồng dữ liệu End-to-End

### 16.1 Luồng Real-time (trong cuộc gọi)

```
   Người gọi                        Hệ thống                          Frontend
   ─────────                        ─────────                          ────────
       │                                │                                  │
       │   Audio stream (PCM16)         │                                  │
       │ ─────────────────────────────► │                                  │
       │                                │                                  │
       │                  ┌─────────────┴─────────────────┐                │
       │                  │ OpenAI Realtime API            │                │
       │                  │ (Whisper transcription)        │                │
       │                  └─────────────┬─────────────────┘                │
       │                                │                                  │
       │                  TranscriptEntry created                          │
       │                  (Speaker=Recipient, Text="...")                   │
       │                                │                                  │
       │                    ┌───────────┤                                  │
       │                    │           │                                  │
       │          SignalR   │           │ FireAndForgetSentiment()         │
       │        "Transcript │           │     │                            │
       │         Update"    │           │     │ Build 5s context window    │
       │                    │           │     │                            │
       │                    │           │     ▼                            │
       │                    │    ┌──────┴───────────────────┐              │
       │                    │    │ SentimentAnalysisService │              │
       │                    │    │ → Azure OpenAI Chat API  │              │
       │                    │    │ → JSON: {label,confidence}│              │
       │                    │    └──────┬───────────────────┘              │
       │                    │           │                                  │
       │                    │           │ entry.Sentiment = result         │
       │                    │           │                                  │
       │                    │    SignalR "SentimentUpdate"                 │
       │                    │    {callId, timestamp, sentiment}            │
       │                    │           │                                  │
       │                    ▼           ▼                                  │
       │                    ───────────────────────────────────►           │
       │                                                       ┌──────────┤
       │                                                       │ Update:  │
       │                                                       │ - Dot    │
       │                                                       │ - Graph  │
       │                                                       │ - KPI    │
       │                                                       └──────────┤
       │                                                                  │
```

### 16.2 Luồng khi kết thúc cuộc gọi

```
  CallService.PersistHistoryIfNeededAsync()
       │
       ▼
  entries = activeCall.TranscriptEntries
       │
       ├──► OverallSentiment = GroupBy(Label).OrderByDesc(Count).First()
       │                        (majority voting)
       │
       ├──► SentimentBreakdown = {
       │        PositivePercent: count(Positive)/total × 100,
       │        NeutralPercent:  count(Neutral)/total × 100,
       │        NegativePercent: count(Negative)/total × 100
       │    }
       │
       ▼
  CallRecord saved → CallHistoryService.SaveCallRecordAsync()
       │
       ▼
  Persistent storage (file/blob)
```

### 16.3 Luồng Batch Processing

```
  POST /api/CallHistory/batch-process
       │
       ▼
  Duyệt tất cả CallRecord
       │
       ├──► [Nếu có recording, chưa có transcript] → Transcribe
       │
       ├──► [Nếu có transcript, chưa có sentiment] →
       │       SentimentAnalysisService.AnalyzeAsync(fullTranscript)
       │           │
       │           ▼
       │       OverallSentiment = result.Label
       │       SentimentBreakdown = single-result mapped + normalized
       │
       ▼
  SaveCallRecordAsync() → cập nhật lưu trữ
```

---

## 17. Xử lý lỗi & Fallback

| Tình huống | Xử lý |
|-----------|-------|
| Service chưa cấu hình (`IsConfigured = false`) | Trả `Neutral / Confidence=0` |
| Text input null/empty | Trả `Neutral / Confidence=0` |
| Azure OpenAI trả lỗi HTTP | Log warning, trả `Neutral / Confidence=0` |
| Model không hỗ trợ tham số mới (`max_completion_tokens`) | Tự động retry với tham số legacy (`max_tokens`) |
| Deployment không tồn tại (HTTP 404 DeploymentNotFound) | Log warning, trả `null` |
| JSON response không parse được | Thử `ExtractJsonObject()` → parse lại; nếu vẫn lỗi → `Neutral / 0` |
| `FireAndForgetSentiment()` exception | Log warning, không ảnh hưởng cuộc gọi chính |
| `SentimentAnalysisService` là null (optional dependency) | Skip hoàn toàn, không phân tích |

**Nguyên tắc chung:** Sentiment analysis **không bao giờ** làm crash cuộc gọi. Mọi lỗi đều được handle gracefully với giá trị mặc định `Neutral`.

---

*Tài liệu được tạo tự động từ source code tại thời điểm 2026-03-16.*
