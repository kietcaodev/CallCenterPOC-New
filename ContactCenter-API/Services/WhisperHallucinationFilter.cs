using System.Text.RegularExpressions;

namespace ContactCenterPOC.Services;

/// <summary>
/// Filters out common Whisper hallucination phrases that appear when the model
/// processes short, noisy, or echo-contaminated Vietnamese audio.
/// 
/// Whisper (especially with language="vi") is well known for hallucinating
/// YouTube outro/intro phrases on ambiguous input. This filter checks the
/// transcription text against known hallucination patterns and returns true
/// if the transcript should be discarded.
/// </summary>
public static class WhisperHallucinationFilter
{
    // Known Vietnamese Whisper hallucination fragments (case-insensitive).
    // These are YouTube outro/intro phrases that Whisper commonly generates
    // when it can't clearly decode the audio.
    private static readonly string[] HallucinationFragments =
    [
        "hẹn gặp lại",
        "hẹn mọi người",           // seen in logs: "HẸN MỌI NGƯỜI MỚI THÂN THƯƠNG"
        "thân thương",              // YouTube outro phrase
        "đăng ký kênh",
        "đăng kí kênh",
        "nhớ đăng ký",
        "nhớ đăng kí",
        "video tiếp theo",
        "like share",
        "subscribe",
        "cảm ơn các bạn đã theo dõi",
        "cảm ơn các bạn đã xem",
        "xin chào các bạn",
        "chào mừng các bạn",
        "kênh của mình",
        "bấm nút đăng ký",
        "bấm nút đăng kí",
        "đại gia đình",
        "nhấn chuông",
        "bật chuông",
        "thông báo",
        "chia sẻ video",
        "ủng hộ kênh",
        "xem video",
        "theo dõi kênh",
        "phụ đề",
        "sub việt",
        "hẹn các bạn",             // "Hẹn các bạn trong video sau"
        "gặp lại các bạn",
        "hẹn gặp",
    ];

    // Regex patterns for more complex hallucination detection
    private static readonly Regex[] HallucinationPatterns =
    [
        // Repeated filler words (e.g., "ừm ừm ừm", "à à à")
        new(@"^[\s,.]*(ừm|à|ờ|uh|um|ah|oh)[\s,.]*$", RegexOptions.IgnoreCase | RegexOptions.Compiled),

        // Very short transcripts that are just noise artifacts (1-2 chars)
        new(@"^\s*.{0,2}\s*$", RegexOptions.Compiled),

        // Common Whisper artifacts: just punctuation or whitespace
        new(@"^[\s.,!?…\-–—]+$", RegexOptions.Compiled),

        // "Tạm biệt" on its own — often hallucinated at silence boundaries
        new(@"^\s*tạm\s+biệt\.?\s*$", RegexOptions.IgnoreCase | RegexOptions.Compiled),
    ];

    /// <summary>
    /// Returns true if the transcript is likely a Whisper hallucination
    /// and should be discarded.
    /// </summary>
    public static bool IsHallucination(string? transcript)
    {
        if (string.IsNullOrWhiteSpace(transcript))
            return true;

        var text = transcript.Trim();

        // Check fragment matches (case-insensitive)
        foreach (var fragment in HallucinationFragments)
        {
            if (text.Contains(fragment, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        // Check regex patterns
        foreach (var pattern in HallucinationPatterns)
        {
            if (pattern.IsMatch(text))
                return true;
        }

        return false;
    }
}
