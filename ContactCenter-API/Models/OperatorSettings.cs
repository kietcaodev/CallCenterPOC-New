namespace ContactCenterPOC.Models
{
    public class OperatorSettings
    {
        public double MaxCallTimeMinutes { get; set; } = 2.0;
        public string VoiceApiMode { get; set; } = "ChatGPT";  // "ChatGPT", "VoiceLive", or "GeminiLive"
        public string SelectedVoice { get; set; } = "alloy";   // alloy, echo, fable, onyx, nova, shimmer

        // VoiceLive-specific fields
        public string TranscriptionMode { get; set; } = "BuiltIn";  // "BuiltIn" or "SeparateSTT"
        public string VoiceLiveModel { get; set; } = "gpt-4o";
        public string SelectedVoiceLiveVoice { get; set; } = "en-US-Ava:DragonHDLatestNeural";

        // GeminiLive-specific fields
        public string GeminiLiveModel { get; set; } = "gemini-2.5-flash-native-audio-preview-12-2025";
        public string GeminiLiveVoice { get; set; } = "Puck";

        // Inbound call script mapping
        public string? InboundCampaignId { get; set; }        // Campaign to use for inbound calls (null = custom prompt or system default)
        public string? InboundCustomPrompt { get; set; }      // Custom prompt for inbound calls (takes priority over campaign)

        public static readonly HashSet<string> ValidVoices = new(StringComparer.OrdinalIgnoreCase)
        {
            "alloy", "echo", "fable", "onyx", "nova", "shimmer"
        };

        public static readonly HashSet<string> ValidVoiceLiveModels = new(StringComparer.OrdinalIgnoreCase)
        {
            "gpt-4o", "gpt-4.1", "gpt-5"
        };

        public static readonly HashSet<string> ValidTranscriptionModes = new(StringComparer.OrdinalIgnoreCase)
        {
            "BuiltIn", "SeparateSTT"
        };

        public static readonly string[] ValidGeminiVoices = new[]
        {
            "Puck", "Charon", "Kore", "Fenrir", "Aoede"
        };
    }
}
