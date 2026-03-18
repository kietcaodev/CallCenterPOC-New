namespace ContactCenterPOC.Models
{
    public class GeminiLiveConfig
    {
        public string ApiKey { get; set; } = "";
        public string Model { get; set; } = "gemini-2.5-flash-native-audio-preview-12-2025";
        public string Voice { get; set; } = "Puck";
        public bool IsConfigured => !string.IsNullOrWhiteSpace(ApiKey);
    }
}
