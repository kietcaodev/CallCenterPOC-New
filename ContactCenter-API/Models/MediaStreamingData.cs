using System.Text.Json;

namespace ContactCenterPOC.Models
{
    /// <summary>
    /// Helper methods to generate media streaming JSON messages, replacing
    /// Azure.Communication.CallAutomation.OutStreamingData / StreamingData.
    /// </summary>
    public static class MediaStreamingData
    {
        public static string GetAudioDataForOutbound(byte[] audioBytes)
        {
            var base64 = Convert.ToBase64String(audioBytes);
            return JsonSerializer.Serialize(new
            {
                kind = "AudioData",
                audioData = new { data = base64 }
            });
        }

        public static string GetStopAudioForOutbound()
        {
            return JsonSerializer.Serialize(new
            {
                kind = "StopAudio",
                stopAudio = new { }
            });
        }

        /// <summary>
        /// Parse incoming audio data JSON and extract raw PCM bytes.
        /// Returns null if not an audio data message or if silent.
        /// </summary>
        public static byte[]? ParseAudioData(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty("kind", out var kind) && kind.GetString() == "AudioData")
                {
                    if (root.TryGetProperty("audioData", out var audioData))
                    {
                        // Check for silence flag
                        if (audioData.TryGetProperty("isSilent", out var isSilent) && isSilent.GetBoolean())
                            return null;

                        if (audioData.TryGetProperty("data", out var data))
                        {
                            var base64 = data.GetString();
                            if (!string.IsNullOrEmpty(base64))
                                return Convert.FromBase64String(base64);
                        }
                    }
                }
            }
            catch
            {
                // Not valid JSON or unexpected format
            }
            return null;
        }
    }
}
