using System.Net.Http.Headers;
using System.Security;
using System.Text;

namespace ContactCenterPOC.Services
{
    /// <summary>
    /// Azure Cognitive Services Text-to-Speech via REST API.
    /// Returns raw PCM16 audio at 16 kHz mono — the exact format mod_audio_fork expects.
    /// No resampling needed; audio can be fed directly into the FreeSWITCH jitter buffer.
    /// 
    /// REST endpoint: POST https://{region}.tts.speech.microsoft.com/cognitiveservices/v1
    /// Output format : raw-16khz-16bit-mono-pcm
    /// 
    /// Configure in appsettings.json:
    ///   "AzureSpeech": {
    ///     "Enabled": true,
    ///     "SubscriptionKey": "<key>",
    ///     "Region": "eastus",
    ///     "VoiceName": "vi-VN-HoaiMyNeural"
    ///   }
    /// </summary>
    public class AzureTtsService
    {
        private readonly IHttpClientFactory _httpFactory;
        private readonly ILogger<AzureTtsService> _logger;
        private readonly IConfiguration _configuration;

        public AzureTtsService(
            IHttpClientFactory httpFactory,
            ILogger<AzureTtsService> logger,
            IConfiguration configuration)
        {
            _httpFactory = httpFactory;
            _logger = logger;
            _configuration = configuration;
        }

        /// <summary>
        /// Returns true when the AzureSpeech section is configured and Enabled=true.
        /// </summary>
        public bool IsEnabled
        {
            get
            {
                var enabled = _configuration["AzureSpeech:Enabled"];
                if (!bool.TryParse(enabled, out var val) || !val) return false;
                var key = _configuration["AzureSpeech:SubscriptionKey"];
                var region = _configuration["AzureSpeech:Region"];
                return !string.IsNullOrWhiteSpace(key) && !string.IsNullOrWhiteSpace(region);
            }
        }

        /// <summary>
        /// Synthesizes <paramref name="text"/> and returns raw PCM16 bytes at 16 kHz mono.
        /// Returns null on failure (caller should fall back to silence or AI audio).
        /// </summary>
        public async Task<byte[]?> SynthesizeAsync(
            string text,
            string? voiceOverride = null,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(text)) return null;

            var subscriptionKey = _configuration["AzureSpeech:SubscriptionKey"];
            var region = _configuration["AzureSpeech:Region"];
            var voiceName = voiceOverride
                ?? _configuration["AzureSpeech:VoiceName"]
                ?? "vi-VN-HoaiMyNeural";

            if (string.IsNullOrWhiteSpace(subscriptionKey) || string.IsNullOrWhiteSpace(region))
            {
                _logger.LogWarning("[TTS] AzureSpeech not configured (SubscriptionKey or Region missing)");
                return null;
            }

            var url = $"https://{region}.tts.speech.microsoft.com/cognitiveservices/v1";
            var ssml = BuildSsml(text, voiceName);

            try
            {
                var client = _httpFactory.CreateClient("AzureTts");
                using var request = new HttpRequestMessage(HttpMethod.Post, url);
                request.Headers.Add("Ocp-Apim-Subscription-Key", subscriptionKey);
                request.Headers.Add("X-Microsoft-OutputFormat", "raw-16khz-16bit-mono-pcm");
                request.Headers.UserAgent.ParseAdd("CallCenterPOC/1.0");
                request.Content = new StringContent(ssml, Encoding.UTF8, "application/ssml+xml");

                var sw = System.Diagnostics.Stopwatch.StartNew();
                using var response = await client.SendAsync(request, ct);

                if (!response.IsSuccessStatusCode)
                {
                    var body = await response.Content.ReadAsStringAsync(ct);
                    _logger.LogError("[TTS] Synthesis failed: HTTP {Status} — {Body}",
                        (int)response.StatusCode, body);
                    return null;
                }

                var pcmBytes = await response.Content.ReadAsByteArrayAsync(ct);
                _logger.LogInformation("[TTS] Synthesized {Chars} chars → {Bytes}B PCM16 in {Ms}ms (voice={Voice})",
                    text.Length, pcmBytes.Length, sw.ElapsedMilliseconds, voiceName);

                return pcmBytes;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[TTS] Synthesis exception");
                return null;
            }
        }

        /// <summary>
        /// Builds a minimal SSML document with XML-escaped text.
        /// </summary>
        private static string BuildSsml(string text, string voiceName)
        {
            // Determine xml:lang from the voice name (e.g. vi-VN-HoaiMyNeural → vi-VN)
            var langParts = voiceName.Split('-');
            var lang = langParts.Length >= 2 ? $"{langParts[0]}-{langParts[1]}" : "vi-VN";

            var escaped = SecurityElement.Escape(text) ?? text;
            return $"""
                <speak version="1.0" xml:lang="{lang}" xmlns="http://www.w3.org/2001/10/synthesis">
                  <voice name="{voiceName}">{escaped}</voice>
                </speak>
                """;
        }
    }
}
