using System.ComponentModel.DataAnnotations;

namespace ContactCenterPOC.Models
{
    public class CallbackEvent
    {
        public string CallConnectionId { get; set; } = string.Empty;
        public string EventType { get; set; } = string.Empty;
        public DateTimeOffset Timestamp { get; set; }
        public CallbackEventData Data { get; set; } = new();
    }

    public class CallbackEventData
    {
        public string CallState { get; set; } = string.Empty;
        public string OperationContext { get; set; } = string.Empty;
        public string ResultCode { get; set; } = string.Empty;
        public string ResultSubcode { get; set; } = string.Empty;
    }

    public class CallRequest
    {
        [Required(ErrorMessage = "At least one phone number is required")]
        [MinLength(1, ErrorMessage = "At least one phone number is required")]
        [MaxLength(2, ErrorMessage = "Maximum of 2 phone numbers allowed")]
        public string[] PhoneNumbers { get; set; } = Array.Empty<string>();

        public string[]? ContactNames { get; set; }

        public string? CampaignId { get; set; }

        public string? Prompt { get; set; }
    }
}
