using ContactCenterPOC.Hubs;
using ContactCenterPOC.Models;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;


namespace ContactCenterPOC.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CallbackController : ControllerBase
    {
        private readonly ILogger<CallbackController> _logger;
        private readonly CallService _callService;
        private readonly CallHistoryService _callHistoryService;
        private readonly IHubContext<TranscriptHub> _hubContext;

        public CallbackController(ILogger<CallbackController> logger, CallService callService, CallHistoryService callHistoryService, IHubContext<TranscriptHub> hubContext)
        {
            _logger = logger;
            _callService = callService;
            _callHistoryService = callHistoryService;
            _hubContext = hubContext;
        }

        /// <summary>
        /// WebSocket endpoint for FreeSWITCH audio streaming.
        /// FreeSWITCH connects here with binary PCM 16kHz audio.
        /// Query params:
        ///   callId       - FreeSWITCH call UUID (required)
        ///   callerNumber - caller phone number (optional)
        ///   calledNumber - callee phone number (optional)  
        ///   campaignId   - campaign to use for AI prompt (optional)
        ///   contactName  - name of the contact (optional)
        /// </summary>
        [HttpGet("ws")]
        public async Task<IActionResult> Get(
            [FromQuery] string? callId,
            [FromQuery] string? callerNumber,
            [FromQuery] string? calledNumber,
            [FromQuery] string? campaignId,
            [FromQuery] string? contactName)
        {
            if (!HttpContext.WebSockets.IsWebSocketRequest)
            {
                _logger.LogWarning("Non-WebSocket request received on WS endpoint");
                return BadRequest("WebSocket connection required");
            }

            // Generate callId if FreeSWITCH didn't provide one
            var effectiveCallId = callId ?? Guid.NewGuid().ToString();

            _logger.LogInformation(
                "[{CallId}] FreeSWITCH WebSocket connection: caller={Caller}, called={Called}, campaign={Campaign}",
                effectiveCallId, callerNumber, calledNumber, campaignId);

            await _callService.StartCallInteraction(
                HttpContext, effectiveCallId, callerNumber, calledNumber, campaignId, contactName);

            return new EmptyResult();
        }



        

    }


}
