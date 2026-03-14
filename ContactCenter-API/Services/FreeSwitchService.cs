using NEventSocket;
using NEventSocket.FreeSwitch;
using System.Reactive.Linq;

namespace ContactCenterPOC.Services
{
    /// <summary>
    /// Manages FreeSWITCH ESL connections for call origination, hangup, and event handling.
    /// Replaces Azure Communication Services CallAutomationClient.
    /// </summary>
    public class FreeSwitchService : IDisposable
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<FreeSwitchService> _logger;
        private InboundSocket? _commandConn;
        private InboundSocket? _eventConn;
        private readonly SemaphoreSlim _reconnectLock = new(1, 1);
        private int _reconnectAttempts;
        private const int MaxReconnectAttempts = 5;
        private const int ReconnectDelayMs = 5000;
        private bool _isDisposed;

        // Events for call lifecycle
        public event Action<string>? CallAnswered;       // uuid
        public event Action<string, string>? CallHangup;  // uuid, cause
        public event Action<string>? CallFailed;          // uuid

        public FreeSwitchService(IConfiguration configuration, ILogger<FreeSwitchService> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task ConnectAsync()
        {
            var host = _configuration["FreeSWITCH:Host"] ?? "127.0.0.1";
            var port = int.Parse(_configuration["FreeSWITCH:Port"] ?? "8021");
            var password = _configuration["FreeSWITCH:Password"] ?? "ClueCon";

            try
            {
                _commandConn = await InboundSocket.Connect(host, port, password);
                _logger.LogInformation("FreeSWITCH command connection established ({Host}:{Port})", host, port);

                _eventConn = await InboundSocket.Connect(host, port, password);
                await _eventConn.SubscribeEvents(
                    EventName.ChannelAnswer,
                    EventName.ChannelHangupComplete,
                    EventName.ChannelState);

                SetupEventHandlers();
                _logger.LogInformation("FreeSWITCH event connection established");
                _reconnectAttempts = 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to connect to FreeSWITCH at {Host}:{Port}", host, port);
                _ = TryReconnectAsync();
            }
        }

        private void SetupEventHandlers()
        {
            if (_eventConn == null) return;

            _eventConn.Events.Where(x => x.EventName == EventName.ChannelAnswer)
                .Subscribe(evt =>
                {
                    var uuid = evt.Headers.ContainsKey("Unique-ID") ? evt.Headers["Unique-ID"] : "";
                    if (!string.IsNullOrEmpty(uuid))
                    {
                        _logger.LogInformation("FreeSWITCH call answered: {UUID}", uuid);
                        CallAnswered?.Invoke(uuid);
                    }
                });

            _eventConn.Events.Where(x => x.EventName == EventName.ChannelHangupComplete)
                .Subscribe(evt =>
                {
                    var uuid = evt.Headers.ContainsKey("Unique-ID") ? evt.Headers["Unique-ID"] : "";
                    var cause = evt.Headers.ContainsKey("Hangup-Cause") ? evt.Headers["Hangup-Cause"] : "unknown";
                    if (!string.IsNullOrEmpty(uuid))
                    {
                        _logger.LogInformation("FreeSWITCH call hangup: {UUID}, cause: {Cause}", uuid, cause);
                        if (cause == "ORIGINATOR_CANCEL" || cause == "NO_ANSWER" || cause == "USER_BUSY" ||
                            cause == "NO_ROUTE_DESTINATION" || cause == "CALL_REJECTED")
                        {
                            CallFailed?.Invoke(uuid);
                        }
                        else
                        {
                            CallHangup?.Invoke(uuid, cause);
                        }
                    }
                });
        }

        /// <summary>
        /// Originate an outbound call via SIP trunk.
        /// When customer answers, FreeSWITCH auto-transfers to the mod_audio_stream dialplan.
        /// Returns the call UUID.
        /// </summary>
        public async Task<string> OriginateOutboundCallAsync(string targetPhoneNumber)
        {
            if (_commandConn == null)
                throw new InvalidOperationException("FreeSWITCH not connected");

            var gateway = _configuration["FreeSWITCH:SipGateway"] ?? "default";
            var dialPrefix = _configuration["FreeSWITCH:DialPrefix"] ?? "";
            var callerIdNumber = _configuration["FreeSWITCH:CallerIdNumber"] ?? "0000000000";
            var callerIdName = _configuration["FreeSWITCH:CallerIdName"] ?? callerIdNumber;
            var transferTarget = _configuration["FreeSWITCH:TransferTarget"] ?? "1800123456";
            var transferContext = _configuration["FreeSWITCH:TransferContext"] ?? "default";

            // Convert E.164 (+84399726129) to local format (0399726129)
            var localNumber = ConvertToLocalNumber(targetPhoneNumber);
            var dialString = $"{dialPrefix}{localNumber}";
            var uuid = Guid.NewGuid().ToString();

            // When customer answers, auto-transfer to the dialplan that runs mod_audio_stream
            var originateCmd = $"originate " +
                $"{{origination_uuid={uuid}," +
                $"origination_caller_id_number={callerIdNumber}," +
                $"origination_caller_id_name={callerIdName}," +
                $"effective_caller_id_number={callerIdNumber}," +
                $"effective_caller_id_name={callerIdName}" +
                $"}}sofia/gateway/{gateway}/{dialString} &transfer({transferTarget} XML {transferContext})";

            _logger.LogInformation("Originating outbound call: uuid={UUID}, target={Target}, dial={Dial}, gateway={Gateway}, transfer={Transfer}@{Context}",
                uuid, targetPhoneNumber, dialString, gateway, transferTarget, transferContext);

            var result = await _commandConn.SendApi(originateCmd);
            var body = result.BodyText?.Trim() ?? "";

            if (body.StartsWith("+OK"))
            {
                _logger.LogInformation("Call originated successfully: {UUID}", uuid);
                return uuid;
            }
            else if (body.StartsWith("-ERR"))
            {
                throw new InvalidOperationException($"FreeSWITCH originate failed: {body}");
            }

            return uuid;
        }

        /// <summary>
        /// Transfer a call to an extension (e.g., 1800123456 which runs mod_audio_stream dialplan).
        /// </summary>
        public async Task TransferCallAsync(string uuid, string? targetExtension = null)
        {
            if (_commandConn == null)
                throw new InvalidOperationException("FreeSWITCH not connected");

            var target = targetExtension ?? _configuration["FreeSWITCH:TransferTarget"] ?? "1800123456";
            var cmd = $"uuid_transfer {uuid} {target}";
            _logger.LogInformation("Transferring call {UUID} to {Target}", uuid, target);

            var result = await _commandConn.SendApi(cmd);
            var body = result.BodyText?.Trim() ?? "";
            _logger.LogInformation("Transfer result for {UUID}: {Result}", uuid, body);
        }

        /// <summary>
        /// Convert E.164 phone number to local Vietnamese format.
        /// +84399726129 → 0399726129
        /// </summary>
        private string ConvertToLocalNumber(string e164Number)
        {
            var countryCode = _configuration["FreeSWITCH:CountryCode"] ?? "84";
            var stripped = e164Number.TrimStart('+');
            if (stripped.StartsWith(countryCode))
            {
                return "0" + stripped.Substring(countryCode.Length);
            }
            return stripped;
        }

        /// <summary>
        /// Hang up a call by UUID.
        /// </summary>
        public async Task HangUpAsync(string uuid)
        {
            if (_commandConn == null)
            {
                _logger.LogWarning("Cannot hang up {UUID}: FreeSWITCH not connected", uuid);
                return;
            }

            try
            {
                var result = await _commandConn.SendApi($"uuid_kill {uuid}");
                _logger.LogInformation("Hangup {UUID}: {Result}", uuid, result.BodyText?.Trim());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to hang up call {UUID}", uuid);
            }
        }

        /// <summary>
        /// Send audio file to FreeSWITCH for playback on the call.
        /// Auto-reconnects if ESL connection is down.
        /// </summary>
        public async Task<string> PlayAudioAsync(string uuid, string filePath)
        {
            // Try reconnect if not connected
            if (_commandConn == null)
            {
                _logger.LogWarning("PlayAudioAsync: ESL command connection is null, attempting reconnect...");
                await ConnectAsync();
            }

            if (_commandConn == null)
            {
                _logger.LogError("PlayAudioAsync: ESL reconnect failed, cannot play audio for {UUID}", uuid);
                return "-ERR not connected";
            }

            var cmd = $"uuid_broadcast {uuid} {filePath} aleg";
            _logger.LogInformation("PlayAudioAsync: Sending ESL command: {Cmd}", cmd);
            var result = await _commandConn.SendApi(cmd);
            var body = result.BodyText?.Trim() ?? "(no body)";
            _logger.LogInformation("PlayAudioAsync: ESL response: {Response}", body);
            return body;
        }

        public bool IsConnected => _commandConn != null;

        private async Task TryReconnectAsync()
        {
            if (_isDisposed || _reconnectAttempts >= MaxReconnectAttempts) return;

            await _reconnectLock.WaitAsync();
            try
            {
                _reconnectAttempts++;
                var delay = ReconnectDelayMs * _reconnectAttempts;
                _logger.LogInformation("Reconnecting to FreeSWITCH in {Delay}ms (attempt {Attempt})", delay, _reconnectAttempts);
                await Task.Delay(delay);
                await ConnectAsync();
            }
            finally
            {
                _reconnectLock.Release();
            }
        }

        public void Dispose()
        {
            _isDisposed = true;
            _commandConn?.Dispose();
            _eventConn?.Dispose();
            _reconnectLock.Dispose();
        }
    }
}
