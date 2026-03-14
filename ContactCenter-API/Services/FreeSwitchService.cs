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
        /// Originate an outbound call via SIP trunk and fork audio to WebSocket.
        /// Returns the call UUID.
        /// </summary>
        public async Task<string> OriginateCallAsync(string targetPhoneNumber, string webSocketUrl)
        {
            if (_commandConn == null)
                throw new InvalidOperationException("FreeSWITCH not connected");

            var gateway = _configuration["FreeSWITCH:SipGateway"] ?? "default";
            var callerIdNumber = _configuration["FreeSWITCH:CallerIdNumber"] ?? "0000000000";
            var callerIdName = _configuration["FreeSWITCH:CallerIdName"] ?? "AI Contact Center";

            // Strip + prefix for SIP dialing if needed
            var dialNumber = targetPhoneNumber.TrimStart('+');

            // Generate a UUID for tracking
            var uuid = Guid.NewGuid().ToString();

            // Originate with audio fork to WebSocket
            // The call will be parked (&park) and audio forked to our WebSocket endpoint
            var originateCmd = $"originate " +
                $"{{origination_uuid={uuid}," +
                $"origination_caller_id_number={callerIdNumber}," +
                $"origination_caller_id_name={callerIdName}," +
                $"execute_on_answer='socket:{webSocketUrl} async full'" +
                $"}}sofia/gateway/{gateway}/{dialNumber} &park()";

            _logger.LogInformation("Originating call: uuid={UUID}, target={Target}, gateway={Gateway}",
                uuid, targetPhoneNumber, gateway);

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
        /// Originate call using audio_fork for WebSocket streaming (mod_audio_fork).
        /// This approach dials the call normally, then forks audio to WS.
        /// </summary>
        public async Task<string> OriginateWithAudioForkAsync(string targetPhoneNumber, string webSocketUrl)
        {
            if (_commandConn == null)
                throw new InvalidOperationException("FreeSWITCH not connected");

            var gateway = _configuration["FreeSWITCH:SipGateway"] ?? "default";
            var callerIdNumber = _configuration["FreeSWITCH:CallerIdNumber"] ?? "0000000000";
            var callerIdName = _configuration["FreeSWITCH:CallerIdName"] ?? "AI Contact Center";
            var dialNumber = targetPhoneNumber.TrimStart('+');
            var uuid = Guid.NewGuid().ToString();

            // Step 1: Originate the call to park
            var originateCmd = $"originate " +
                $"{{origination_uuid={uuid}," +
                $"origination_caller_id_number={callerIdNumber}," +
                $"origination_caller_id_name={callerIdName}" +
                $"}}sofia/gateway/{gateway}/{dialNumber} &park()";

            _logger.LogInformation("Originating call with audio fork: uuid={UUID}, target={Target}", uuid, targetPhoneNumber);

            var result = await _commandConn.SendApi(originateCmd);
            var body = result.BodyText?.Trim() ?? "";

            if (!body.StartsWith("+OK"))
            {
                throw new InvalidOperationException($"FreeSWITCH originate failed: {body}");
            }

            return uuid;
        }

        /// <summary>
        /// Start audio fork on an existing call, streaming to WebSocket endpoint.
        /// </summary>
        public async Task StartAudioForkAsync(string uuid, string webSocketUrl)
        {
            if (_commandConn == null)
                throw new InvalidOperationException("FreeSWITCH not connected");

            // uuid_audio_fork <uuid> start <ws://url> mono 16000
            var cmd = $"uuid_audio_fork {uuid} start {webSocketUrl} mono 16000";
            _logger.LogInformation("Starting audio fork: {UUID} -> {WsUrl}", uuid, webSocketUrl);

            var result = await _commandConn.SendApi(cmd);
            _logger.LogInformation("Audio fork result: {Result}", result.BodyText?.Trim());
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
        /// </summary>
        public async Task PlayAudioAsync(string uuid, string filePath)
        {
            if (_commandConn == null) return;
            var cmd = $"uuid_broadcast {uuid} {filePath} aleg async";
            await _commandConn.SendApi(cmd);
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
