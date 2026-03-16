namespace ContactCenterPOC.Services
{
    /// <summary>
    /// Reusable call-scoped logger helper that standardizes all log entries with the
    /// FreeSWITCH UUID as the primary correlation key.
    /// 
    /// Output format:
    ///   [{uuid}] message                     (no component)
    ///   [{uuid}][{component}] message        (with component tag: AI, FS, VL, etc.)
    /// 
    /// Usage:
    ///   var log = new CallLogger(_logger, uuid);           // basic
    ///   var log = new CallLogger(_logger, uuid, "AI");     // with component
    ///   log.Info("Session started");
    ///   log.Warn(ex, "Something went wrong");
    /// </summary>
    public readonly struct CallLogger
    {
        private readonly ILogger _logger;
        private readonly string _uuid;
        private readonly string? _component;
        private readonly string _prefix;

        public CallLogger(ILogger logger, string uuid, string? component = null)
        {
            _logger = logger;
            _uuid = uuid;
            _component = component;
            _prefix = component != null ? $"[{uuid}][{component}]" : $"[{uuid}]";
        }

        /// <summary>
        /// Create a new CallLogger with a different component tag, keeping the same UUID.
        /// </summary>
        public CallLogger WithComponent(string component) => new(_logger, _uuid, component);

        // ── Information ────────────────────────────────────────

        public void Info(string message)
            => _logger.LogInformation($"{_prefix} {message}");

        public void Info<T0>(string message, T0 arg0)
            => _logger.LogInformation($"{_prefix} {message}", arg0);

        public void Info<T0, T1>(string message, T0 arg0, T1 arg1)
            => _logger.LogInformation($"{_prefix} {message}", arg0, arg1);

        public void Info<T0, T1, T2>(string message, T0 arg0, T1 arg1, T2 arg2)
            => _logger.LogInformation($"{_prefix} {message}", arg0, arg1, arg2);

        public void Info(string message, params object?[] args)
            => _logger.LogInformation($"{_prefix} {message}", args);

        // ── Warning ────────────────────────────────────────────

        public void Warn(string message)
            => _logger.LogWarning($"{_prefix} {message}");

        public void Warn<T0>(string message, T0 arg0)
            => _logger.LogWarning($"{_prefix} {message}", arg0);

        public void Warn<T0, T1>(string message, T0 arg0, T1 arg1)
            => _logger.LogWarning($"{_prefix} {message}", arg0, arg1);

        public void Warn(Exception? exception, string message)
            => _logger.LogWarning(exception, $"{_prefix} {message}");

        public void Warn<T0>(Exception? exception, string message, T0 arg0)
            => _logger.LogWarning(exception, $"{_prefix} {message}", arg0);

        public void Warn<T0, T1>(Exception? exception, string message, T0 arg0, T1 arg1)
            => _logger.LogWarning(exception, $"{_prefix} {message}", arg0, arg1);

        public void Warn(string message, params object?[] args)
            => _logger.LogWarning($"{_prefix} {message}", args);

        public void Warn(Exception? exception, string message, params object?[] args)
            => _logger.LogWarning(exception, $"{_prefix} {message}", args);

        // ── Error ──────────────────────────────────────────────

        public void Error(string message)
            => _logger.LogError($"{_prefix} {message}");

        public void Error<T0>(string message, T0 arg0)
            => _logger.LogError($"{_prefix} {message}", arg0);

        public void Error<T0, T1>(string message, T0 arg0, T1 arg1)
            => _logger.LogError($"{_prefix} {message}", arg0, arg1);

        public void Error(Exception? exception, string message)
            => _logger.LogError(exception, $"{_prefix} {message}");

        public void Error<T0>(Exception? exception, string message, T0 arg0)
            => _logger.LogError(exception, $"{_prefix} {message}", arg0);

        public void Error<T0, T1>(Exception? exception, string message, T0 arg0, T1 arg1)
            => _logger.LogError(exception, $"{_prefix} {message}", arg0, arg1);

        public void Error(string message, params object?[] args)
            => _logger.LogError($"{_prefix} {message}", args);

        public void Error(Exception? exception, string message, params object?[] args)
            => _logger.LogError(exception, $"{_prefix} {message}", args);

        // ── Debug ──────────────────────────────────────────────

        public void Debug(string message)
            => _logger.LogDebug($"{_prefix} {message}");

        public void Debug<T0>(string message, T0 arg0)
            => _logger.LogDebug($"{_prefix} {message}", arg0);

        public void Debug<T0, T1>(string message, T0 arg0, T1 arg1)
            => _logger.LogDebug($"{_prefix} {message}", arg0, arg1);

        public void Debug(string message, params object?[] args)
            => _logger.LogDebug($"{_prefix} {message}", args);
    }
}
