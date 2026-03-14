using System.Collections.Concurrent;

namespace ContactCenterPOC.Services
{
    /// <summary>
    /// Simple file logger provider that writes log entries to a daily rotating file.
    /// Logs go to /opt/CallCenterPOC-New/logs/ (Linux) or ./logs/ (Windows).
    /// </summary>
    public sealed class FileLoggerProvider : ILoggerProvider
    {
        private readonly string _logDirectory;
        private readonly LogLevel _minLevel;
        private readonly ConcurrentDictionary<string, FileLogger> _loggers = new();
        private readonly StreamWriter _writer;
        private readonly object _writeLock = new();
        private readonly string _currentDate;

        public FileLoggerProvider(string logDirectory, LogLevel minLevel = LogLevel.Information)
        {
            _logDirectory = logDirectory;
            _minLevel = minLevel;
            Directory.CreateDirectory(_logDirectory);
            _currentDate = DateTime.Now.ToString("yyyy-MM-dd");
            var filePath = Path.Combine(_logDirectory, $"app-{_currentDate}.log");
            _writer = new StreamWriter(filePath, append: true) { AutoFlush = true };
        }

        public ILogger CreateLogger(string categoryName)
        {
            return _loggers.GetOrAdd(categoryName, name => new FileLogger(name, this));
        }

        internal void WriteEntry(string category, LogLevel level, string message)
        {
            if (level < _minLevel) return;

            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var levelStr = level switch
            {
                LogLevel.Trace => "TRACE",
                LogLevel.Debug => "DEBUG",
                LogLevel.Information => "INFO ",
                LogLevel.Warning => "WARN ",
                LogLevel.Error => "ERROR",
                LogLevel.Critical => "CRIT ",
                _ => "NONE "
            };

            // Shorten category: "ContactCenterPOC.Services.CallService" → "CallService"
            var shortCategory = category.Contains('.') ? category[(category.LastIndexOf('.') + 1)..] : category;
            var line = $"{timestamp} [{levelStr}] [{shortCategory}] {message}";

            lock (_writeLock)
            {
                _writer.WriteLine(line);
            }
        }

        public void Dispose()
        {
            _writer.Dispose();
            _loggers.Clear();
        }
    }

    internal sealed class FileLogger : ILogger
    {
        private readonly string _category;
        private readonly FileLoggerProvider _provider;

        public FileLogger(string category, FileLoggerProvider provider)
        {
            _category = category;
            _provider = provider;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;

            var message = formatter(state, exception);
            if (exception != null)
            {
                message += Environment.NewLine + exception;
            }

            _provider.WriteEntry(_category, logLevel, message);
        }
    }

    public static class FileLoggerExtensions
    {
        public static ILoggingBuilder AddFileLogger(this ILoggingBuilder builder, string logDirectory, LogLevel minLevel = LogLevel.Information)
        {
            builder.AddProvider(new FileLoggerProvider(logDirectory, minLevel));
            return builder;
        }
    }
}
