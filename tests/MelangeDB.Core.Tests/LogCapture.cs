using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Captures log entries with their structured state, not only their rendered message: an alert
/// keys on the fields, so a test that only matched the text would pass while the fields it exists
/// to guarantee were missing.
/// </summary>
internal sealed class LogCapture : ILoggerFactory
{
    public ConcurrentQueue<LogEntry> Entries { get; } = [];

    public LogEntry Single(int eventId) => Entries.Single(e => e.EventId == eventId);

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(LogCapture owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var fields = state is IReadOnlyList<KeyValuePair<string, object?>> pairs
                ? pairs.Where(p => p.Key != "{OriginalFormat}").ToDictionary(p => p.Key, p => p.Value)
                : [];
            owner.Entries.Enqueue(new LogEntry(
                eventId.Id,
                eventId.Name ?? string.Empty,
                logLevel,
                formatter(state, exception),
                fields));
        }
    }
}

internal sealed record LogEntry(
    int EventId,
    string EventName,
    LogLevel Level,
    string Message,
    IReadOnlyDictionary<string, object?> Fields)
{
    public double Number(string field) => Convert.ToDouble(Fields[field], System.Globalization.CultureInfo.InvariantCulture);
}
