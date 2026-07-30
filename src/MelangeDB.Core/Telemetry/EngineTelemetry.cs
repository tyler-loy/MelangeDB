using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace MelangeDB.Core;

/// <summary>
/// MelangeDB's built-in signals: one <see cref="ActivitySource"/> and one <see cref="Meter"/>, both
/// named <c>MelangeDB</c>, with zero telemetry package references — both types live in the
/// framework, and the host chooses exporters. Names and cardinality rules are contract; see
/// docs/OBSERVABILITY.md. Caller identity goes on spans, never on metric dimensions.
/// </summary>
internal sealed class EngineTelemetry : IDisposable
{
    /// <summary>The one source name. Hosts subscribe with <c>AddSource("MelangeDB")</c> / <c>AddMeter("MelangeDB")</c>.</summary>
    public const string SourceName = "MelangeDB";

    private static readonly ActivitySource Source = new(SourceName);

    private readonly TelemetryOptions _options;
    private readonly Meter _meter;
    private readonly Counter<long> _transactions;
    private readonly Histogram<double> _reducerDuration;
    private readonly Histogram<double> _commitDuration;
    private readonly Histogram<double> _fsyncDuration;
    private readonly Histogram<long> _writeSetRows;

    public EngineTelemetry(TelemetryOptions options, Func<ulong> headLsn, Func<IEnumerable<(string Applier, long Lag)>> applierLags)
    {
        _options = options;
        _meter = new Meter(SourceName);
        _transactions = _meter.CreateCounter<long>("melange.transactions", "{tx}", "Committed, aborted, and rejected transactions.");
        _reducerDuration = _meter.CreateHistogram<double>("melange.reducer.duration", "ms", "Reducer body plus commit duration.");
        _commitDuration = _meter.CreateHistogram<double>("melange.commit.duration", "ms", "Log append duration, fsync included.");
        _fsyncDuration = _meter.CreateHistogram<double>("melange.fsync.duration", "ms", "Durability flush duration.");
        _writeSetRows = _meter.CreateHistogram<long>("melange.writeset.rows", "{row}", "Collapsed row ops per transaction.");
        _meter.CreateObservableGauge("melange.log.head_lsn", () => (long)headLsn(), "{lsn}", "LSN of the newest log record.");
        _meter.CreateObservableGauge(
            "melange.applier.lag",
            () => applierLags().Select(l => new Measurement<long>(l.Lag, new KeyValuePair<string, object?>("applier", l.Applier))),
            "{tx}",
            "Transactions between the log head and each applier's checkpoint.");
    }

    public Activity? StartReducer(string reducerName, Identity caller, IReadOnlyList<object?>? arguments, ReadOnlyMemory<byte> encodedArguments)
    {
        var activity = Source.StartActivity("melange.reducer");
        if (activity is null)
            return null;
        activity.SetTag("melange.reducer.name", reducerName);
        if (_options.IncludeCallerIdentity)
            activity.SetTag("melange.caller", caller.ToString());
        if (_options.IncludeReducerArguments)
        {
            // In-process calls carry boxed values worth formatting; encoded dispatch carries the
            // wire payload, tagged as bounded hex so the opt-in still works for real traffic.
            if (arguments is { Count: > 0 })
                activity.SetTag("melange.reducer.args", string.Join(", ", arguments.Select(a => a?.ToString() ?? "null")));
            else if (!encodedArguments.IsEmpty)
                activity.SetTag("melange.reducer.args", FormatEncodedArguments(encodedArguments.Span));
        }

        return activity;
    }

    private static string FormatEncodedArguments(ReadOnlySpan<byte> encoded)
    {
        const int capBytes = 256;
        return encoded.Length <= capBytes
            ? Convert.ToHexStringLower(encoded)
            : $"{Convert.ToHexStringLower(encoded[..capBytes])}… ({encoded.Length} bytes)";
    }

    public Activity? StartCommit() => Source.StartActivity("melange.commit");

    public Activity? StartFsync() => Source.StartActivity("melange.fsync");

    public Activity? StartApply(string applier)
    {
        var activity = Source.StartActivity("melange.apply");
        activity?.SetTag("melange.applier", applier);
        return activity;
    }

    public void RecordTransaction(string reducerName, string outcome, double durationMs, int writeSetRows)
    {
        var reducerTag = new KeyValuePair<string, object?>("reducer", reducerName);
        _transactions.Add(1, reducerTag, new KeyValuePair<string, object?>("outcome", outcome));
        _reducerDuration.Record(durationMs, reducerTag);
        _writeSetRows.Record(writeSetRows, reducerTag);
    }

    public void RecordCommitDuration(double durationMs) => _commitDuration.Record(durationMs);

    public void RecordFsyncDuration(double durationMs) => _fsyncDuration.Record(durationMs);

    public void Dispose() => _meter.Dispose();
}
