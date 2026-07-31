using System.Globalization;

namespace MelangeDB.LoadTest;

/// <summary>A latency reservoir: thread-safe sample collection with percentile snapshots.</summary>
internal sealed class LatencySeries
{
    private readonly Lock _lock = new();
    private readonly List<double> _all = [];
    private readonly List<double> _window = [];

    public void Record(double milliseconds)
    {
        lock (_lock)
        {
            _all.Add(milliseconds);
            _window.Add(milliseconds);
        }
    }

    public long Count
    {
        get
        {
            lock (_lock)
            {
                return _all.Count;
            }
        }
    }

    /// <summary>Percentiles over everything recorded since the last reset (the measured run).</summary>
    public LatencySnapshot Total()
    {
        lock (_lock)
        {
            return LatencySnapshot.Of(_all);
        }
    }

    /// <summary>Percentiles over the current sample window, then starts the next window.</summary>
    public LatencySnapshot DrainWindow()
    {
        lock (_lock)
        {
            var snapshot = LatencySnapshot.Of(_window);
            _window.Clear();
            return snapshot;
        }
    }

    /// <summary>Drops warm-up samples so no reported statistic includes them.</summary>
    public void Reset()
    {
        lock (_lock)
        {
            _all.Clear();
            _window.Clear();
        }
    }
}

/// <summary>Percentiles of one latency population, in milliseconds.</summary>
internal readonly record struct LatencySnapshot(long Count, double P50, double P90, double P99, double Max)
{
    public static LatencySnapshot Of(List<double> samples)
    {
        if (samples.Count == 0)
            return new LatencySnapshot(0, 0, 0, 0, 0);
        var sorted = samples.ToArray();
        Array.Sort(sorted);
        return new LatencySnapshot(
            sorted.Length,
            At(sorted, 0.50),
            At(sorted, 0.90),
            At(sorted, 0.99),
            sorted[^1]);
    }

    /// <summary>Nearest-rank percentile — no interpolation, so small populations stay honest.</summary>
    private static double At(double[] sorted, double p) =>
        sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(p * sorted.Length) - 1)];

    public override string ToString() =>
        Count == 0 ? "no samples" : $"p50={P50:F1}ms p90={P90:F1}ms p99={P99:F1}ms max={Max:F0}ms";
}

/// <summary>
/// Everything the driver counts, shared by all player clients. Counters are cumulative for the
/// whole process lifetime; <see cref="ResetForMeasurement"/> zeroes them when the warm-up ends so
/// every reported number describes the measured window only.
/// </summary>
internal sealed class DriveMetrics
{
    private int _epoch;
    private long _attempted;
    private long _acked;
    private long _rejected;
    private long _transportErrors;
    private long _deltaRows;
    private long _crossings;
    private long _absorbedSelfDeltas;
    private long _lostSelfDeltas;
    private long _disconnects;
    private long _resyncErrors;
    private long _inconsistencies;

    /// <summary>Call-to-delta: reducer call sent, to the matching self-delta on the caller's own subscription.</summary>
    public LatencySeries CallToDelta { get; } = new();

    /// <summary>Client-observed seam-crossing continuity: the block-crossing call, to self-deltas resuming.</summary>
    public LatencySeries CrossingContinuity { get; } = new();

    /// <summary>
    /// The measurement epoch a call belongs to: captured at the attempt, presented back at the
    /// outcome. An outcome from a call attempted before <see cref="ResetForMeasurement"/> is
    /// dropped, so acked can never exceed attempted just because warm-up calls landed late.
    /// </summary>
    public int Epoch => Volatile.Read(ref _epoch);

    public void CallAttempted() => Interlocked.Increment(ref _attempted);

    public void CallAcked(int epoch)
    {
        if (epoch == Epoch)
            Interlocked.Increment(ref _acked);
    }

    public void CallRejected(int epoch)
    {
        if (epoch == Epoch)
            Interlocked.Increment(ref _rejected);
    }

    public void TransportError(int epoch)
    {
        if (epoch == Epoch)
            Interlocked.Increment(ref _transportErrors);
    }

    public void DeltaRow() => Interlocked.Increment(ref _deltaRows);

    public void Crossing() => Interlocked.Increment(ref _crossings);

    public void AbsorbedSelfDelta() => Interlocked.Increment(ref _absorbedSelfDeltas);

    public void LostSelfDelta() => Interlocked.Increment(ref _lostSelfDeltas);

    public void Disconnected() => Interlocked.Increment(ref _disconnects);

    public void ResyncError() => Interlocked.Increment(ref _resyncErrors);

    public void Inconsistencies(long count) => Interlocked.Add(ref _inconsistencies, count);

    public long Attempted => Interlocked.Read(ref _attempted);

    public long Acked => Interlocked.Read(ref _acked);

    public long Rejected => Interlocked.Read(ref _rejected);

    public long TransportErrors => Interlocked.Read(ref _transportErrors);

    public long DeltaRows => Interlocked.Read(ref _deltaRows);

    public long Crossings => Interlocked.Read(ref _crossings);

    public long AbsorbedSelfDeltas => Interlocked.Read(ref _absorbedSelfDeltas);

    public long LostSelfDeltas => Interlocked.Read(ref _lostSelfDeltas);

    public long Disconnects => Interlocked.Read(ref _disconnects);

    public long ResyncErrors => Interlocked.Read(ref _resyncErrors);

    public long InconsistencyCount => Interlocked.Read(ref _inconsistencies);

    public void ResetForMeasurement()
    {
        Interlocked.Increment(ref _epoch);
        Interlocked.Exchange(ref _attempted, 0);
        Interlocked.Exchange(ref _acked, 0);
        Interlocked.Exchange(ref _rejected, 0);
        Interlocked.Exchange(ref _transportErrors, 0);
        Interlocked.Exchange(ref _deltaRows, 0);
        Interlocked.Exchange(ref _crossings, 0);
        Interlocked.Exchange(ref _absorbedSelfDeltas, 0);
        Interlocked.Exchange(ref _lostSelfDeltas, 0);
        CallToDelta.Reset();
        CrossingContinuity.Reset();
    }
}

/// <summary>One periodic sample of the run — a row of the <c>--out</c> time series.</summary>
internal sealed record TimeSeriesSample
{
    public required double ElapsedSeconds { get; init; }

    public required long Attempted { get; init; }

    public required long Acked { get; init; }

    public required long Rejected { get; init; }

    public required long TransportErrors { get; init; }

    public required double CallsPerSecond { get; init; }

    public required double LatencyP50Ms { get; init; }

    public required double LatencyP90Ms { get; init; }

    public required double LatencyP99Ms { get; init; }

    public required double LatencyMaxMs { get; init; }

    public required double DeltaRowsPerSecond { get; init; }

    public required double DeltaBytesPerSecond { get; init; }

    public required long HandoffsCompleted { get; init; }

    public required long HandoffsAborted { get; init; }

    /// <summary>Null when the serve side's stats endpoint is unreachable — absent, not guessed.</summary>
    public long? ServerWorkingSetBytes { get; init; }

    public long? ServerGcHeapBytes { get; init; }

    public int? ServerGen2Collections { get; init; }

    public static string CsvHeader =>
        "elapsed_s,attempted,acked,rejected,transport_errors,calls_per_s,latency_p50_ms,latency_p90_ms," +
        "latency_p99_ms,latency_max_ms,delta_rows_per_s,delta_bytes_per_s,handoffs_completed,handoffs_aborted," +
        "server_working_set_bytes,server_gc_heap_bytes,server_gen2_collections";

    public string ToCsv() => string.Join(',', new[]
    {
        ElapsedSeconds.ToString("F1", CultureInfo.InvariantCulture),
        Attempted.ToString(CultureInfo.InvariantCulture),
        Acked.ToString(CultureInfo.InvariantCulture),
        Rejected.ToString(CultureInfo.InvariantCulture),
        TransportErrors.ToString(CultureInfo.InvariantCulture),
        CallsPerSecond.ToString("F1", CultureInfo.InvariantCulture),
        LatencyP50Ms.ToString("F2", CultureInfo.InvariantCulture),
        LatencyP90Ms.ToString("F2", CultureInfo.InvariantCulture),
        LatencyP99Ms.ToString("F2", CultureInfo.InvariantCulture),
        LatencyMaxMs.ToString("F2", CultureInfo.InvariantCulture),
        DeltaRowsPerSecond.ToString("F1", CultureInfo.InvariantCulture),
        DeltaBytesPerSecond.ToString("F0", CultureInfo.InvariantCulture),
        HandoffsCompleted.ToString(CultureInfo.InvariantCulture),
        HandoffsAborted.ToString(CultureInfo.InvariantCulture),
        ServerWorkingSetBytes?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
        ServerGcHeapBytes?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
        ServerGen2Collections?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
    });
}
