using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using static System.FormattableString;

namespace MelangeDB.LoadTest;

/// <summary>What one drive run produced; the smoke test asserts on this shape.</summary>
public sealed record DriveResult
{
    public required bool Pass { get; init; }

    public required string SummaryLine { get; init; }

    public required long Acked { get; init; }

    public required long Attempted { get; init; }

    /// <summary>Completed handoffs over the run, or null when the serve stats were unreachable.</summary>
    public long? HandoffsCompleted { get; init; }
}

/// <summary>
/// The drive side: connects N player clients to a gateway address, runs the walk workload for
/// warm-up plus a measured window, samples progress every few seconds, and reports. Honest by
/// construction: warm-up is excluded from every statistic, and anything this process cannot
/// measure (server memory when the stats endpoint is unreachable) is reported as unavailable,
/// never guessed.
/// </summary>
internal static class LoadTestDriver
{
    public static async Task<DriveResult> RunAsync(LoadTestOptions options, TextWriter output, CancellationToken ct)
    {
        if (options.Address is null)
            throw new ArgumentException("drive needs --address ws://host:port/gateway (or run via 'all').");

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var statsUri = options.PollServerStats ? StatsUriFrom(options.Address) : null;
        var baseline = statsUri is null ? null : await TryFetchStatsAsync(http, statsUri, ct).ConfigureAwait(false);
        if (statsUri is not null && baseline is null)
        {
            output.WriteLine(
                $"Server stats endpoint unreachable at {statsUri}; handoff counters and server memory " +
                "will be reported as unavailable rather than guessed.");
            statsUri = null;
        }

        if (baseline is not null && (baseline.WorldBlocksX != options.WorldBlocksX
            || baseline.WorldBlocksY != options.WorldBlocksY
            || baseline.BlockChunksX != options.BlockChunksX
            || baseline.BlockChunksY != options.BlockChunksY))
        {
            throw new InvalidOperationException(
                $"World geometry mismatch: the server runs {baseline.WorldBlocksX}x{baseline.WorldBlocksY} blocks of " +
                $"{baseline.BlockChunksX}x{baseline.BlockChunksY} chunks, this driver was told {options.World}. " +
                "Pass matching --world-blocks/--block-chunks.");
        }

        var metrics = new DriveMetrics();
        var players = Enumerable.Range(0, options.Players)
            .Select(i => new PlayerDriver(i, options, metrics))
            .ToList();
        output.WriteLine(
            $"Drive: {options.Players} players ({options.SeamFraction:P0} seam walkers) at {options.TickHz} Hz, " +
            $"chunk step every {options.ChunkEveryTicks} ticks, against {options.Address}.");

        try
        {
            var connectClock = Stopwatch.StartNew();
            foreach (var batch in players.Chunk(16))
                await Task.WhenAll(batch.Select(p => p.ConnectAsync(ct))).ConfigureAwait(false);
            output.WriteLine($"Connected and subscribed {players.Count} clients in {connectClock.Elapsed.TotalSeconds:F1} s.");

            using var walkers = new CancellationTokenSource();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(walkers.Token, ct);
            var walkTasks = players.Select(p => Task.Run(() => p.RunAsync(linked.Token), CancellationToken.None)).ToList();

            output.WriteLine($"Warm-up: {options.WarmupSeconds} s (excluded from all statistics)...");
            await Task.Delay(TimeSpan.FromSeconds(options.WarmupSeconds), ct).ConfigureAwait(false);
            metrics.ResetForMeasurement();
            var bytesBaseline = players.Sum(static p => p.BytesReceived);
            var measuredStats = statsUri is null ? null : await TryFetchStatsAsync(http, statsUri, ct).ConfigureAwait(false);
            var handoffBaseline = measuredStats?.HandoffsCompleted;
            var abortBaseline = measuredStats?.HandoffsAborted;

            var series = new List<TimeSeriesSample>();
            var clock = Stopwatch.StartNew();
            var lastSample = (Elapsed: TimeSpan.Zero, Attempted: 0L, Rows: 0L, Bytes: 0L);
            while (clock.Elapsed < TimeSpan.FromSeconds(options.DurationSeconds))
            {
                var wait = TimeSpan.FromSeconds(options.SampleSeconds) - (clock.Elapsed - lastSample.Elapsed);
                var remaining = TimeSpan.FromSeconds(options.DurationSeconds) - clock.Elapsed;
                await Task.Delay(Min(wait, remaining) is var d && d > TimeSpan.Zero ? d : TimeSpan.FromMilliseconds(1), ct)
                    .ConfigureAwait(false);

                var stats = statsUri is null ? null : await TryFetchStatsAsync(http, statsUri, ct).ConfigureAwait(false);
                var elapsed = clock.Elapsed;
                var window = metrics.CallToDelta.DrainWindow();
                var attempted = metrics.Attempted;
                var rows = metrics.DeltaRows;
                var bytes = players.Sum(static p => p.BytesReceived) - bytesBaseline;
                var seconds = (elapsed - lastSample.Elapsed).TotalSeconds;
                var sample = new TimeSeriesSample
                {
                    ElapsedSeconds = elapsed.TotalSeconds,
                    Attempted = attempted,
                    Acked = metrics.Acked,
                    Rejected = metrics.Rejected,
                    TransportErrors = metrics.TransportErrors,
                    CallsPerSecond = (attempted - lastSample.Attempted) / seconds,
                    LatencyP50Ms = window.P50,
                    LatencyP90Ms = window.P90,
                    LatencyP99Ms = window.P99,
                    LatencyMaxMs = window.Max,
                    DeltaRowsPerSecond = (rows - lastSample.Rows) / seconds,
                    DeltaBytesPerSecond = (bytes - lastSample.Bytes) / seconds,
                    HandoffsCompleted = stats is null || handoffBaseline is null ? 0 : stats.HandoffsCompleted - handoffBaseline.Value,
                    HandoffsAborted = stats is null || abortBaseline is null ? 0 : stats.HandoffsAborted - abortBaseline.Value,
                    ServerWorkingSetBytes = stats?.WorkingSetBytes,
                    ServerGcHeapBytes = stats?.GcHeapBytes,
                    ServerGen2Collections = stats?.Gen2Collections,
                };
                series.Add(sample);
                lastSample = (elapsed, attempted, rows, bytes);
                output.WriteLine(
                    $"[{elapsed.TotalSeconds,5:F0}s] calls {sample.CallsPerSecond:F0}/s " +
                    $"(acked {sample.Acked}, rejected {sample.Rejected}) latency {window} " +
                    $"deltas {sample.DeltaRowsPerSecond:F0} rows/s {sample.DeltaBytesPerSecond / 1024:F0} KiB/s" +
                    (stats is null
                        ? " server: unavailable"
                        : $" handoffs {sample.HandoffsCompleted} server ws {stats.WorkingSetBytes / (1024 * 1024)} MiB " +
                          $"heap {stats.GcHeapBytes / (1024 * 1024)} MiB gen2 {stats.Gen2Collections}"));
            }

            var measured = clock.Elapsed;
            await walkers.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(walkTasks).ConfigureAwait(false);

            // Let in-flight deltas land before the final tally; their latency is part of the run.
            await Task.Delay(500, ct).ConfigureAwait(false);
            var finalStats = statsUri is null ? null : await TryFetchStatsAsync(http, statsUri, ct).ConfigureAwait(false);
            foreach (var player in players)
                metrics.Inconsistencies(player.Inconsistencies);

            var result = Summarize(
                options, metrics, players, measured, bytesBaseline, finalStats, handoffBaseline, abortBaseline, output,
                LockSplitShare(measuredStats, finalStats));
            if (options.OutPath is { } path)
            {
                WriteSeries(path, series);
                output.WriteLine($"Time series written to {path} ({series.Count} samples).");
            }

            return result;
        }
        finally
        {
            foreach (var batch in players.Chunk(32))
                await Task.WhenAll(batch.Select(static p => p.DisposeAsync().AsTask())).ConfigureAwait(false);
        }
    }

    private static DriveResult Summarize(
        LoadTestOptions options,
        DriveMetrics metrics,
        List<PlayerDriver> players,
        TimeSpan measured,
        long bytesBaseline,
        ServerStats? finalStats,
        long? handoffBaseline,
        long? abortBaseline,
        TextWriter output,
        (long Transactions, double Share, double MeanLockedMs)? lockSplit)
    {
        var latency = metrics.CallToDelta.Total();
        var crossing = metrics.CrossingContinuity.Total();
        var seconds = measured.TotalSeconds;
        var bytes = players.Sum(static p => p.BytesReceived) - bytesBaseline;
        long? handoffs = finalStats is null || handoffBaseline is null ? null : finalStats.HandoffsCompleted - handoffBaseline.Value;
        long? handoffsAborted = finalStats is null || abortBaseline is null ? null : finalStats.HandoffsAborted - abortBaseline.Value;

        output.WriteLine();
        output.WriteLine($"==== Measured window: {seconds:F0} s, {options.Players} players ====");
        output.WriteLine($"Reducer calls:   {metrics.Attempted / seconds:F0}/s attempted, {metrics.Acked / seconds:F0}/s acked " +
            $"({metrics.Attempted} attempted, {metrics.Acked} acked, {metrics.Rejected} rejected, {metrics.TransportErrors} transport errors)");
        output.WriteLine($"Call-to-delta:   {latency} over {latency.Count} samples " +
            $"({metrics.AbsorbedSelfDeltas} absorbed by handoff swaps and unmeasurable, {metrics.LostSelfDeltas} lost)");
        output.WriteLine($"Seam crossings:  {metrics.Crossings} block crossings; continuity {crossing}");
        output.WriteLine(handoffs is null
            ? "Handoffs:        server counters unreachable — not measured"
            : $"Handoffs:        {handoffs} completed, {handoffsAborted} aborted, " +
              $"{finalStats!.HandoffsUnresolved} unresolved, {finalStats.HandoffsRateLimited} rate-limited (whole-run counters where unlabeled)");
        output.WriteLine($"Delta traffic:   {metrics.DeltaRows / seconds:F0} rows/s aggregate " +
            $"({metrics.DeltaRows / seconds / options.Players:F1} rows/client/s); " +
            $"{bytes / seconds / 1024:F1} KiB/s aggregate ({bytes / seconds / options.Players:F0} B/client/s)");
        output.WriteLine(finalStats is null
            ? "Server process:  unreachable — memory not measured (drive the serve host locally or scrape its console)"
            : $"Server process:  working set {finalStats.WorkingSetBytes / (1024 * 1024)} MiB, GC heap " +
              $"{finalStats.GcHeapBytes / (1024 * 1024)} MiB, collections {finalStats.Gen0Collections}/{finalStats.Gen1Collections}/{finalStats.Gen2Collections}");
        if (lockSplit is { } split)
        {
            output.WriteLine(Invariant(
                $"Write lock:      post-commit (fan-out) is {split.Share:P1} of locked time over {split.Transactions} Move transactions (mean lock hold {split.MeanLockedMs * 1000:F0} µs)"));
        }

        output.WriteLine($"Client health:   {metrics.Disconnects} disconnects, {metrics.ResyncErrors} resync errors, " +
            $"{metrics.InconsistencyCount} cache inconsistencies");
        if (finalStats is not null)
        {
            output.WriteLine(Invariant(
                $"Liveness:        hub heartbeat gaps max {finalStats.HeartbeatGapMaxMs:F0} ms over {finalStats.HeartbeatGapCount} ({GapsOver(finalStats.HeartbeatGapsOverMs)}); ") +
                Invariant($"lease renewal gaps max {finalStats.LeaseRenewalGapMaxMs:F0} ms over {finalStats.LeaseRenewalGapCount} ({GapsOver(finalStats.LeaseRenewalGapsOverMs)}); ") +
                Invariant($"{finalStats.NodesDeclaredDead} node(s) declared dead, {finalStats.LeaseLapses} lease lapse(s) (whole run)"));
            output.WriteLine(finalStats.ReplicaLagSamples == 0
                ? "Replica lag:     no replicas sampled (one shard node, or Cluster:ShardReplicas 0)"
                : Invariant($"Replica lag:     p50 {finalStats.ReplicaLagP50Records} / p99 {finalStats.ReplicaLagP99Records} / max {finalStats.ReplicaLagMaxRecords} records behind the owner's durable LSN over {finalStats.ReplicaLagSamples} samples; ") +
                  Invariant($"oldest unapplied record durable for p99 {finalStats.ReplicaStalenessP99Ms:F0} / max {finalStats.ReplicaStalenessMaxMs:F0} ms (whole run)"));
        }

        var failures = new List<string>();
        if (metrics.Acked == 0)
            failures.Add("no acked calls");
        if (metrics.Disconnects > 0)
            failures.Add($"{metrics.Disconnects} client disconnects");
        if (metrics.ResyncErrors > 0)
            failures.Add($"{metrics.ResyncErrors} resync errors");
        if (handoffs == 0 && options.SeamFraction > 0 && (options.WorldBlocksX > 1 || options.WorldBlocksY > 1))
            failures.Add("no completed handoffs despite seam walkers");

        // Nothing is killed in a drive run, so any death is a false one.
        if (finalStats is { NodesDeclaredDead: > 0 })
            failures.Add($"{finalStats.NodesDeclaredDead} healthy node(s) declared dead");
        if (finalStats is { LeaseLapses: > 0 })
            failures.Add($"{finalStats.LeaseLapses} lease lapse(s) on healthy nodes");
        var pass = failures.Count == 0;

        var summary =
            Invariant($"LOADTEST RESULT={(pass ? "PASS" : "FAIL")} players={options.Players} measured_s={seconds:F0} ") +
            Invariant($"attempted={metrics.Attempted} acked={metrics.Acked} rejected={metrics.Rejected} ") +
            Invariant($"transport_errors={metrics.TransportErrors} call_to_delta_ms_p50={latency.P50:F1} p90={latency.P90:F1} ") +
            Invariant($"p99={latency.P99:F1} max={latency.Max:F0} crossings={metrics.Crossings} ") +
            Invariant($"crossing_ms_p50={crossing.P50:F1} p99={crossing.P99:F1} ") +
            Invariant($"handoffs_completed={handoffs?.ToString(CultureInfo.InvariantCulture) ?? "unavailable"} ") +
            Invariant($"handoffs_aborted={handoffsAborted?.ToString(CultureInfo.InvariantCulture) ?? "unavailable"} ") +
            Invariant($"delta_rows_per_s={metrics.DeltaRows / seconds:F0} delta_bytes_per_s={bytes / seconds:F0} ") +
            Invariant($"disconnects={metrics.Disconnects} inconsistencies={metrics.InconsistencyCount}") +
            (finalStats is { } live
                ? Invariant($" hb_gap_max_ms={live.HeartbeatGapMaxMs:F0} hb_gaps_over_1000={live.HeartbeatGapsOverMs.GetValueOrDefault(1_000)} ") +
                  Invariant($"hb_gaps_over_2000={live.HeartbeatGapsOverMs.GetValueOrDefault(2_000)} hb_gaps_over_3000={live.HeartbeatGapsOverMs.GetValueOrDefault(3_000)} ") +
                  Invariant($"lease_gap_max_ms={live.LeaseRenewalGapMaxMs:F0} lease_gaps_over_1000={live.LeaseRenewalGapsOverMs.GetValueOrDefault(1_000)} ") +
                  Invariant($"lease_gaps_over_2000={live.LeaseRenewalGapsOverMs.GetValueOrDefault(2_000)} nodes_declared_dead={live.NodesDeclaredDead} lease_lapses={live.LeaseLapses} ") +
                  Invariant($"replica_lag_p50={live.ReplicaLagP50Records} replica_lag_p99={live.ReplicaLagP99Records} replica_lag_max={live.ReplicaLagMaxRecords} ") +
                  Invariant($"replica_staleness_p99_ms={live.ReplicaStalenessP99Ms:F0} replica_staleness_max_ms={live.ReplicaStalenessMaxMs:F0}")
                : string.Empty) +
            (lockSplit is { } s ? Invariant($" post_commit_share={s.Share:F4} lock_split_transactions={s.Transactions} mean_locked_us={s.MeanLockedMs * 1000:F0}") : string.Empty) +
            (pass ? string.Empty : $" failures=[{string.Join("; ", failures)}]");
        output.WriteLine();
        output.WriteLine(summary);
        return new DriveResult
        {
            Pass = pass,
            SummaryLine = summary,
            Acked = metrics.Acked,
            Attempted = metrics.Attempted,
            HandoffsCompleted = handoffs,
        };
    }

    /// <summary>"&gt;1 s: n, &gt;2 s: n, ..." over the thresholds a timeout choice is made among.</summary>
    private static string GapsOver(Dictionary<int, long> over) =>
        string.Join(", ", new[] { 500, 1_000, 2_000, 3_000, 5_000 }.Select(t => Invariant($">{t} ms: {over.GetValueOrDefault(t)}")));

    /// <summary>The measured window's post-commit share of locked time, from the serve side's running sums; null when not traced.</summary>
    private static (long Transactions, double Share, double MeanLockedMs)? LockSplitShare(ServerStats? start, ServerStats? end)
    {
        if (start is null || end is null)
            return null;
        var transactions = end.LockSplitTransactions - start.LockSplitTransactions;
        var locked = end.LockedMsSum - start.LockedMsSum;
        return transactions <= 0 || locked <= 0
            ? null
            : (transactions, (end.PostCommitMsSum - start.PostCommitMsSum) / locked, locked / transactions);
    }

    private static void WriteSeries(string path, List<TimeSeriesSample> series)
    {
        if (Path.GetExtension(path).Equals(".json", StringComparison.OrdinalIgnoreCase))
        {
            File.WriteAllText(path, JsonSerializer.Serialize(series, SeriesJson));
            return;
        }

        using var writer = new StreamWriter(path);
        writer.WriteLine(TimeSeriesSample.CsvHeader);
        foreach (var sample in series)
            writer.WriteLine(sample.ToCsv());
    }

    private static readonly JsonSerializerOptions SeriesJson = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private static Uri StatsUriFrom(Uri gateway) =>
        new UriBuilder(gateway)
        {
            Scheme = gateway.Scheme == "wss" ? "https" : "http",
            Path = "/loadtest/stats",
        }.Uri;

    private static async Task<ServerStats?> TryFetchStatsAsync(HttpClient http, Uri uri, CancellationToken ct)
    {
        try
        {
            await using var stream = await http.GetStreamAsync(uri, ct).ConfigureAwait(false);
            return await JsonSerializer.DeserializeAsync<ServerStats>(
                stream, SeriesJson, ct).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or JsonException)
        {
            return null;
        }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}
