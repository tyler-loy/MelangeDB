using System.Threading.Channels;
using MelangeDB.Core;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The origin-decides half of seamless handoff, one per owned shard: a commit observer that
/// assesses every committed Partitioned write of an anchored entity against the shard's boundary
/// and turns the answer into hub notifications — an <c>handoff-approach</c> when the entity
/// enters the border band (the gateway pre-opens the destination session on it), and an
/// <c>handoff-request</c> once it has crossed past the hysteresis margin (immediately, for
/// creatures). The origin decides because its committed rows are the only trusted position — the
/// client's claimed position never is, and the gateway cannot see positions at all. The hub
/// enforces the authoritative rate limit; the local cooldowns here only keep a busy boundary from
/// spamming the link.
/// </summary>
internal sealed class BoundaryMonitor : ICommitObserver, IDisposable
{
    private readonly record struct Signal(MigrationAnchor Anchor, BoundaryAssessment Assessment);

    private readonly MelangeEngine _engine;
    private readonly ShardKey _shard;
    private readonly IBoundaryStrategy _boundary;
    private readonly IShardStrategy _strategy;
    private readonly IMigrationAnchors _anchors;
    private readonly Func<NodeLink?> _link;
    private readonly Func<long> _fencingToken;
    private readonly Func<TableId, RowKey, ulong?> _borrowedOwner;
    private readonly Func<TableId, RowKey, bool> _isFrozen;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly TimeProvider _time;
    private readonly Channel<Signal> _signals = Channel.CreateBounded<Signal>(
        new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.DropOldest });
    private readonly Dictionary<Identity, long> _lastRequestTicks = [];
    private readonly Dictionary<(Identity, ShardKey), long> _lastApproachTicks = [];

    /// <summary>
    /// Anchored entities currently strayed across the boundary, keyed by anchor. Triggers are
    /// commit-driven, but a suppressed (rate-limited) trigger with no further movement would
    /// otherwise never re-fire — an entity standing still just past the margin would be stranded.
    /// The sweep re-assesses these from the store until each either transfers (its row leaves
    /// this shard) or walks back inside.
    /// </summary>
    private readonly Dictionary<Identity, (TableId Table, RowKey Key, bool Immediate)> _strays = [];
    private readonly CancellationTokenSource _stopped = new();
    private Task? _loop;
    private Task? _straySweep;

    /// <summary>Test-visible counters: what this monitor saw, and what it asked the hub for.</summary>
    internal long CrossingsObserved;
    internal long RequestsSent;
    internal long SweepPasses;

    internal int StrayCount
    {
        get
        {
            lock (_strays)
            {
                return _strays.Count;
            }
        }
    }

    public BoundaryMonitor(
        MelangeEngine engine,
        ShardKey shard,
        IShardStrategy strategy,
        IMigrationAnchors anchors,
        Func<NodeLink?> link,
        Func<long> fencingToken,
        Func<TableId, RowKey, ulong?> borrowedOwner,
        Func<TableId, RowKey, bool> isFrozen,
        IOptionsMonitor<MelangeDbOptions> options,
        TimeProvider time)
    {
        _isFrozen = isFrozen;
        _engine = engine;
        _shard = shard;
        _boundary = (IBoundaryStrategy)strategy;
        _strategy = strategy;
        _anchors = anchors;
        _link = link;
        _fencingToken = fencingToken;
        _borrowedOwner = borrowedOwner;
        _options = options;
        _time = time;
    }

    public void Start()
    {
        _engine.AddCommitObserver(this);
        _loop = Task.Run(LoopAsync);
        _straySweep = Task.Run(SweepStraysAsync);

        // Recovery re-assessment: an anchored entity that crossed the boundary in the instant
        // before a crash has no future commit to re-trigger it (its AI skips foreign-resolving
        // rows on purpose), so a restarted shard walks its rows once and re-signals any strays.
        _ = Task.Run(AssessExistingRowsAsync);
    }

    /// <summary>Runs under the engine's write lock: assess and enqueue, nothing slower.</summary>
    public void OnCommit(CommitRecord record)
    {
        if (record.ReducerName.StartsWith("melange/", StringComparison.Ordinal))
            return;
        foreach (var op in record.WriteSet)
        {
            if (op.Kind == RowOpKind.Delete)
                continue;
            if (!_engine.Schema.TryGet(op.Table, out var table) || table.Placement != Placement.Partitioned)
                continue;
            if (_borrowedOwner(op.Table, op.Key) is not null)
                continue; // A border copy: its owner's monitor watches it, not this shard's.
            var row = table.ToRowRef(op.Row);
            if (_anchors.AnchorOf(table, row) is not { } anchor)
                continue;
            var assessment = _boundary.Assess(_shard, op.Table, row);
            lock (_strays)
            {
                if (assessment.CrossedInto is not null)
                {
                    Interlocked.Increment(ref CrossingsObserved);
                    _strays[anchor.Id] = (op.Table, op.Key, anchor.Immediate);
                }
                else
                {
                    _strays.Remove(anchor.Id);
                }
            }

            if (assessment.CrossedInto is not null || assessment.Approaching.Count > 0)
                _signals.Writer.TryWrite(new Signal(anchor, assessment));
        }
    }

    private async Task AssessExistingRowsAsync()
    {
        await Task.Yield();
        foreach (var table in _engine.Schema.Tables)
        {
            if (_stopped.IsCancellationRequested)
                return;
            if (table.Placement != Placement.Partitioned)
                continue;
            foreach (var (key, bytes) in _engine.HotStore.Scan(table.Id))
            {
                if (_borrowedOwner(table.Id, key) is not null)
                    continue; // Border copies resolve to their owner by construction; never ours to move.
                if (_isFrozen(table.Id, key))
                    continue; // Mid-transfer already; the reconciler owns its fate, not this monitor.
                var row = table.ToRowRef(bytes);
                if (_anchors.AnchorOf(table, row) is not { } anchor)
                    continue;
                var assessment = _boundary.Assess(_shard, table.Id, row);
                if (assessment.CrossedInto is not null)
                {
                    lock (_strays)
                    {
                        _strays[anchor.Id] = (table.Id, key, anchor.Immediate);
                    }

                    _signals.Writer.TryWrite(new Signal(anchor, assessment));
                }
            }
        }
    }

    /// <summary>Re-signals standing strays until each transfers away or walks back inside.</summary>
    private async Task SweepStraysAsync()
    {
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1_000, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            Interlocked.Increment(ref SweepPasses);
            List<(Identity Id, TableId Table, RowKey Key, bool Immediate)> strays;
            lock (_strays)
            {
                if (_strays.Count == 0)
                    continue;
                strays = [.. _strays.Select(static pair => (pair.Key, pair.Value.Table, pair.Value.Key, pair.Value.Immediate))];
            }

            foreach (var stray in strays)
            {
                try
                {
                    // Transferred away (the release deleted it), re-borrowed, or moved back: done.
                    if (!_engine.HotStore.TryGetRow(stray.Table, stray.Key, out var bytes)
                        || _borrowedOwner(stray.Table, stray.Key) is not null)
                    {
                        lock (_strays)
                        {
                            _strays.Remove(stray.Id);
                        }

                        continue;
                    }

                    // Frozen means a transfer for it is in flight right now: keep watching, but
                    // never re-request — a second saga for a mid-transfer entity re-imports its
                    // pre-freeze bytes over whatever the destination has done since.
                    if (_isFrozen(stray.Table, stray.Key))
                        continue;

                    var table = _engine.Schema.Get(stray.Table);
                    var assessment = _boundary.Assess(_shard, stray.Table, table.ToRowRef(bytes.ToArray()));
                    if (assessment.CrossedInto is null)
                    {
                        lock (_strays)
                        {
                            _strays.Remove(stray.Id);
                        }

                        continue;
                    }

                    _signals.Writer.TryWrite(new Signal(new MigrationAnchor(stray.Id, stray.Immediate), assessment));
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    // One stray must not kill the sweep; the entity re-signals next pass.
                }
            }
        }
    }

    private async Task LoopAsync()
    {
        var ct = _stopped.Token;
        try
        {
            await foreach (var signal in _signals.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    await HandleSignalAsync(signal, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception)
                {
                    // Hub unreachable: the next commit (or the recovery sweep) re-signals.
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task HandleSignalAsync(Signal signal, CancellationToken ct)
    {
        if (_link() is not { } link)
            return;
        var cluster = _options.CurrentValue.Cluster;
        var now = _time.GetUtcNow().UtcTicks;

        if (signal.Assessment.CrossedInto is { } destination)
        {
            var margin = Math.Max(0, cluster.HandoffMarginChunks);
            if (!signal.Anchor.Immediate && signal.Assessment.CrossedDepthChunks <= margin)
                return; // Within the hysteresis margin: still the origin's, deliberately.

            // Local cooldown only — the hub's HandoffMinIntervalMs is the authoritative limit;
            // this keeps one entity's movement burst from sending a request per step.
            lock (_lastRequestTicks)
            {
                if (_lastRequestTicks.TryGetValue(signal.Anchor.Id, out var last)
                    && now - last < TimeSpan.TicksPerMillisecond * 500)
                {
                    return;
                }

                _lastRequestTicks[signal.Anchor.Id] = now;
            }

            Interlocked.Increment(ref RequestsSent);
            await link.NotifyAsync(
                "handoff-request",
                new HandoffRequest(signal.Anchor.Id.ToString(), _shard.Value, destination.Value, _fencingToken()),
                ct).ConfigureAwait(false);
            return;
        }

        var targets = new List<ulong>();
        lock (_lastRequestTicks)
        {
            foreach (var approaching in signal.Assessment.Approaching)
            {
                var key = (signal.Anchor.Id, approaching);
                if (_lastApproachTicks.TryGetValue(key, out var last)
                    && now - last < TimeSpan.TicksPerSecond * 3)
                {
                    continue;
                }

                _lastApproachTicks[key] = now;
                targets.Add(approaching.Value);
            }
        }

        if (targets.Count > 0)
        {
            await link.NotifyAsync(
                "handoff-approach",
                new HandoffApproach(signal.Anchor.Id.ToString(), _shard.Value, [.. targets]),
                ct).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        _stopped.Cancel();
        _signals.Writer.TryComplete();
    }
}
