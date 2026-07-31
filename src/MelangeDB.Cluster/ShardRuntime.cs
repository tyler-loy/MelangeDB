using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The durable saga record a handoff marker carries in its log arguments. On an import marker,
/// <see cref="FromShard"/> is <see cref="NoOrigin"/> for a first-entry transfer (the player had
/// no rows anywhere yet), which is what tells the destination's reconciler there is no origin to
/// wait on before settling.
/// </summary>
internal sealed record HandoffMarker(string HandoffId, string PlayerHex, ulong FromShard, ulong ToShard, WireOp[]? Rows)
{
    public const ulong NoOrigin = ulong.MaxValue;
}

/// <summary>Reserved reducer names for cluster-internal log records.</summary>
internal static class ClusterRecordNames
{
    public const string Replica = "melange/replica";
    public const string HandoffFreeze = "melange/handoff-freeze";
    public const string HandoffImport = "melange/handoff-import";
    public const string HandoffRelease = "melange/handoff-release";
    public const string HandoffAbort = "melange/handoff-abort";
    public const string HandoffSettled = "melange/handoff-settled";
}

/// <summary>
/// One shard, alive on the node that owns it: its own engine (and therefore its own commit log —
/// one log per shard, no global order), its own reducer host, its own scheduler (timers are rows
/// in this log, so they fire here and only here), its own transport for gateway-routed clients,
/// and the origin/destination halves of the handoff saga, recoverable because every step appended
/// a marker to this log before it was acknowledged.
/// </summary>
internal sealed class ShardRuntime : IDisposable
{
    private static readonly Identity ClusterCaller = Identity.Hash("melange/cluster");

    private readonly Lock _handoffLock = new();
    private readonly HashSet<(TableId Table, RowKey Key)> _frozenRows = [];
    private readonly Dictionary<string, (HandoffMarker Marker, ulong Lsn)> _pendingFreezes = new(StringComparer.Ordinal);
    private readonly HashSet<string> _importedHandoffs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (HandoffMarker Marker, ulong Lsn)> _unsettledImports = new(StringComparer.Ordinal);
    private readonly IHandoffSet? _handoffSet;
    private readonly IShardStrategy? _strategy;

    public ShardRuntime(
        ShardKey shard,
        long fencingToken,
        ushort originator,
        string shardDirectory,
        string nodeName,
        SchemaRegistry schema,
        ReducerRegistry reducers,
        IServiceProvider services,
        IOptionsMonitor<MelangeDbOptions> options,
        Func<bool> leaseValid,
        ILoggerFactory loggerFactory,
        TimeProvider time,
        MelangeAuthenticator authenticator,
        MelangeSessions sessions)
    {
        Shard = shard;
        FencingToken = fencingToken;
        Directory = shardDirectory;
        _handoffSet = services.GetService<IHandoffSet>();
        var strategy = services.GetService<IShardStrategy>();
        _strategy = strategy;
        var current = options.CurrentValue;
        Options = MelangeOptionsClone.DeepClone(current);
        Options.HotStore.Path = Path.Combine(shardDirectory, "hot");
        Options.CommitLog.Path = Path.Combine(shardDirectory, "log");
        Options.Events.DeadLetterPath = Path.Combine(shardDirectory, "deadletter");

        Engine = new MelangeEngine(
            Options,
            schema,
            loggerFactory,
            time,
            services.GetService<IHotStoreProvider>(),
            originator);
        Engine.SetTableAccessGuard(PlacementGuards.ShardAccess(shard, nodeName));
        Engine.AddCommitGuard(new ShardCommitGuard(
            schema,
            () => Engine.HotStore,
            shard,
            strategy,
            () => ShardSpanCheck.IsEnabled(options.CurrentValue.Cluster.ShardSpanCheck),
            leaseValid,
            FrozenSnapshot));

        RecoverHandoffState();

        // Handoff markers must survive log truncation for as long as their saga is unresolved —
        // the origin's freeze until it releases or aborts, the destination's import until the
        // origin is known settled. Truncating one away would silently unfreeze a mid-transfer
        // player (origin) or make WasImported answer a wrong "no" (destination): either way, two
        // owners — the exact silent-gap bug class phase 08 fixed for the Postgres checkpoint.
        Engine.AddTruncationFloor(() => MarkerFloor(_pendingFreezes));
        Engine.AddTruncationFloor(() => MarkerFloor(_unsettledImports));

        ReducerHost = new MelangeReducerHost(
            Engine, reducers, services.GetRequiredService<IServiceScopeFactory>(), options, time);
        Scheduler = new MelangeScheduler(Engine, ReducerHost, options, loggerFactory, time);
        Transport = new MelangeTransport(
            Engine, ReducerHost, options, time, loggerFactory, authenticator, sessions,
            new PolicySet(services, schema));
        Scheduler.Start();
    }

    public ShardKey Shard { get; }

    /// <summary>The fencing token of this ownership term; refreshed from heartbeat assignments.</summary>
    public long FencingToken { get; set; }

    public string Directory { get; }

    public MelangeDbOptions Options { get; }

    public MelangeEngine Engine { get; }

    public MelangeReducerHost ReducerHost { get; }

    public MelangeScheduler Scheduler { get; }

    public MelangeTransport Transport { get; }

    /// <summary>Handoffs frozen on this shard (as origin) that neither released nor aborted yet.</summary>
    public IReadOnlyList<HandoffMarker> PendingFreezes
    {
        get
        {
            lock (_handoffLock)
            {
                return [.. _pendingFreezes.Values.Select(static p => p.Marker)];
            }
        }
    }

    /// <summary>Whether the given handoff's freeze is still unresolved on this shard (as origin).</summary>
    public bool IsFreezePending(string handoffId)
    {
        lock (_handoffLock)
        {
            return _pendingFreezes.ContainsKey(handoffId);
        }
    }

    /// <summary>
    /// Imports (as destination) whose origin is not yet known settled. Their markers pin log
    /// truncation, so a restarted destination can never answer <see cref="WasImported"/> with a
    /// silently wrong "no" — the marker is guaranteed to still be in the log it recovers from.
    /// </summary>
    public IReadOnlyList<HandoffMarker> UnsettledImports
    {
        get
        {
            lock (_handoffLock)
            {
                return [.. _unsettledImports.Values.Select(static p => p.Marker)];
            }
        }
    }

    /// <summary>Whether this shard (as destination) durably imported the given handoff.</summary>
    public bool WasImported(string handoffId)
    {
        lock (_handoffLock)
        {
            return _importedHandoffs.Contains(handoffId);
        }
    }

    /// <summary>
    /// The truncation floor a marker set pins: everything from the oldest live marker onward must
    /// stay in the log (the floor is the highest removable LSN). Null pins nothing — sagas
    /// resolve, so the pin is bounded by the slowest in-flight handoff, not by history.
    /// </summary>
    private ulong? MarkerFloor(Dictionary<string, (HandoffMarker Marker, ulong Lsn)> markers)
    {
        lock (_handoffLock)
        {
            if (markers.Count == 0)
                return null;
            return markers.Values.Min(static p => p.Lsn) - 1;
        }
    }

    private IReadOnlySet<(TableId, RowKey)> FrozenSnapshot()
    {
        lock (_handoffLock)
        {
            return _frozenRows.Count == 0 ? EmptyFrozen : new HashSet<(TableId, RowKey)>(_frozenRows);
        }
    }

    private static readonly IReadOnlySet<(TableId, RowKey)> EmptyFrozen = new HashSet<(TableId, RowKey)>();

    /// <summary>
    /// The origin half, step one: freeze the player's rows, then collect them — both under one
    /// write-lock hold, so no write can interleave between "still writable" and "collected" —
    /// then append the freeze marker so a crash recovers the frozen state from this log.
    /// </summary>
    public WireOp[] FreezeAndCollect(string handoffId, Identity player, ShardKey toShard)
    {
        lock (_handoffLock)
        {
            if (_pendingFreezes.TryGetValue(handoffId, out _))
                return CollectFrozen(handoffId); // Idempotent retry of an already-frozen handoff.
        }

        if (_handoffSet is null)
        {
            throw new InvalidOperationException(
                "No IHandoffSet is registered; player transfer requires the application to define which rows follow " +
                "a player (services.AddSingleton<IHandoffSet, ...>).");
        }

        var collector = new Collector(Engine.Schema);
        Engine.ReadConsistent(_ =>
        {
            _handoffSet.Collect(player, Shard, Engine.CommittedView, collector);
            lock (_handoffLock)
            {
                foreach (var (table, key, _) in collector.Rows)
                    _frozenRows.Add((table.Id, key));
            }
        });

        var rows = collector.Rows
            .Select(static r => new WireOp((byte)RowOpKind.Insert, r.Table.Id.Value, r.Key.ToArray(), r.Bytes))
            .ToArray();
        var marker = new HandoffMarker(handoffId, player.ToString(), Shard.Value, toShard.Value, RowRefsOf(rows));

        // Registered under a conservative placeholder LSN *before* the append: the append itself
        // can trigger an automatic snapshot, and the truncation floor must already cover the
        // marker about to land. The real LSN replaces the placeholder right after.
        lock (_handoffLock)
        {
            _pendingFreezes[handoffId] = (marker, Engine.Log.HeadLsn + 1);
        }

        var record = Engine.ApplyInternal(
            ClusterRecordNames.HandoffFreeze, ClusterCaller, [],
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), alwaysAppend: true);
        lock (_handoffLock)
        {
            if (_pendingFreezes.ContainsKey(handoffId))
                _pendingFreezes[handoffId] = (marker, record!.Lsn);
        }

        return rows;
    }

    private WireOp[] CollectFrozen(string handoffId)
    {
        HandoffMarker marker;
        lock (_handoffLock)
        {
            marker = _pendingFreezes[handoffId].Marker;
        }

        var rows = new List<WireOp>();
        Engine.ReadConsistent(_ =>
        {
            foreach (var reference in marker.Rows ?? [])
            {
                if (Engine.HotStore.TryGetRow(new TableId(reference.Table), new RowKey(reference.Key), out var bytes))
                    rows.Add(new WireOp((byte)RowOpKind.Insert, reference.Table, reference.Key, bytes.ToArray()));
            }
        });
        return [.. rows];
    }

    /// <summary>
    /// The destination half: rewrite each row's shard column to this shard and append them as one
    /// internal commit whose marker carries the handoff id and the real origin shard
    /// (<see cref="HandoffMarker.NoOrigin"/> for a first-entry transfer). Idempotent — a
    /// re-delivered import reconciles into updates and the id was already recorded. The marker
    /// stays truncation-pinned until <see cref="Settle"/>.
    /// </summary>
    public void Import(string handoffId, string playerHex, ulong fromShard, WireOp[] rows)
    {
        lock (_handoffLock)
        {
            if (_importedHandoffs.Contains(handoffId))
                return;
        }

        var ops = new List<RowOp>(rows.Length);
        foreach (var wire in rows)
        {
            var table = Engine.Schema.Get(new TableId(wire.Table));
            ops.Add(new RowOp(RowOpKind.Insert, table.Id, new RowKey(wire.Key), RehomeRow(table, wire.Row!)));
        }

        var marker = new HandoffMarker(handoffId, playerHex, fromShard, Shard.Value, null);
        lock (_handoffLock)
        {
            // Placeholder-then-real LSN, same reason as the freeze: the append below may snapshot.
            _unsettledImports[handoffId] = (marker, Engine.Log.HeadLsn + 1);
        }

        var record = Engine.ApplyInternal(
            ClusterRecordNames.HandoffImport, ClusterCaller, ops,
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), reconcile: true, alwaysAppend: true);
        lock (_handoffLock)
        {
            _importedHandoffs.Add(handoffId);
            if (_unsettledImports.ContainsKey(handoffId))
                _unsettledImports[handoffId] = (marker, record!.Lsn);
        }
    }

    /// <summary>
    /// The destination's last word: the origin is known resolved (released or aborted, or there
    /// never was one), so the import marker stops pinning log truncation. The settled marker is
    /// appended first, so a restart between append and truncation recovers the same conclusion.
    /// Idempotent.
    /// </summary>
    public void Settle(string handoffId)
    {
        HandoffMarker? marker;
        lock (_handoffLock)
        {
            if (!_unsettledImports.TryGetValue(handoffId, out var pending))
                return;
            marker = pending.Marker;
        }

        Engine.ApplyInternal(
            ClusterRecordNames.HandoffSettled, ClusterCaller, [],
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), alwaysAppend: true);
        lock (_handoffLock)
        {
            _unsettledImports.Remove(handoffId);
        }
    }

    /// <summary>
    /// The origin half, last step: delete the transferred rows and unfreeze. Only called once the
    /// destination confirmed its import is durable — between import and release the player exists
    /// on both logs but is writable on neither, which is the invariant.
    /// </summary>
    public void Release(string handoffId)
    {
        HandoffMarker marker;
        lock (_handoffLock)
        {
            if (!_pendingFreezes.TryGetValue(handoffId, out var pending))
                return; // Already released or aborted.
            marker = pending.Marker;
        }

        var ops = (marker.Rows ?? [])
            .Select(static r => new RowOp(RowOpKind.Delete, new TableId(r.Table), new RowKey(r.Key)))
            .ToList();
        Engine.ApplyInternal(
            ClusterRecordNames.HandoffRelease, ClusterCaller, ops,
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), reconcile: true, alwaysAppend: true);
        Unfreeze(handoffId, marker);
    }

    /// <summary>The origin half's failure exit: the destination never imported, so the player stays here.</summary>
    public void Abort(string handoffId)
    {
        HandoffMarker marker;
        lock (_handoffLock)
        {
            if (!_pendingFreezes.TryGetValue(handoffId, out var pending))
                return;
            marker = pending.Marker;
        }

        Engine.ApplyInternal(
            ClusterRecordNames.HandoffAbort, ClusterCaller, [],
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), alwaysAppend: true);
        Unfreeze(handoffId, marker);
    }

    private void Unfreeze(string handoffId, HandoffMarker marker)
    {
        lock (_handoffLock)
        {
            _pendingFreezes.Remove(handoffId);
            foreach (var reference in marker.Rows ?? [])
                _frozenRows.Remove((new TableId(reference.Table), new RowKey(reference.Key)));
        }
    }

    /// <summary>
    /// Rebuilds the handoff saga state from this shard's own log: a freeze with no release or
    /// abort re-freezes its rows (the node's reconciler then asks the hub whether the destination
    /// imported, and completes or aborts); an import record marks its handoff id done — and stays
    /// unsettled, pinning truncation, until a settled record follows it. The truncation floors
    /// registered over these sets are what guarantee this scan can never miss a live marker.
    /// </summary>
    private void RecoverHandoffState()
    {
        foreach (var record in Engine.Log.ReadFrom(Engine.Log.BaseLsn + 1))
        {
            if (!record.ReducerName.StartsWith("melange/handoff", StringComparison.Ordinal))
                continue;
            var marker = JsonSerializer.Deserialize<HandoffMarker>(record.Arguments.Span);
            if (marker is null)
                continue;
            switch (record.ReducerName)
            {
                case ClusterRecordNames.HandoffFreeze:
                    _pendingFreezes[marker.HandoffId] = (marker, record.Lsn);
                    break;
                case ClusterRecordNames.HandoffRelease:
                case ClusterRecordNames.HandoffAbort:
                    _pendingFreezes.Remove(marker.HandoffId);
                    break;
                case ClusterRecordNames.HandoffImport:
                    _importedHandoffs.Add(marker.HandoffId);
                    _unsettledImports[marker.HandoffId] = (marker, record.Lsn);
                    break;
                case ClusterRecordNames.HandoffSettled:
                    _unsettledImports.Remove(marker.HandoffId);
                    break;
            }
        }

        foreach (var (pending, _) in _pendingFreezes.Values)
        {
            foreach (var reference in pending.Rows ?? [])
                _frozenRows.Add((new TableId(reference.Table), new RowKey(reference.Key)));
        }
    }

    private static WireOp[] RowRefsOf(WireOp[] rows) =>
        [.. rows.Select(static r => new WireOp(r.Kind, r.Table, r.Key, null))];

    /// <summary>
    /// Re-homes a transferred row per the strategy's <see cref="IShardStrategy.RehomingOf"/>:
    /// rewrite the ShardBy column (instancing), or assert the content already resolves here
    /// (spatial) — a transferred row whose position contradicts its destination is a protocol
    /// error, and rewriting a chunk id to a shard key would be silent corruption.
    /// </summary>
    private byte[] RehomeRow(TableSchema table, byte[] rowBytes)
    {
        if ((_strategy?.RehomingOf(table.Id) ?? RowRehoming.RewriteShardBy) == RowRehoming.RewriteShardBy)
            return RewriteShardColumn(table, rowBytes);

        var resolved = _strategy!.ShardForRow(table.Id, table.ToRowRef(rowBytes));
        if (resolved != Shard)
        {
            throw new InvalidOperationException(
                $"Handoff row of table '{table.Name}' resolves to {resolved}, not this destination {Shard}. A " +
                "ByContent strategy transfers rows only once their content places them in the destination shard — " +
                "the boundary monitor triggers past the crossing margin, so an import resolving elsewhere means the " +
                "transfer was initiated for the wrong destination.");
        }

        return rowBytes;
    }

    private byte[] RewriteShardColumn(TableSchema table, byte[] rowBytes)
    {
        if (table.ShardBy is null)
        {
            throw new InvalidOperationException(
                $"Handoff row of table '{table.Name}' cannot be re-homed: the table declares no ShardBy column.");
        }

        var boxed = RowSerializer.Deserialize(table, rowBytes);
        var column = table.Column(table.ShardBy);
        column.SetValue(boxed, Convert.ChangeType(Shard.Value, column.ClrType, System.Globalization.CultureInfo.InvariantCulture));
        return RowSerializer.Serialize(table, boxed);
    }

    public void Dispose()
    {
        Scheduler.Stop();
        Scheduler.Dispose();
        Transport.Dispose();
        Engine.Dispose();
    }

    private sealed class Collector(SchemaRegistry schema) : IHandoffCollector
    {
        public readonly List<(TableSchema Table, RowKey Key, byte[] Bytes)> Rows = [];

        public void Add<TRow>(TRow row)
            where TRow : struct
        {
            var table = schema.Get(typeof(TRow));
            if (table.Placement != Placement.Partitioned)
                throw new InvalidOperationException($"Handoff moves Partitioned rows; table '{table.Name}' is {table.Placement}.");
            object boxed = row;
            var key = KeyCodec.Encode(table.PrimaryKey, table.PrimaryKey.GetValue(boxed)!);
            Rows.Add((table, key, RowSerializer.Serialize(table, boxed)));
        }
    }
}

/// <summary>Deep-clones the options object so per-shard engines can re-root their paths.</summary>
internal static class MelangeOptionsClone
{
    private static readonly JsonSerializerOptions CloneOptions = new()
    {
        PreferredObjectCreationHandling = System.Text.Json.Serialization.JsonObjectCreationHandling.Populate,
    };

    public static MelangeDbOptions DeepClone(MelangeDbOptions source) =>
        JsonSerializer.Deserialize<MelangeDbOptions>(JsonSerializer.SerializeToUtf8Bytes(source), CloneOptions)
        ?? throw new InvalidOperationException("Options clone deserialized to null.");
}
