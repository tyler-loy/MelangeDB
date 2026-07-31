using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>The durable saga record a handoff marker carries in its log arguments.</summary>
internal sealed record HandoffMarker(string HandoffId, string PlayerHex, ulong FromShard, ulong ToShard, WireOp[]? Rows);

/// <summary>Reserved reducer names for cluster-internal log records.</summary>
internal static class ClusterRecordNames
{
    public const string Replica = "melange/replica";
    public const string HandoffFreeze = "melange/handoff-freeze";
    public const string HandoffImport = "melange/handoff-import";
    public const string HandoffRelease = "melange/handoff-release";
    public const string HandoffAbort = "melange/handoff-abort";
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
    private readonly Dictionary<string, HandoffMarker> _pendingFreezes = new(StringComparer.Ordinal);
    private readonly HashSet<string> _importedHandoffs = new(StringComparer.Ordinal);
    private readonly IHandoffSet? _handoffSet;

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
                return [.. _pendingFreezes.Values];
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
        lock (_handoffLock)
        {
            _pendingFreezes[handoffId] = marker;
        }

        Engine.ApplyInternal(
            ClusterRecordNames.HandoffFreeze, ClusterCaller, [],
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), alwaysAppend: true);
        return rows;
    }

    private WireOp[] CollectFrozen(string handoffId)
    {
        HandoffMarker marker;
        lock (_handoffLock)
        {
            marker = _pendingFreezes[handoffId];
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
    /// internal commit whose marker carries the handoff id. Idempotent — a re-delivered import
    /// reconciles into updates and the id was already recorded.
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
            ops.Add(new RowOp(RowOpKind.Insert, table.Id, new RowKey(wire.Key), RewriteShardColumn(table, wire.Row!)));
        }

        var marker = new HandoffMarker(handoffId, playerHex, fromShard, Shard.Value, null);
        Engine.ApplyInternal(
            ClusterRecordNames.HandoffImport, ClusterCaller, ops,
            arguments: JsonSerializer.SerializeToUtf8Bytes(marker), reconcile: true, alwaysAppend: true);
        lock (_handoffLock)
        {
            _importedHandoffs.Add(handoffId);
        }
    }

    /// <summary>
    /// The origin half, last step: delete the transferred rows and unfreeze. Only called once the
    /// destination confirmed its import is durable — between import and release the player exists
    /// on both logs but is writable on neither, which is the invariant.
    /// </summary>
    public void Release(string handoffId)
    {
        HandoffMarker? marker;
        lock (_handoffLock)
        {
            if (!_pendingFreezes.TryGetValue(handoffId, out marker))
                return; // Already released or aborted.
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
        HandoffMarker? marker;
        lock (_handoffLock)
        {
            if (!_pendingFreezes.TryGetValue(handoffId, out marker))
                return;
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
    /// abort re-freezes its rows (the node runtime then asks the hub whether the destination
    /// imported, and completes or aborts); an import record marks its handoff id done, which is
    /// how a re-delivered import stays idempotent across a crash.
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
                    _pendingFreezes[marker.HandoffId] = marker;
                    break;
                case ClusterRecordNames.HandoffRelease:
                case ClusterRecordNames.HandoffAbort:
                    _pendingFreezes.Remove(marker.HandoffId);
                    break;
                case ClusterRecordNames.HandoffImport:
                    _importedHandoffs.Add(marker.HandoffId);
                    break;
            }
        }

        foreach (var pending in _pendingFreezes.Values)
        {
            foreach (var reference in pending.Rows ?? [])
                _frozenRows.Add((new TableId(reference.Table), new RowKey(reference.Key)));
        }
    }

    private static WireOp[] RowRefsOf(WireOp[] rows) =>
        [.. rows.Select(static r => new WireOp(r.Kind, r.Table, r.Key, null))];

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
