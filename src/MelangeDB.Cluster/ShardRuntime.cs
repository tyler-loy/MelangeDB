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

/// <summary>
/// The durable form of the borrowed-row registry: the registry's full state, consistent at
/// <see cref="Lsn"/> of log epoch <see cref="Epoch"/>. Border records below a snapshot's
/// truncation base are gone while their rows survive in the snapshot, so the registry cannot be
/// rebuilt from the log alone — this sidecar is its snapshot, and recovery is sidecar plus log
/// tail, exactly the engine's own snapshot-plus-replay pattern.
/// </summary>
internal sealed record BorrowedSidecar(Guid Epoch, ulong Lsn, BorrowedSidecar.Entry[] Rows)
{
    public sealed record Entry(uint Table, byte[] Key, ulong Owner);
}

/// <summary>Reserved reducer names for cluster-internal log records.</summary>
internal static class ClusterRecordNames
{
    public const string Replica = "melange/replica";
    public const string Border = "melange/border";
    public const string BorderReset = "melange/border-reset";
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
internal sealed partial class ShardRuntime : IDisposable
{
    private static readonly Identity ClusterCaller = Identity.Hash("melange/cluster");

    private readonly Lock _handoffLock = new();
    private readonly HashSet<(TableId Table, RowKey Key)> _frozenRows = [];
    private readonly Dictionary<string, (HandoffMarker Marker, ulong Lsn)> _pendingFreezes = new(StringComparer.Ordinal);
    private readonly HashSet<string> _importedHandoffs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (HandoffMarker Marker, ulong Lsn)> _unsettledImports = new(StringComparer.Ordinal);
    private readonly IHandoffSet? _handoffSet;
    private readonly IShardStrategy? _strategy;

    /// <summary>
    /// Serializes ownership <em>decisions</em> — border applies versus handoff imports — so a
    /// border op can never judge a row "not owned here" concurrently with the import that makes it
    /// owned. Never taken by the commit guard (which runs under the engine's write lock), so the
    /// lock order is always ownership lock then engine lock, never the reverse.
    /// </summary>
    private readonly Lock _ownershipLock = new();

    /// <summary>
    /// The read-only border copies this shard holds, keyed to the owning shard. Concurrent because
    /// the commit guard reads it under the engine's write lock while appliers mutate it under
    /// <see cref="_ownershipLock"/>.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(TableId Table, RowKey Key), ulong> _borrowedRows = new();
    private readonly ILogger _logger;

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
        _logger = loggerFactory.CreateLogger("MelangeDB.Cluster.Shard");
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
            FrozenSnapshot,
            BorrowedOwnerOf));

        RecoverHandoffState();

        // Handoff markers must survive log truncation for as long as their saga is unresolved —
        // the origin's freeze until it releases or aborts, the destination's import until the
        // origin is known settled. Truncating one away would silently unfreeze a mid-transfer
        // player (origin) or make WasImported answer a wrong "no" (destination): either way, two
        // owners — the exact silent-gap bug class phase 08 fixed for the Postgres checkpoint.
        Engine.AddTruncationFloor(() => MarkerFloor(_pendingFreezes));
        Engine.AddTruncationFloor(() => MarkerFloor(_unsettledImports));

        // Not a pin — a refresh point. Truncation may erase border records whose effects only the
        // borrowed sidecar remembers, so the sidecar is rewritten at the head the moment
        // truncation is being decided (this runs under the engine's write lock, like the snapshot
        // itself): after the write, everything at or below the head is reconstructible from
        // sidecar plus tail, and nothing needs pinning. The stale-sidecar check in recovery stays
        // as the loud safety net.
        Engine.AddTruncationFloor(() =>
        {
            WriteBorrowedSidecar();
            return null;
        });

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

    /// <summary>Which shard owns this row's border copy, or null when the row is not borrowed.</summary>
    public ulong? BorrowedOwnerOf(TableId table, RowKey key) =>
        _borrowedRows.TryGetValue((table, key), out var owner) ? owner : null;

    /// <summary>How many read-only border copies this shard currently holds — the band's footprint in rows.</summary>
    public int BorrowedRowCount => _borrowedRows.Count;

    /// <summary>
    /// Applies one border batch from a neighbouring owner shard. Owner-wins rules, judged per op
    /// under the ownership lock: an op never touches a row this shard holds authoritatively (the
    /// import supersedes any stale copy in flight), and a delete only lands if the copy was
    /// borrowed from the same owner that sent it — during an ownership transfer two neighbours
    /// briefly publish the same entity, and the origin's trailing delete must not erase the
    /// destination's fresh copy. The cursor is persisted before the caller acks, so delivery is
    /// at-least-once and re-application reconciles.
    /// </summary>
    public void ApplyBorder(BorderBatch batch)
    {
        lock (_ownershipLock)
        {
            var ops = new List<RowOp>(batch.Ops.Length);
            foreach (var wire in batch.Ops)
            {
                if (!Engine.Schema.TryGet(new TableId(wire.Table), out var table) || table.Placement != Placement.Partitioned)
                    continue;
                var key = (table.Id, new RowKey(wire.Key));
                var borrowed = _borrowedRows.TryGetValue(key, out var owner);
                var ownedHere = !borrowed && Engine.HotStore.TryGetRow(key.Item1, key.Item2, out _);
                if (ownedHere)
                    continue;
                if (wire.Kind == (byte)RowOpKind.Delete)
                {
                    if (!borrowed || owner != batch.OwnerShard)
                        continue;
                    ops.Add(wire.ToRowOp());
                    _borrowedRows.TryRemove(key, out _);
                }
                else
                {
                    ops.Add(wire.ToRowOp());
                    _borrowedRows[key] = batch.OwnerShard;
                }
            }

            if (ops.Count > 0)
            {
                Engine.ApplyInternal(
                    ClusterRecordNames.Border, ClusterCaller, ops,
                    arguments: JsonSerializer.SerializeToUtf8Bytes(new BorderMarker(batch.OwnerShard)), reconcile: true);
                WriteBorrowedSidecar();
            }

            WriteBorderCursor(batch.OwnerShard, batch.Epoch, batch.UpToLsn);
        }
    }

    /// <summary>
    /// Applies a full band reset from one owner: upsert every snapshot row, delete every copy
    /// previously borrowed from <em>that owner</em> the snapshot lacks — the owner deleted it
    /// during the gap its truncated log can no longer serve, and a pure upsert would resurrect it.
    /// The same rigor as the replica stream's bootstrap.
    /// </summary>
    public void ApplyBorderReset(BorderReset reset)
    {
        lock (_ownershipLock)
        {
            var ops = new List<RowOp>();
            var seen = new HashSet<(TableId, RowKey)>();
            foreach (var table in reset.Tables)
            {
                if (!Engine.Schema.TryGet(new TableId(table.Table), out var schema) || schema.Placement != Placement.Partitioned)
                    continue;
                foreach (var wire in table.Rows)
                {
                    var key = (schema.Id, new RowKey(wire.Key));
                    seen.Add(key);
                    var borrowed = _borrowedRows.ContainsKey(key);
                    if (!borrowed && Engine.HotStore.TryGetRow(key.Item1, key.Item2, out _))
                        continue; // Owned here authoritatively; the snapshot's copy is the stale one.
                    ops.Add(wire.ToRowOp());
                    _borrowedRows[key] = reset.OwnerShard;
                }
            }

            foreach (var (key, owner) in _borrowedRows)
            {
                if (owner == reset.OwnerShard && !seen.Contains(key))
                {
                    ops.Add(new RowOp(RowOpKind.Delete, key.Table, key.Key));
                    _borrowedRows.TryRemove(key, out _);
                }
            }

            if (ops.Count > 0)
            {
                Engine.ApplyInternal(
                    ClusterRecordNames.BorderReset, ClusterCaller, ops,
                    arguments: JsonSerializer.SerializeToUtf8Bytes(new BorderMarker(reset.OwnerShard)), reconcile: true);
                WriteBorrowedSidecar();
            }

            WriteBorderCursor(reset.OwnerShard, reset.Epoch, reset.Lsn);
        }
    }

    /// <summary>The observer's durable cursor into one owner's log: epoch, LSN, and the band depth it was taken at.</summary>
    public (string Epoch, ulong Lsn, int BandChunks) ReadBorderCursor(ulong ownerShard, int currentBand)
    {
        var path = BorderCursorPath(ownerShard);
        try
        {
            if (File.Exists(path))
            {
                var parts = File.ReadAllText(path).Split('|');
                if (parts.Length == 3 && ulong.TryParse(parts[1], out var lsn) && int.TryParse(parts[2], out var band))
                    return (parts[0], lsn, band);
            }
        }
        catch (IOException)
        {
        }

        return (string.Empty, 0, currentBand);
    }

    private int _borderCursorBand;

    /// <summary>The band depth the next persisted cursor records; set by the subscribe sweep.</summary>
    public void SetBorderCursorBand(int bandChunks) => _borderCursorBand = bandChunks;

    private void WriteBorderCursor(ulong ownerShard, string epoch, ulong lsn) =>
        File.WriteAllText(BorderCursorPath(ownerShard), $"{epoch}|{lsn}|{_borderCursorBand}");

    private string BorderCursorPath(ulong ownerShard) => Path.Combine(Directory, $"border-from-{ownerShard}.cursor");

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

            // Defense in depth against a confused trigger: a freeze may only cover rows this
            // shard owns. A border copy in the transfer set means someone asked the wrong origin
            // to move an entity — exporting the stale copy would overwrite the true owner's row.
            foreach (var (table, key, _) in collector.Rows)
            {
                if (_borrowedRows.TryGetValue((table.Id, key), out var owner))
                {
                    throw new InvalidOperationException(
                        $"Handoff {handoffId}: the transfer set for {player} includes a row of '{table.Name}' that is " +
                        $"a read-only border copy owned by shard:{owner} — this shard does not own the entity and " +
                        "cannot be its transfer origin.");
                }
            }

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

        // Under the ownership lock: the import is what turns a borrowed border copy into an
        // authoritative row, and a border op racing this decision must observe either the copy or
        // the ownership, never a half of each.
        CommitRecord? record;
        lock (_ownershipLock)
        {
            record = Engine.ApplyInternal(
                ClusterRecordNames.HandoffImport, ClusterCaller, ops,
                arguments: JsonSerializer.SerializeToUtf8Bytes(marker), reconcile: true, alwaysAppend: true);
            foreach (var op in ops)
                _borrowedRows.TryRemove((op.Table, op.Key), out _);
            WriteBorrowedSidecar();
        }

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
    /// Rebuilds the handoff saga state and the borrowed-row registry from this shard's own log: a
    /// freeze with no release or abort re-freezes its rows (the node's reconciler then asks the
    /// hub whether the destination imported, and completes or aborts); an import record marks its
    /// handoff id done — and stays unsettled, pinning truncation, until a settled record follows
    /// it — and un-borrows the rows it made authoritative; border records add and remove borrowed
    /// copies in the order they were applied. The truncation floors registered over the saga sets
    /// are what guarantee this scan can never miss a live marker, and the borrowed registry needs
    /// no floor at all — border records survive exactly as long as their rows, because both live
    /// in the same log the snapshot captures.
    /// </summary>
    private void RecoverHandoffState()
    {
        var (sidecarLsn, sidecarUsable) = LoadBorrowedSidecar();
        foreach (var record in Engine.Log.ReadFrom(Engine.Log.BaseLsn + 1))
        {
            if (record.ReducerName is ClusterRecordNames.Border or ClusterRecordNames.BorderReset)
            {
                // Only records past the sidecar's LSN: an older add replayed over a newer sidecar
                // would resurrect an entry a later import already retired.
                if (record.Lsn <= sidecarLsn)
                    continue;
                var border = JsonSerializer.Deserialize<BorderMarker>(record.Arguments.Span);
                if (border is null)
                    continue;
                foreach (var op in record.WriteSet)
                {
                    if (op.Kind == RowOpKind.Delete)
                        _borrowedRows.TryRemove((op.Table, op.Key), out _);
                    else
                        _borrowedRows[(op.Table, op.Key)] = border.Owner;
                }

                continue;
            }

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
                    if (record.Lsn > sidecarLsn)
                    {
                        foreach (var op in record.WriteSet)
                            _borrowedRows.TryRemove((op.Table, op.Key), out _);
                    }

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

        if (!sidecarUsable)
            RebuildBorrowedFromStore();
        WriteBorrowedSidecar();
    }

    /// <summary>
    /// Seeds the borrowed registry from its sidecar. Returns the LSN the sidecar is consistent at
    /// and whether it was usable — an unusable one (missing while the log is truncated, another
    /// epoch, older than the truncation base, or unreadable) means the log tail alone cannot
    /// reconstruct the registry, and the store-scan fallback must run instead.
    /// </summary>
    private (ulong Lsn, bool Usable) LoadBorrowedSidecar()
    {
        var path = BorrowedSidecarPath;
        try
        {
            if (!File.Exists(path))
                return (0, Engine.Log.BaseLsn == 0); // A fresh (or pre-sidecar) shard: the full log is the registry.
            var sidecar = JsonSerializer.Deserialize<BorrowedSidecar>(File.ReadAllBytes(path));
            if (sidecar is null || sidecar.Epoch != Engine.Log.EpochId || sidecar.Lsn < Engine.Log.BaseLsn)
                return (0, Engine.Log.BaseLsn == 0);
            foreach (var entry in sidecar.Rows)
                _borrowedRows[(new TableId(entry.Table), new RowKey(entry.Key))] = entry.Owner;
            return (sidecar.Lsn, true);
        }
        catch (Exception exception) when (exception is IOException or JsonException)
        {
            return (0, Engine.Log.BaseLsn == 0);
        }
    }

    /// <summary>
    /// The loud fallback: rebuilds the registry from row content — a Partitioned row resolving to
    /// a foreign shard is a border copy owned by that shard, unless a pending freeze names it (an
    /// entity this shard still owns, frozen mid-transfer across the line). Correct by the
    /// strategy's own definition, and a full scan, which is why the sidecar exists to make it the
    /// exception. Also the upgrade path for shard directories that predate the sidecar.
    /// </summary>
    private void RebuildBorrowedFromStore()
    {
        if (_strategy is null)
            return;
        _borrowedRows.Clear();
        var frozen = FrozenSnapshot();
        var rebuilt = 0;
        foreach (var table in Engine.Schema.Tables)
        {
            if (table.Placement != Placement.Partitioned)
                continue;
            foreach (var (key, bytes) in Engine.HotStore.Scan(table.Id))
            {
                if (frozen.Contains((table.Id, key)))
                    continue;
                var owner = _strategy.ShardForRow(table.Id, table.ToRowRef(bytes));
                if (owner != Shard)
                {
                    _borrowedRows[(table.Id, key)] = owner.Value;
                    rebuilt++;
                }
            }
        }

        LogBorrowedRebuilt(_logger, Shard.Value, rebuilt);
    }

    /// <summary>Only file-write serialization: holders never take the engine or ownership lock inside it.</summary>
    private readonly Lock _sidecarLock = new();

    private void WriteBorrowedSidecar()
    {
        var entries = _borrowedRows
            .Select(static pair => new BorrowedSidecar.Entry(pair.Key.Table.Value, pair.Key.Key.ToArray(), pair.Value))
            .ToArray();
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new BorrowedSidecar(Engine.Log.EpochId, Engine.Log.HeadLsn, entries));
        lock (_sidecarLock)
        {
            File.WriteAllBytes(BorrowedSidecarPath, bytes);
        }
    }

    private string BorrowedSidecarPath => Path.Combine(Directory, "borrowed.sidecar");

    [LoggerMessage(EventId = 1716, EventName = "BorrowedRegistryRebuilt", Level = LogLevel.Warning,
        Message = "Shard {Shard}'s borrowed-row sidecar was missing or unusable while its log is truncated; the registry was " +
            "rebuilt from row content ({Rows} border copies) — correct, but a full scan. Expected once when upgrading a " +
            "pre-phase-10 shard directory; recurring, it means the sidecar is not surviving restarts.")]
    private static partial void LogBorrowedRebuilt(ILogger logger, ulong shard, int rows);

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
