using System.Collections.Concurrent;
using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>
/// A registered subscription's initial set: the anchor LSN and the matching, policy-visible row
/// references. <see cref="RowColumns"/> carries each row's mask-evaluated column set when the
/// table has column policies — computed under the write lock at the anchor, so the columns match
/// the state the rows were collected at; null means the subscription's static wire columns apply.
/// </summary>
internal sealed record InitialSet(
    ulong AnchorLsn,
    IReadOnlyList<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Rows,
    long Bytes,
    IReadOnlyList<IReadOnlySet<string>?>? RowColumns = null);

/// <summary>
/// The server-wide subscription registry and delta computer. Subscriptions are indexed by table,
/// so a commit touching table T tests only T's subscriptions — the settled answer to fan-out cost;
/// key-range indexing within a table is deferred until a measurement demands it. Every mutation
/// and every fan-out runs under the engine's write lock, which is the whole consistency story: a
/// subscription registered at anchor LSN A sees exactly the commits with LSN &gt; A.
/// </summary>
internal sealed class SubscriptionEngine
{
    private readonly MelangeEngine _engine;
    private readonly Dictionary<TableId, List<ServerSubscription>> _byTable = [];
    private readonly ConcurrentDictionary<string, int> _activeByTable = new(StringComparer.Ordinal);
    private readonly ServerTelemetry? _telemetry;

    /// <summary>
    /// Initial sets being collected off the write lock, each recording the commits that land while
    /// it reads. Mutated and read only under the engine's write lock (Fanout runs under it).
    /// </summary>
    private readonly List<CatchUp> _catchUps = [];

    public SubscriptionEngine(MelangeEngine engine, ServerTelemetry? telemetry)
    {
        _engine = engine;
        _telemetry = telemetry;
    }

    /// <summary>Active subscription counts per table — the <c>melange.subscriptions.active</c> gauge.</summary>
    public IEnumerable<KeyValuePair<string, int>> ActiveByTable => _activeByTable;

    /// <summary>
    /// Compiles, cost-checks, and registers a subscription, returning its initial set anchored at
    /// the current head LSN. Runs under the engine's write lock. Cost ceilings reject before
    /// anything is serialized or sent — by streaming time the damage is done.
    /// </summary>
    public (ServerSubscription Subscription, InitialSet InitialSet) Register(
        IDeltaSink sink,
        uint id,
        SubscriptionQuery query,
        SubscriptionsOptions limits,
        ulong anchorLsn,
        bool computeInitialSet,
        PolicySet? policies = null,
        PolicyContext? context = null)
    {
        var subscription = Compile(sink, id, query, limits, policies, context);
        var initialSet = computeInitialSet
            ? CollectWithCeilings(subscription, limits, anchorLsn, _engine.HotStore)
            : new InitialSet(anchorLsn, [], 0);
        Add(subscription);
        return (subscription, initialSet);
    }

    /// <summary>Compiles a subscription without registering it; needs no lock.</summary>
    public ServerSubscription Compile(
        IDeltaSink sink, uint id, SubscriptionQuery query, SubscriptionsOptions limits, PolicySet? policies, PolicyContext? context) =>
        ServerSubscription.Compile(sink, id, query, _engine.Schema, limits, policies, context);

    /// <summary>
    /// Registers a compiled subscription for the live fan-out: it sees every commit after this
    /// point. Runs under the engine's write lock.
    /// </summary>
    public void Add(ServerSubscription subscription)
    {
        if (!_byTable.TryGetValue(subscription.Schema.Id, out var list))
            _byTable[subscription.Schema.Id] = list = [];

        // Collapse an equal wire column set onto a peer's instance so the fan-out memo can key on
        // reference identity. Every player running the same client asks for the same projection,
        // so in practice a table's subscriptions share one set object.
        subscription.ShareWireColumns(list);
        list.Add(subscription);
        _activeByTable.AddOrUpdate(subscription.Schema.Name, 1, static (_, count) => count + 1);
    }

    /// <summary>
    /// The commits that land while an initial set is read off the write lock. Begun under the same
    /// lock hold that pins the read (see <c>MelangeEngine.ReadAtHead</c>), so it holds exactly the
    /// records after the pinned LSN; see <see cref="CatchUpInitialSet"/>.
    /// </summary>
    internal sealed class CatchUp
    {
        public List<CommitRecord> Records { get; } = [];
    }

    /// <summary>Starts recording commits for an off-lock initial set. Runs under the engine's write lock.</summary>
    public CatchUp BeginCatchUp()
    {
        var catchUp = new CatchUp();
        _catchUps.Add(catchUp);
        return catchUp;
    }

    /// <summary>Stops recording. Runs under the engine's write lock; idempotent.</summary>
    public void EndCatchUp(CatchUp catchUp) => _catchUps.Remove(catchUp);

    /// <summary>
    /// Collects an initial set from <paramref name="source"/>, a view pinned at
    /// <paramref name="anchorLsn"/>, with the cost ceilings enforced. Needs no lock: this is the
    /// part of a subscribe that scans, and on a large paged table it used to hold every commit and
    /// the node's heartbeat for its whole length (#213). Only for a subscription without row or
    /// column policies, whose verdicts would otherwise read live state rather than the pin.
    /// </summary>
    public InitialSet CollectInitialSet(ServerSubscription subscription, SubscriptionsOptions limits, IHotStoreReader source, ulong anchorLsn) =>
        CollectWithCeilings(subscription, limits, anchorLsn, source);

    /// <summary>
    /// Brings an initial set collected at its anchor up to <paramref name="head"/> by applying the
    /// commits <paramref name="catchUp"/> recorded since, and re-checks the ceilings. Runs under
    /// the engine's write lock, immediately before <see cref="Add"/>, so the returned set's anchor
    /// is the registration's: the client sees exactly the commits after it, as if the whole set
    /// had been read under the lock. Exact, not conservative: the set itself is in hand, so a row
    /// that left scope is simply removed.
    /// </summary>
    public InitialSet CatchUpInitialSet(
        ServerSubscription subscription, SubscriptionsOptions limits, InitialSet set, CatchUp catchUp, ulong head)
    {
        if (catchUp.Records.Count == 0)
            return set with { AnchorLsn = head };

        var rows = new Dictionary<RowKey, (ReadOnlyMemory<byte> Row, IReadOnlySet<string>? Columns)>(set.Rows.Count);
        for (var i = 0; i < set.Rows.Count; i++)
            rows[set.Rows[i].Key] = (set.Rows[i].Value, set.RowColumns?[i]);

        var decoded = new DecodedRow();
        foreach (var record in catchUp.Records)
        {
            foreach (var op in record.WriteSet)
            {
                if (op.Table != subscription.Schema.Id)
                    continue;
                if (op.Kind == RowOpKind.Delete)
                {
                    rows.Remove(op.Key);
                    continue;
                }

                decoded.Reset(subscription.Schema, op.Row);
                if (subscription.Matches(op.Key, decoded) && subscription.PolicyAdmits(decoded))
                    rows[op.Key] = (op.Row, set.RowColumns is null ? null : subscription.VisibleColumns(decoded));
                else
                    rows.Remove(op.Key);
            }
        }

        var list = new List<KeyValuePair<RowKey, ReadOnlyMemory<byte>>>(rows.Count);
        var columns = set.RowColumns is null ? null : new List<IReadOnlySet<string>?>(rows.Count);
        long bytes = 0;
        foreach (var (key, (row, rowColumns)) in rows)
        {
            list.Add(new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, row));
            columns?.Add(rowColumns);
            bytes += row.Length;
        }

        EnforceCeilings(subscription, limits, list.Count, bytes);
        return new InitialSet(head, list, bytes, columns);
    }

    /// <summary>Removes a subscription. Runs under the engine's write lock.</summary>
    public void Unregister(ServerSubscription subscription)
    {
        if (_byTable.TryGetValue(subscription.Schema.Id, out var list) && list.Remove(subscription))
            _activeByTable.AddOrUpdate(subscription.Schema.Name, 0, static (_, count) => count - 1);
    }

    /// <summary>
    /// Re-scopes a subscription and returns the precise diff — inserts for rows newly in scope,
    /// deletes for rows that left it — anchored at the current head. Runs under the engine's
    /// write lock, so the diff and the delta stream cannot interleave incorrectly.
    /// </summary>
    public IReadOnlyList<WireRowOp> Rescope(ServerSubscription subscription, SubscriptionQuery query, SubscriptionsOptions limits)
    {
        // Validate the new scope on a throwaway compilation first, so a rejected re-scope leaves
        // the live subscription untouched.
        var probe = ServerSubscription.Compile(subscription.Sink, subscription.Id, query, _engine.Schema, limits);
        if (probe.Schema.Id != subscription.Schema.Id)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.ParseError,
                "Re-scoping a subscription cannot change its table; unsubscribe and subscribe instead.");
        }

        CheckCeilings(probe, limits);

        var decoded = new DecodedRow();
        var previousKeys = new HashSet<RowKey>();
        foreach (var (key, row) in subscription.MatchingRows(_engine.HotStore))
        {
            if (subscription.PolicyAdmits(decoded.Reset(subscription.Schema, row)))
                previousKeys.Add(key);
        }

        subscription.Rescope(query, limits);

        var ops = new List<WireRowOp>();
        var nowKeys = new HashSet<RowKey>();
        foreach (var (key, row) in subscription.MatchingRows(_engine.HotStore))
        {
            decoded.Reset(subscription.Schema, row);
            if (!subscription.PolicyAdmits(decoded))
            {
                _telemetry?.RecordRowsFiltered(subscription.Schema.Name, 1);
                continue;
            }

            nowKeys.Add(key);
            if (previousKeys.Contains(key))
                continue;
            var visible = subscription.VisibleColumns(decoded);
            ops.Add(new WireRowOp(RowOpKind.Insert, key.ToArray(), RowWire.Project(subscription.Schema, row, visible), subscription.MaskFor(visible)));
        }

        foreach (var key in previousKeys)
        {
            if (!nowKeys.Contains(key))
                ops.Add(new WireRowOp(RowOpKind.Delete, key.ToArray(), default, default));
        }

        return ops;
    }

    /// <summary>
    /// The live fan-out, invoked as a commit observer: under the write lock, before the hot store
    /// applies — so the store still answers with each row's pre-image, which is what decides that
    /// an update moved a row out of a predicate (delete to that client) or into it (insert).
    /// </summary>
    public void Fanout(CommitRecord record)
    {
        foreach (var catchUp in _catchUps)
            catchUp.Records.Add(record);

        var perSink = default(Dictionary<IDeltaSink, Dictionary<ServerSubscription, List<WireRowOp>>>);
        var wire = default(WireRowMemo);
        var before = default(DecodedRow);
        var after = default(DecodedRow);

        // A record may carry several ops for one key (a border batch shipping a hot row's last
        // few ticks; reducer write sets coalesce and never do). The store's pre-image is
        // pre-*record*, so each op after the first must see the one before it as its old row, or
        // every observer holding the row is sent duplicate inserts.
        var withinRecord = record.WriteSet.Count > 1
            ? new Dictionary<(TableId, RowKey), (bool Exists, ReadOnlyMemory<byte> Row)>()
            : null;
        foreach (var op in record.WriteSet)
        {
            if (!_byTable.TryGetValue(op.Table, out var subscriptions) || subscriptions.Count == 0)
            {
                withinRecord?[(op.Table, op.Key)] = (op.Kind != RowOpKind.Delete, op.Row);
                continue;
            }

            bool hasOld;
            ReadOnlyMemory<byte> oldRow;
            if (withinRecord is not null && withinRecord.TryGetValue((op.Table, op.Key), out var effect))
                (hasOld, oldRow) = effect;
            else
                hasOld = TryReadPreImage(op.Table, op.Key, out oldRow);
            withinRecord?[(op.Table, op.Key)] = (op.Kind != RowOpKind.Delete, op.Row);
            _telemetry?.SampleDeltaSpan(subscriptions[0].Schema.Name, subscriptions.Count);

            // Every subscriber that receives this op receives the same key bytes and — unless a
            // column policy narrows them per row — the same decoded columns. Both are computed
            // once here and shared, because this loop runs under the engine's write lock, where
            // repeating a subscriber's worth of work N times is a global stall, not a local one.
            // Nothing downstream writes to either: the key is a read-only frame field and the
            // column map is serialized, never mutated.
            var key = op.Key.ToArray();
            var schema = subscriptions[0].Schema;
            (wire ??= new WireRowMemo()).Reset(schema, op.Row);

            // The pre-image and the new row, each decoded at most once for every subscriber on the
            // table: every predicate on an indexed column and every row or column policy reads the
            // same typed row, and only the verdict is per subscriber.
            (before ??= new DecodedRow()).Reset(schema, oldRow, present: hasOld);
            (after ??= new DecodedRow()).Reset(schema, op.Row, present: op.Kind != RowOpKind.Delete);

            foreach (var subscription in subscriptions)
            {
                var delta = ComputeDelta(subscription, op.Key, key, wire, before, after);
                if (delta is not { } wireOp)
                    continue;
                perSink ??= [];
                if (!perSink.TryGetValue(subscription.Sink, out var perSubscription))
                    perSink[subscription.Sink] = perSubscription = [];
                if (!perSubscription.TryGetValue(subscription, out var ops))
                    perSubscription[subscription] = ops = [];
                ops.Add(wireOp);
            }
        }

        if (perSink is null)
            return;

        var woken = new IDeltaSink[perSink.Count];
        var next = 0;
        foreach (var (sink, bySubscription) in perSink)
        {
            var updates = new List<SubscriptionUpdate>(bySubscription.Count);
            foreach (var (subscription, ops) in bySubscription)
            {
                updates.Add(new SubscriptionUpdate(subscription.Id, ops));
                _telemetry?.RecordDeltaRows(subscription.Schema.Name, ops.Count);
            }

            sink.EnqueueDeltaQuietly(new TransactionUpdateFrame(record.Lsn, updates) { Channel = MelangeChannels.Data });
            woken[next++] = sink;
        }

        // One pool item wakes every connection this commit reached, instead of one wake-up per
        // connection inside the lock (each queues a send loop's continuation to the pool). A frame
        // that waits for its wake-up waits for one pool dispatch, and its sender would need a pool
        // thread to run anyway.
        ThreadPool.UnsafeQueueUserWorkItem(static sinks => WakeAll(sinks), woken, preferLocal: false);
    }

    /// <summary>
    /// The deferred wake-up. It runs on a pool thread, possibly after a connection has closed, so a
    /// sink that throws is skipped rather than allowed to stop the others or escape: an exception
    /// on a pool work item ends the process.
    /// </summary>
    internal static void WakeAll(IDeltaSink[] sinks)
    {
        foreach (var sink in sinks)
        {
            try
            {
                sink.Wake();
            }
            catch (Exception)
            {
                // A closed connection has nothing left to send.
            }
        }
    }

    /// <summary>
    /// Computes one record's deltas for resumed subscriptions from the log alone. The log has no
    /// pre-images, so updates that are not visible — by predicate or by row policy — emit
    /// conservative deletes: a client applies a delete for a row it never had as a no-op, which
    /// keeps replay correct (and leak-free) without them.
    /// </summary>
    public static IReadOnlyList<SubscriptionUpdate> ComputeReplayUpdates(
        IReadOnlyList<ServerSubscription> subscriptions,
        CommitRecord record)
    {
        var updates = new List<SubscriptionUpdate>();
        foreach (var subscription in subscriptions)
        {
            List<WireRowOp>? ops = null;
            foreach (var op in record.WriteSet)
            {
                if (op.Table != subscription.Schema.Id)
                    continue;
                if (op.Kind == RowOpKind.Delete)
                {
                    (ops ??= []).Add(new WireRowOp(RowOpKind.Delete, op.Key.ToArray(), default, default));
                }
                else if (subscription.RowVisible(op.Key, op.Row.Span))
                {
                    var (bytes, mask) = subscription.WireForm(op.Row);
                    (ops ??= []).Add(new WireRowOp(op.Kind, op.Key.ToArray(), bytes, mask));
                }
                else if (op.Kind == RowOpKind.Update)
                {
                    (ops ??= []).Add(new WireRowOp(RowOpKind.Delete, op.Key.ToArray(), default, default));
                }
            }

            if (ops is not null)
                updates.Add(new SubscriptionUpdate(subscription.Id, ops));
        }

        return updates;
    }

    /// <summary>
    /// The pre-image of a row about to change, or "absent" when its stored projection cannot be
    /// read. The fan-out reads each op's pre-image to decide who held the row; a row whose
    /// out-of-line payload is unreadable would otherwise throw here and fail the whole commit —
    /// so a write that <em>repairs or removes</em> a poisoned row could never land, and the row
    /// would be stuck until a restart. Treating an unreadable pre-image as no prior row lets the
    /// write commit: a repairing update reaches subscribers as an insert of the corrected row, and
    /// a delete removes it. The store logs the unreadable row on its scan path (EventId 1511).
    /// </summary>
    private bool TryReadPreImage(TableId table, in RowKey key, out ReadOnlyMemory<byte> row)
    {
        try
        {
            return _engine.HotStore.TryGetRow(table, key, out row);
        }
        catch (InvalidDataException)
        {
            row = default;
            return false;
        }
    }

    private WireRowOp? ComputeDelta(
        ServerSubscription subscription,
        in RowKey rowKey,
        byte[] key,
        WireRowMemo wire,
        DecodedRow before,
        DecodedRow after)
    {
        // Predicate AND row policy decide visibility on both sides of the change. The store still
        // holds the pre-image here (the fan-out runs before the hot store applies), and policy
        // reads of other tables see the same pre-transaction committed state — never a partially
        // applied write set.
        var oldMatch = before.Present && subscription.Matches(rowKey, before);
        var newMatch = after.Present && subscription.Matches(rowKey, after);
        var oldVisible = oldMatch && subscription.PolicyAdmits(before);
        var newVisible = newMatch && subscription.PolicyAdmits(after);
        if (newMatch && !newVisible)
            _telemetry?.RecordRowsFiltered(subscription.Schema.Name, 1);

        if (newVisible && !oldVisible)
        {
            var inserted = subscription.VisibleColumns(after);
            return new WireRowOp(RowOpKind.Insert, key, wire.For(inserted), subscription.MaskFor(inserted));
        }

        if (newVisible && oldVisible)
        {
            var newColumns = subscription.VisibleColumns(after);
            if (newColumns is not null)
            {
                // A restricted subscription must not emit when only invisible columns changed:
                // beyond wasted bandwidth, an update frame for a [ServerOnly]-column change is a
                // timing oracle. A mask that itself changed still emits, with the new columns.
                var oldColumns = subscription.VisibleColumns(before);
                if (ColumnsEqual(oldColumns, newColumns)
                    && RowWire.ProjectedEqual(subscription.Schema, before.Span, after.Span, newColumns))
                {
                    return null;
                }
            }

            return new WireRowOp(RowOpKind.Update, key, wire.For(newColumns), subscription.MaskFor(newColumns));
        }

        if (!newVisible && oldVisible)
            return new WireRowOp(RowOpKind.Delete, key, default, default);
        return null;
    }

    /// <summary>
    /// One row's wire bytes, memoized across the subscriptions a fan-out visits it for.
    /// <para>
    /// The unprojected case is now free outright: the store holds the row in the format the wire
    /// wants, so a full row is the store's own memory handed to every subscriber — no decode, no
    /// dictionary, no copy. That is protocol v2's point, and it is why this memo shrank from
    /// deduplicating an expensive build to deduplicating a cheap one.
    /// </para>
    /// <para>
    /// Projections still cost a copy, so they are still memoized, keyed on the column set by
    /// reference: <see cref="ServerSubscription.ShareWireColumns"/> converges equal projections
    /// onto one instance at registration, so two hundred players running the same client share one
    /// projected copy. A per-row column-policy mask is a fresh set on every call and so misses
    /// deliberately — it is genuinely per row, and paying a set comparison to discover that would
    /// cost more than the copy it saves.
    /// </para>
    /// </summary>
    private sealed class WireRowMemo
    {
        private readonly Dictionary<object, ReadOnlyMemory<byte>> _projected = new(ReferenceEqualityComparer.Instance);
        private TableSchema _schema = null!;
        private ReadOnlyMemory<byte> _row;

        public void Reset(TableSchema schema, ReadOnlyMemory<byte> row)
        {
            _schema = schema;
            _row = row;
            _projected.Clear();
        }

        public ReadOnlyMemory<byte> For(IReadOnlySet<string>? columns)
        {
            if (columns is null)
                return _row;
            if (_projected.TryGetValue(columns, out var cached))
                return cached;

            var built = RowWire.Project(_schema, _row, columns);
            _projected[columns] = built;
            return built;
        }
    }

    private static bool ColumnsEqual(IReadOnlySet<string>? left, IReadOnlySet<string>? right)
    {
        if (left is null || right is null)
            return left is null && right is null;
        return left.Count == right.Count && left.SetEquals(right);
    }

    private InitialSet CollectWithCeilings(ServerSubscription subscription, SubscriptionsOptions limits, ulong anchorLsn, IHotStoreReader source)
    {
        var rows = new List<KeyValuePair<RowKey, ReadOnlyMemory<byte>>>();
        var perRowColumns = subscription.Evaluator is { HasColumnPolicies: true } && subscription.Context is not null
            ? new List<IReadOnlySet<string>?>()
            : null;
        long bytes = 0;
        var filtered = 0;
        var decoded = new DecodedRow();
        foreach (var pair in subscription.MatchingRows(source))
        {
            decoded.Reset(subscription.Schema, pair.Value);
            if (!subscription.PolicyAdmits(decoded))
            {
                filtered++;
                continue;
            }

            rows.Add(pair);
            perRowColumns?.Add(subscription.VisibleColumns(decoded));
            bytes += pair.Value.Length;
            EnforceCeilings(subscription, limits, rows.Count, bytes);
        }

        if (filtered > 0)
            _telemetry?.RecordRowsFiltered(subscription.Schema.Name, filtered);
        return new InitialSet(anchorLsn, rows, bytes, perRowColumns);
    }

    private void CheckCeilings(ServerSubscription subscription, SubscriptionsOptions limits)
    {
        long rows = 0;
        long bytes = 0;
        foreach (var pair in subscription.MatchingRows(_engine.HotStore))
        {
            rows++;
            bytes += pair.Value.Length;
            EnforceCeilings(subscription, limits, rows, bytes);
        }
    }

    private static void EnforceCeilings(ServerSubscription subscription, SubscriptionsOptions limits, long rows, long bytes)
    {
        if (rows > limits.MaxRowsPerSubscription)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.TooManyRows,
                $"Subscription on '{subscription.Schema.Name}' exceeds Subscriptions:MaxRowsPerSubscription ({limits.MaxRowsPerSubscription} rows). Narrow the predicate.");
        }

        if (bytes > limits.MaxBytesPerSubscription)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.TooManyBytes,
                $"Subscription on '{subscription.Schema.Name}' exceeds Subscriptions:MaxBytesPerSubscription ({limits.MaxBytesPerSubscription} bytes). Narrow the predicate or project fewer columns.");
        }
    }
}
