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
        var subscription = ServerSubscription.Compile(sink, id, query, _engine.Schema, limits, policies, context);
        var initialSet = computeInitialSet
            ? CollectWithCeilings(subscription, limits, anchorLsn)
            : new InitialSet(anchorLsn, [], 0);

        if (!_byTable.TryGetValue(subscription.Schema.Id, out var list))
            _byTable[subscription.Schema.Id] = list = [];

        // Collapse an equal wire column set onto a peer's instance so the fan-out memo can key on
        // reference identity. Every player running the same client asks for the same projection,
        // so in practice a table's subscriptions share one set object.
        subscription.ShareWireColumns(list);
        list.Add(subscription);
        _activeByTable.AddOrUpdate(subscription.Schema.Name, 1, static (_, count) => count + 1);
        return (subscription, initialSet);
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
                hasOld = _engine.HotStore.TryGetRow(op.Table, op.Key, out oldRow);
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

        foreach (var (sink, bySubscription) in perSink)
        {
            var updates = new List<SubscriptionUpdate>(bySubscription.Count);
            foreach (var (subscription, ops) in bySubscription)
            {
                updates.Add(new SubscriptionUpdate(subscription.Id, ops));
                _telemetry?.RecordDeltaRows(subscription.Schema.Name, ops.Count);
            }

            sink.EnqueueDelta(new TransactionUpdateFrame(record.Lsn, updates) { Channel = MelangeChannels.Data });
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

    private InitialSet CollectWithCeilings(ServerSubscription subscription, SubscriptionsOptions limits, ulong anchorLsn)
    {
        var rows = new List<KeyValuePair<RowKey, ReadOnlyMemory<byte>>>();
        var perRowColumns = subscription.Evaluator is { HasColumnPolicies: true } && subscription.Context is not null
            ? new List<IReadOnlySet<string>?>()
            : null;
        long bytes = 0;
        var filtered = 0;
        var decoded = new DecodedRow();
        foreach (var pair in subscription.MatchingRows(_engine.HotStore))
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
