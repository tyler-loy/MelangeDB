using System.Collections.Concurrent;
using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>A registered subscription's initial set: the anchor LSN and the matching row references.</summary>
internal sealed record InitialSet(ulong AnchorLsn, IReadOnlyList<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Rows, long Bytes);

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
        bool computeInitialSet)
    {
        var subscription = ServerSubscription.Compile(sink, id, query, _engine.Schema, limits);
        var initialSet = computeInitialSet
            ? CollectWithCeilings(subscription, limits, anchorLsn)
            : new InitialSet(anchorLsn, [], 0);

        if (!_byTable.TryGetValue(subscription.Schema.Id, out var list))
            _byTable[subscription.Schema.Id] = list = [];
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

        var previousKeys = new HashSet<RowKey>();
        foreach (var (key, _) in subscription.MatchingRows(_engine.HotStore))
            previousKeys.Add(key);

        subscription.Rescope(query, limits);

        var ops = new List<WireRowOp>();
        var nowKeys = new HashSet<RowKey>();
        foreach (var (key, row) in subscription.MatchingRows(_engine.HotStore))
        {
            nowKeys.Add(key);
            if (!previousKeys.Contains(key))
                ops.Add(new WireRowOp(RowOpKind.Insert, key.ToArray(), RowWire.ToColumns(subscription.Schema, row.Span, subscription.Projection)));
        }

        foreach (var key in previousKeys)
        {
            if (!nowKeys.Contains(key))
                ops.Add(new WireRowOp(RowOpKind.Delete, key.ToArray(), null));
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
        foreach (var op in record.WriteSet)
        {
            if (!_byTable.TryGetValue(op.Table, out var subscriptions) || subscriptions.Count == 0)
                continue;

            var hasOld = _engine.HotStore.TryGetRow(op.Table, op.Key, out var oldRow);
            _telemetry?.SampleDeltaSpan(subscriptions[0].Schema.Name, subscriptions.Count);
            foreach (var subscription in subscriptions)
            {
                var delta = ComputeDelta(subscription, op, hasOld, oldRow);
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
    /// pre-images, so updates that do not match emit conservative deletes — a client applies a
    /// delete for a row it never had as a no-op, which keeps replay correct without them.
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
                    (ops ??= []).Add(new WireRowOp(RowOpKind.Delete, op.Key.ToArray(), null));
                }
                else if (subscription.Matches(op.Key, op.Row.Span))
                {
                    (ops ??= []).Add(new WireRowOp(
                        op.Kind,
                        op.Key.ToArray(),
                        RowWire.ToColumns(subscription.Schema, op.Row.Span, subscription.Projection)));
                }
                else if (op.Kind == RowOpKind.Update)
                {
                    (ops ??= []).Add(new WireRowOp(RowOpKind.Delete, op.Key.ToArray(), null));
                }
            }

            if (ops is not null)
                updates.Add(new SubscriptionUpdate(subscription.Id, ops));
        }

        return updates;
    }

    private WireRowOp? ComputeDelta(ServerSubscription subscription, in RowOp op, bool hasOld, ReadOnlyMemory<byte> oldRow)
    {
        var oldMatch = hasOld && subscription.Matches(op.Key, oldRow.Span);
        var newMatch = op.Kind != RowOpKind.Delete && subscription.Matches(op.Key, op.Row.Span);
        if (newMatch && !oldMatch)
            return new WireRowOp(RowOpKind.Insert, op.Key.ToArray(), RowWire.ToColumns(subscription.Schema, op.Row.Span, subscription.Projection));
        if (newMatch && oldMatch)
        {
            // A projected subscription must not emit when only non-projected columns changed —
            // that would be wasted bandwidth on the hottest path.
            if (subscription.Projection is { } projection
                && RowWire.ProjectedEqual(subscription.Schema, oldRow.Span, op.Row.Span, projection))
            {
                return null;
            }

            return new WireRowOp(RowOpKind.Update, op.Key.ToArray(), RowWire.ToColumns(subscription.Schema, op.Row.Span, subscription.Projection));
        }

        if (!newMatch && oldMatch)
            return new WireRowOp(RowOpKind.Delete, op.Key.ToArray(), null);
        return null;
    }

    private InitialSet CollectWithCeilings(ServerSubscription subscription, SubscriptionsOptions limits, ulong anchorLsn)
    {
        var rows = new List<KeyValuePair<RowKey, ReadOnlyMemory<byte>>>();
        long bytes = 0;
        foreach (var pair in subscription.MatchingRows(_engine.HotStore))
        {
            rows.Add(pair);
            bytes += pair.Value.Length;
            EnforceCeilings(subscription, limits, rows.Count, bytes);
        }

        return new InitialSet(anchorLsn, rows, bytes);
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
