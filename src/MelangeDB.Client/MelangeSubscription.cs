using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>
/// One row in a subscription's local cache: the encoded primary key and the row's schema-ordered
/// v1 bytes, shaped by the subscription's <see cref="WireDescriptor"/>.
/// <para>
/// <see cref="Columns"/> is the untyped view, and it is decoded on first read rather than on
/// arrival. That laziness is most of protocol v2's client-side win: a typed client holds a
/// <see cref="MelangeSubscription"/> under every typed cache, and it never asks for the map at all,
/// so the dictionary, the strings, and the boxes are never built.
/// </para>
/// </summary>
public sealed class MelangeRow
{
    // Benignly racy: two readers can each build a map, and both are equal and correct. A lock here
    // would put contention on the read path of every row to save an allocation that happens once.
    private IReadOnlyDictionary<string, object?>? _columns;

    internal MelangeRow(byte[] key, ReadOnlyMemory<byte> row, ReadOnlyMemory<byte> columnMask, WireDescriptor descriptor)
    {
        Key = key;
        Row = row;
        ColumnMask = columnMask;
        Descriptor = descriptor;
    }

    /// <summary>The encoded primary key — the same bytes the server keys deltas with.</summary>
    public byte[] Key { get; }

    /// <summary>The row's values as v1 row bytes, in <see cref="Descriptor"/> order.</summary>
    public ReadOnlyMemory<byte> Row { get; }

    /// <summary>
    /// Which descriptor columns <see cref="Row"/> carries, as a bitset; empty means all of them,
    /// which is every row on a subscription without column policies.
    /// </summary>
    public ReadOnlyMemory<byte> ColumnMask { get; }

    /// <summary>The shape <see cref="Row"/> is encoded in, sent once per subscription.</summary>
    public WireDescriptor Descriptor { get; }

    /// <summary>The row's values by column name, decoded on first read and then cached.</summary>
    public IReadOnlyDictionary<string, object?> Columns =>
        _columns ??= WireRowValues.ToColumns(Descriptor, Row.Span, ColumnMask.Span);
}

/// <summary>
/// A live subscription and its locally maintained row cache — a projection of the server's state,
/// loaded from the initial set and advanced by deltas. Events fire on the thread that applies
/// frames: the client's receive loop under Immediate dispatch, the <c>FrameTick</c> caller under
/// Manual. Deltas that arrive while the initial set is still streaming are buffered and applied
/// once the set completes, keeping the anchor-LSN boundary gap-free and duplicate-free on this
/// side too.
/// </summary>
public sealed class MelangeSubscription
{
    private readonly Lock _lock = new();
    private readonly Dictionary<ByteKey, MelangeRow> _rows = [];
    private readonly List<WireRow> _initialRows = [];
    private readonly List<(ulong Lsn, IReadOnlyList<WireRowOp> Ops)> _pending = [];
    private readonly ISubscriptionSink? _sink;
    private TaskCompletionSource _applied = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private WireDescriptor? _descriptor;
    private ulong _anchorLsn;
    private bool _live;

    internal MelangeSubscription(uint id, string query, IReadOnlyDictionary<string, object?>? parameters, ISubscriptionSink? sink = null)
    {
        Id = id;
        Query = query;
        Parameters = parameters;
        _sink = sink;
    }

    public uint Id { get; }

    public string Query { get; }

    /// <summary>The current parameter values; replaced when the subscription is re-scoped.</summary>
    public IReadOnlyDictionary<string, object?>? Parameters { get; internal set; }

    /// <summary>Completes when the initial result set has been fully applied.</summary>
    public Task Applied => _applied.Task;

    /// <summary>The LSN the initial set was consistent at.</summary>
    public ulong AnchorLsn => _anchorLsn;

    /// <summary>
    /// The shape this subscription's rows arrive in, sent by the server on the first initial-set
    /// chunk. Null until the first chunk lands. It survives a resume deliberately: a resume is only
    /// accepted against the same log epoch, and a schema change is a new epoch.
    /// </summary>
    public WireDescriptor? Descriptor => _descriptor;

    /// <summary>Fires for a row entering the subscription — by insert, or by moving into scope.</summary>
    public event Action<MelangeRow>? OnInsert;

    /// <summary>Fires for a changed row: (previous, current).</summary>
    public event Action<MelangeRow, MelangeRow>? OnUpdate;

    /// <summary>Fires for a row leaving the subscription — by delete, or by moving out of scope.</summary>
    public event Action<MelangeRow>? OnDelete;

    /// <summary>
    /// Deltas that contradicted the cache: an insert for a key already present, or an update for a
    /// key never seen. Always zero unless the initial-set/delta boundary leaked a gap or a
    /// duplicate — the invariant the anchored-LSN design exists to hold. Deletes for absent keys
    /// are not counted; resume replay emits those legitimately.
    /// </summary>
    public long Inconsistencies { get; private set; }

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _rows.Count;
            }
        }
    }

    /// <summary>A snapshot of the cached rows.</summary>
    public IReadOnlyList<MelangeRow> Rows
    {
        get
        {
            lock (_lock)
            {
                return [.. _rows.Values];
            }
        }
    }

    public bool TryGetRow(byte[] key, out MelangeRow row)
    {
        lock (_lock)
        {
            return _rows.TryGetValue(new ByteKey(key), out row!);
        }
    }

    internal void AcceptInitialChunk(SubscriptionAppliedFrame chunk)
    {
        List<(ulong, IReadOnlyList<WireRowOp>)>? replay = null;
        List<MelangeRow>? snapshot = null;
        lock (_lock)
        {
            // An initial set arriving for a live subscription is a server-driven re-scope — the
            // gateway swapped the attachment to another shard and re-issued the subscription
            // there. The old anchor counts against the old log, so applying the new node's
            // deltas against it while this set streams would corrupt the cache it is about to
            // replace; buffer them (exactly as during the first subscribe) and let the replay
            // below filter them against the anchor this set actually names.
            _live = false;

            // The descriptor rides on chunk 0 and describes every row that follows it, including
            // the deltas that arrive long after the set completes. A re-established or re-scoped
            // subscription gets a fresh one and replaces the old.
            if (chunk.Descriptor is { } descriptor)
                _descriptor = descriptor;
            _initialRows.AddRange(chunk.Rows);
            if (!chunk.IsLast)
                return;

            var shape = _descriptor ?? throw new MelangeProtocolException(
                $"Subscription {Id} received an initial set with no wire descriptor; the server never sent one on chunk 0.");
            _anchorLsn = chunk.AnchorLsn;
            _rows.Clear();
            foreach (var row in _initialRows)
                _rows[new ByteKey(row.Key)] = new MelangeRow(row.Key, row.Row, row.ColumnMask, shape);
            _initialRows.Clear();
            _live = true;
            replay = [.. _pending];
            _pending.Clear();
            if (_sink is not null)
                snapshot = [.. _rows.Values];
        }

        // The typed cache sees the completed set before the buffered deltas replay through
        // OnRowOp, so its reconciliation order matches the untyped cache's exactly. A decode
        // failure here is schema drift caught at its earliest observable moment — fail the
        // subscribe if it is still failable, else let the receive loop die loudly.
        if (snapshot is not null)
        {
            try
            {
                _sink!.OnSnapshot(snapshot);
            }
            catch (MelangeSchemaMismatchException exception)
            {
                if (!FailSubscribe(MelangeErrorCodes.Protocol, exception.Message))
                    throw;
            }
        }

        if (replay is not null)
        {
            foreach (var (lsn, ops) in replay)
                Apply(lsn, ops);
        }

        _applied.TrySetResult();
    }

    internal void Apply(ulong lsn, IReadOnlyList<WireRowOp> ops)
    {
        lock (_lock)
        {
            if (!_live)
            {
                _pending.Add((lsn, ops));
                return;
            }

            // Deltas at or below the anchor were already in the initial set; the server never
            // sends them, and dropping any stray one is what "no duplicate" means here.
            if (lsn != 0 && lsn <= _anchorLsn)
                return;
        }

        foreach (var op in ops)
            ApplyOp(op);
    }

    internal void ResetForResync()
    {
        lock (_lock)
        {
            _live = false;
            _rows.Clear();
            _initialRows.Clear();
            _pending.Clear();
            if (_applied.Task.IsCompleted)
                _applied = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        _sink?.OnReset();
    }

    internal bool FailSubscribe(string code, string message) =>
        _applied.TrySetException(new MelangeSubscriptionException(code, message));

    internal bool IsLive
    {
        get
        {
            lock (_lock)
            {
                return _live;
            }
        }
    }

    private void ApplyOp(in WireRowOp op)
    {
        MelangeRow? previous = null;
        MelangeRow? current = null;
        var kind = op.Kind;
        lock (_lock)
        {
            // A subscription is only live after a completed initial set, and that set carried the
            // descriptor — so a delta can never reach here without one.
            var shape = _descriptor!;
            var key = new ByteKey(op.Key);
            switch (op.Kind)
            {
                case RowOpKind.Insert:
                    if (_rows.TryGetValue(key, out var duplicate))
                    {
                        Inconsistencies++;
                        previous = duplicate;
                        kind = RowOpKind.Update;
                    }

                    current = new MelangeRow(op.Key, op.Row, op.ColumnMask, shape);
                    _rows[key] = current;
                    break;
                case RowOpKind.Update:
                    if (_rows.TryGetValue(key, out var existing))
                    {
                        previous = existing;
                    }
                    else
                    {
                        Inconsistencies++;
                        kind = RowOpKind.Insert;
                    }

                    current = new MelangeRow(op.Key, op.Row, op.ColumnMask, shape);
                    _rows[key] = current;
                    break;
                case RowOpKind.Delete:
                    if (_rows.Remove(key, out var removed))
                        previous = removed;
                    break;
            }
        }

        // The typed cache hears the resolved op first: its consistency must not depend on what a
        // user's untyped handler does with the same event.
        if (kind is RowOpKind.Insert or RowOpKind.Update ? current is not null : previous is not null)
            _sink?.OnRowOp(kind, previous, current);

        switch (kind)
        {
            case RowOpKind.Insert when current is not null:
                OnInsert?.Invoke(current);
                break;
            case RowOpKind.Update when current is not null:
                OnUpdate?.Invoke(previous ?? current, current);
                break;
            case RowOpKind.Delete when previous is not null:
                OnDelete?.Invoke(previous);
                break;
        }
    }
}
