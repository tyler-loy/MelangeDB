using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>
/// The internal seam a subscription reports through so a typed cache can mirror it: the completed
/// initial set (first subscribe, re-establishment, and server-driven rescope all land here), each
/// applied row op with its self-healed kind, and resync resets. Invoked on the thread applying
/// the frame — the receive loop under Immediate dispatch, the <c>FrameTick</c> caller under
/// Manual — in apply order, before the subscription's public events fire.
/// </summary>
internal interface ISubscriptionSink
{
    /// <summary>A completed initial set replaced the subscription's rows; buffered deltas replay next.</summary>
    void OnSnapshot(IReadOnlyList<MelangeRow> rows);

    /// <summary>
    /// One applied row op. <paramref name="kind"/> is the resolved kind after the cache's
    /// insert↔update self-healing; deletes carry the removed row as <paramref name="previous"/>
    /// and are not reported when the key was absent.
    /// </summary>
    void OnRowOp(RowOpKind kind, MelangeRow? previous, MelangeRow? current);

    /// <summary>The subscription reset for a full resync; a fresh snapshot follows.</summary>
    void OnReset();
}

/// <summary>
/// One table's merged client cache: rows from every subscription over the table, keyed by encoded
/// primary key, refcounted per key. The server sends a row once <em>per subscription</em> — the
/// engine computes deltas per registered subscription with no cross-subscription dedup on a
/// connection — so the merge counts covering subscriptions per key and derives typed events from
/// transitions, not from wire op kinds: first coverage fires <c>OnInsert</c>, a value change fires
/// <c>OnUpdate</c> once (the overlapping subscription's identical copy compares equal and stays
/// silent), and only the last uncovering fires <c>OnDelete</c>. A rescope's snapshot reconciles by
/// diff against that subscription's covered keys — no flush, no event storm.
/// </summary>
public sealed class ClientCache<TRow>
    where TRow : struct
{
    private readonly Lock _lock = new();
    private readonly IClientRowCodec<TRow> _codec;
    private readonly ClientCacheRegistry _registry;
    private readonly Dictionary<ByteKey, Entry> _entries = [];

    internal ClientCache(IClientRowCodec<TRow> codec, ClientCacheRegistry registry)
    {
        _codec = codec;
        _registry = registry;
    }

    /// <summary>Fires when a key gains its first covering subscription.</summary>
    public event Action<TRow>? OnInsert;

    /// <summary>Fires once per value change: (previous, current). Overlapping copies stay silent.</summary>
    public event Action<TRow, TRow>? OnUpdate;

    /// <summary>Fires when a key loses its last covering subscription — by delete, scope exit, or unsubscribe.</summary>
    public event Action<TRow>? OnDelete;

    /// <summary>The wire table name this cache mirrors.</summary>
    public string TableName => _codec.TableName;

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>A snapshot of the cached rows.</summary>
    public IReadOnlyList<TRow> Rows
    {
        get
        {
            lock (_lock)
            {
                var rows = new TRow[_entries.Count];
                var i = 0;
                foreach (var entry in _entries.Values)
                    rows[i++] = entry.Row;
                return rows;
            }
        }
    }

    /// <summary>Finds a row by its already-encoded primary key.</summary>
    public bool TryFind(byte[] encodedPrimaryKey, out TRow row)
    {
        ArgumentNullException.ThrowIfNull(encodedPrimaryKey);
        lock (_lock)
        {
            if (_entries.TryGetValue(new ByteKey(encodedPrimaryKey), out var entry))
            {
                row = entry.Row;
                return true;
            }

            row = default;
            return false;
        }
    }

    /// <summary>Finds the cached row with the same primary key as <paramref name="row"/>.</summary>
    public bool TryFind(in TRow row, out TRow cached) => TryFind(_codec.EncodePrimaryKey(in row), out cached);

    internal IClientRowCodec<TRow> Codec => _codec;

    internal TypedCacheBinding<TRow> CreateBinding() => new(this);

    internal void Cover(TypedCacheBinding<TRow> binding, MelangeRow row)
    {
        List<Action>? pending = null;
        lock (_lock)
        {
            CoverLocked(binding, new ByteKey(row.Key), row, ref pending);
        }

        Dispatch(pending);
    }

    internal void Uncover(TypedCacheBinding<TRow> binding, byte[] key)
    {
        List<Action>? pending = null;
        lock (_lock)
        {
            UncoverLocked(binding, new ByteKey(key), ref pending);
        }

        Dispatch(pending);
    }

    /// <summary>
    /// Reconciles one subscription's completed initial set against its previous coverage: deletes
    /// for keys that left scope, inserts for arrivals, an update for survivors whose bytes
    /// changed — the rescope contract, and re-establishment after resync takes the same path.
    /// </summary>
    internal void ApplySnapshot(TypedCacheBinding<TRow> binding, IReadOnlyList<MelangeRow> rows)
    {
        List<Action>? pending = null;
        lock (_lock)
        {
            var incoming = new HashSet<ByteKey>();
            foreach (var row in rows)
                incoming.Add(new ByteKey(row.Key));

            var departed = new List<ByteKey>();
            foreach (var key in binding.Covered)
            {
                if (!incoming.Contains(key))
                    departed.Add(key);
            }

            foreach (var key in departed)
                UncoverLocked(binding, key, ref pending);
            foreach (var row in rows)
                CoverLocked(binding, new ByteKey(row.Key), row, ref pending);
        }

        Dispatch(pending);
    }

    /// <summary>Drops every key this subscription covered; rows it alone held leave with OnDelete.</summary>
    internal void Detach(TypedCacheBinding<TRow> binding)
    {
        List<Action>? pending = null;
        lock (_lock)
        {
            foreach (var key in new List<ByteKey>(binding.Covered))
                UncoverLocked(binding, key, ref pending);
        }

        Dispatch(pending);
    }

    private void CoverLocked(
        TypedCacheBinding<TRow> binding,
        ByteKey key,
        MelangeRow row,
        ref List<Action>? pending)
    {
        binding.VerifyShape(_codec, row);
        var newlyCovered = binding.Covered.Add(key);
        if (_entries.TryGetValue(key, out var entry))
        {
            if (newlyCovered)
                entry.RefCount++;
            if (!entry.Bytes.Span.SequenceEqual(row.Row.Span))
            {
                var previous = entry.Row;
                var current = _codec.DecodeRow(row.Row.Span);
                entry.Row = current;
                entry.Bytes = row.Row;
                var fire = OnUpdate;
                if (fire is not null)
                    (pending ??= []).Add(() => fire(previous, current));
            }

            return;
        }

        var inserted = _codec.DecodeRow(row.Row.Span);
        _entries[key] = new Entry { Row = inserted, Bytes = row.Row, RefCount = 1 };
        var fireInsert = OnInsert;
        if (fireInsert is not null)
            (pending ??= []).Add(() => fireInsert(inserted));
    }

    private void UncoverLocked(TypedCacheBinding<TRow> binding, ByteKey key, ref List<Action>? pending)
    {
        if (!binding.Covered.Remove(key) || !_entries.TryGetValue(key, out var entry))
            return;
        entry.RefCount--;
        if (entry.RefCount > 0)
            return;
        _entries.Remove(key);
        var fire = OnDelete;
        if (fire is not null)
        {
            var removed = entry.Row;
            (pending ??= []).Add(() => fire(removed));
        }
    }

    private void Dispatch(List<Action>? pending)
    {
        if (pending is null)
            return;
        foreach (var action in pending)
            _registry.DispatchTypedEvent(action);
    }

    /// <summary>
    /// One cached row: the decoded struct, and the wire bytes it was decoded from — which are the
    /// duplicate detector that keeps an overlapping subscription's identical copy of an update from
    /// firing a second event.
    /// <para>
    /// Under protocol v1 this compared two column maps value by value, with an explicit
    /// <c>Equals</c> case so a NaN would compare equal to itself. Byte comparison gets that for
    /// free and is exact besides: two subscriptions over one table share a descriptor, so equal
    /// rows are byte-identical rows.
    /// </para>
    /// </summary>
    private sealed class Entry
    {
        public required TRow Row;
        public required ReadOnlyMemory<byte> Bytes;
        public required int RefCount;
    }
}

/// <summary>
/// The bridge from one subscription's sink callbacks to a table's merged cache, owning the set of
/// keys that subscription currently covers. Coverage mutates only under the cache's lock.
/// </summary>
internal sealed class TypedCacheBinding<TRow> : ISubscriptionSink
    where TRow : struct
{
    private readonly ClientCache<TRow> _cache;
    private WireDescriptor? _verified;

    internal TypedCacheBinding(ClientCache<TRow> cache) => _cache = cache;

    /// <summary>The keys this subscription currently covers. Guarded by the cache's lock.</summary>
    internal HashSet<ByteKey> Covered { get; } = [];

    /// <summary>
    /// Checks, once per descriptor, that the server's shape is the one these bindings were
    /// generated from — and per row, that no column policy narrowed it. A binding belongs to one
    /// subscription, which holds one descriptor instance for its life, so the structural comparison
    /// runs once per initial set and the per-row cost is an emptiness test on a span.
    /// </summary>
    internal void VerifyShape(IClientRowCodec<TRow> codec, MelangeRow row)
    {
        if (!ReferenceEquals(_verified, row.Descriptor))
        {
            ClientRowShape.Verify(codec.TableName, codec.Columns, row.Descriptor);
            _verified = row.Descriptor;
        }

        if (row.ColumnMask.IsEmpty)
            return;

        throw new MelangeSchemaMismatchException(
            $"Table '{codec.TableName}': a column policy masked columns out of this row, so it cannot fill the generated row struct. "
            + "Read partially visible rows through the untyped subscription API, which reports exactly which columns arrived.");
    }

    public void OnSnapshot(IReadOnlyList<MelangeRow> rows) => _cache.ApplySnapshot(this, rows);

    public void OnRowOp(RowOpKind kind, MelangeRow? previous, MelangeRow? current)
    {
        if (kind == RowOpKind.Delete)
        {
            if (previous is not null)
                _cache.Uncover(this, previous.Key);
        }
        else if (current is not null)
        {
            _cache.Cover(this, current);
        }
    }

    public void OnReset()
    {
        // Deliberately nothing: a reset is always followed by a fresh initial set, and the
        // snapshot diff against the still-held coverage is what turns a full resync into precise
        // deletes and inserts instead of a flush.
    }

    public void Detach() => _cache.Detach(this);
}
