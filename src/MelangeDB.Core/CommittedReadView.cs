namespace MelangeDB.Core;

/// <summary>
/// A read-only <see cref="IDbView"/> over committed state — the view handed to policies and
/// reports. Reads go straight to the hot store with no overlay; because commit observers run
/// under the engine's write lock <em>before</em> the store applies, a policy evaluating during
/// delta fan-out reads exactly the pre-transaction committed state its deltas are computed
/// against, never a partially applied write set. Mutation methods throw.
/// </summary>
/// <remarks>
/// <para>
/// <b>Per-record read cache (#315).</b> The subscription fan-out judges every subscriber's row
/// policy against every op of a record, and a policy that reads the caller's own row
/// (<c>ctx.Db.Find&lt;T&gt;(ctx.Caller)</c>) used to read and decode that row from the store each
/// time: twice per op per subscriber. Committed state cannot change while one record fans out
/// (the fan-out holds the write lock and runs before the store applies the record), so between
/// <see cref="BeginRecord"/> and <see cref="EndRecord"/> a <see cref="Find{TRow}"/> on the
/// fan-out's thread remembers what it found, misses included, and each looked-up row is decoded
/// once per record.
/// </para>
/// <para>
/// The cache serves only the thread that began the record. This view is shared: ad-hoc SQL,
/// handoff collection and consistent reads use it from other threads, off the write lock, and
/// they keep reading the store. A hit returns a copy of the row struct, but reference-typed
/// columns (a <c>byte[]</c>, a list) are shared with the cache, so a policy must not modify
/// them, which it must not do to committed state anyway.
/// </para>
/// </remarks>
internal sealed class CommittedReadView : IDbView
{
    private readonly TransactionDb _reads;
    private readonly Dictionary<Type, FoundRows> _found = [];

    /// <summary>The managed thread id the cache serves, or 0 with no record open.</summary>
    private int _cacheThread;

    public CommittedReadView(SchemaRegistry registry, IHotStoreReader store, TableAccessGuard? guard = null) =>
        _reads = new TransactionDb(registry, store, new WriteSet(), null!, guard);

    public TRow Insert<TRow>(TRow row)
        where TRow : struct
        => throw ReadOnly();

    public void Update<TRow>(TRow row)
        where TRow : struct
        => throw ReadOnly();

    public bool Delete<TRow>(object primaryKey)
        where TRow : struct
        => throw ReadOnly();

    public TRow? Find<TRow>(object primaryKey)
        where TRow : struct
        => Volatile.Read(ref _cacheThread) == Environment.CurrentManagedThreadId
            ? _reads.Find<TRow>(primaryKey, FoundFor<TRow>())
            : _reads.Find<TRow>(primaryKey);

    /// <summary>
    /// Starts caching <see cref="Find{TRow}"/> on the calling thread for one commit record. The
    /// caller guarantees committed state does not change until <see cref="EndRecord"/>: it holds
    /// the engine's write lock, ahead of the store applying the record.
    /// </summary>
    internal void BeginRecord()
    {
        ClearFound();
        Volatile.Write(ref _cacheThread, Environment.CurrentManagedThreadId);
    }

    /// <summary>Stops caching and forgets the record's reads. Call it in a <c>finally</c>.</summary>
    internal void EndRecord()
    {
        Volatile.Write(ref _cacheThread, 0);
        ClearFound();
    }

    /// <summary>Entries a cleared table cache keeps room for, so one huge record does not pin its capacity.</summary>
    private const int RetainedEntries = 4096;

    private Dictionary<RowKey, TRow?> FoundFor<TRow>()
        where TRow : struct
    {
        if (!_found.TryGetValue(typeof(TRow), out var rows))
        {
            rows = new FoundRows<TRow>();
            _found[typeof(TRow)] = rows;
        }

        return ((FoundRows<TRow>)rows).Rows;
    }

    private void ClearFound()
    {
        foreach (var rows in _found.Values)
            rows.Clear();
    }

    /// <summary>
    /// One table's cached lookups, typed so a row is stored inline rather than boxed: the cache
    /// keeps its capacity between records, so a steady fan-out allocates nothing for it.
    /// </summary>
    private abstract class FoundRows
    {
        public abstract void Clear();
    }

    private sealed class FoundRows<TRow> : FoundRows
        where TRow : struct
    {
        public Dictionary<RowKey, TRow?> Rows { get; } = [];

        public override void Clear()
        {
            var large = Rows.Count > RetainedEntries;
            Rows.Clear();
            if (large)
                Rows.TrimExcess(RetainedEntries);
        }
    }

    public IEnumerable<TRow> Scan<TRow>()
        where TRow : struct
        => _reads.Scan<TRow>();

    public IEnumerable<TRow> Filter<TRow>(string column, object value)
        where TRow : struct
        => _reads.Filter<TRow>(column, value);

    public IEnumerable<TRow> FilterRange<TRow>(string column, object low, object high)
        where TRow : struct
        => _reads.FilterRange<TRow>(column, low, high);

    public bool ContainsKey<TRow>(object primaryKey)
        where TRow : struct
        => _reads.ContainsKey<TRow>(primaryKey);

    public bool Any<TRow>()
        where TRow : struct
        => _reads.Any<TRow>();

    public long Count<TRow>()
        where TRow : struct
        => _reads.Count<TRow>();

    public TRow? First<TRow>()
        where TRow : struct
        => _reads.First<TRow>();

    private static InvalidOperationException ReadOnly() =>
        new("This view is read-only committed state; policies must not write. Mutate through a reducer.");
}
