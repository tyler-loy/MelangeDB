namespace MelangeDB.Core;

/// <summary>
/// A read-only <see cref="IDbView"/> over committed state — the view handed to policies and
/// reports. Reads go straight to the hot store with no overlay; because commit observers run
/// under the engine's write lock <em>before</em> the store applies, a policy evaluating during
/// delta fan-out reads exactly the pre-transaction committed state its deltas are computed
/// against, never a partially applied write set. Mutation methods throw.
/// </summary>
internal sealed class CommittedReadView : IDbView
{
    private readonly TransactionDb _reads;

    public CommittedReadView(SchemaRegistry registry, IHotStore store) =>
        _reads = new TransactionDb(registry, store, new WriteSet(), null!);

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
        => _reads.Find<TRow>(primaryKey);

    public IEnumerable<TRow> Scan<TRow>()
        where TRow : struct
        => _reads.Scan<TRow>();

    public IEnumerable<TRow> Filter<TRow>(string column, object value)
        where TRow : struct
        => _reads.Filter<TRow>(column, value);

    public IEnumerable<TRow> FilterRange<TRow>(string column, object low, object high)
        where TRow : struct
        => _reads.FilterRange<TRow>(column, low, high);

    private static InvalidOperationException ReadOnly() =>
        new("This view is read-only committed state; policies must not write. Mutate through a reducer.");
}
