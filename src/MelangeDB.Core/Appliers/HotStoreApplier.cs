namespace MelangeDB.Core;

/// <summary>
/// The applier advancing the hot store projection. Thin by design: the store owns index
/// maintenance and its own checkpoint, so this is just the cursor.
/// </summary>
public sealed class HotStoreApplier : ILogApplier
{
    private readonly IHotStore _store;
    private readonly Func<CommitRecord, CommitRecord>? _filter;

    /// <param name="store">The projection to advance.</param>
    /// <param name="filter">
    /// Narrows each record to what the hot store holds — the engine passes one that removes
    /// archive-tier ops, whose rows live only in Postgres.
    /// </param>
    public HotStoreApplier(IHotStore store, Func<CommitRecord, CommitRecord>? filter = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
        _filter = filter;
    }

    public string Name => "hot-store";

    public ulong AppliedLsn => _store.AppliedLsn;

    public void Apply(CommitRecord record) => _store.Apply(_filter is null ? record : _filter(record));
}
