namespace MelangeDB.Core;

/// <summary>
/// The applier advancing the hot store projection. Thin by design: the store owns index
/// maintenance and its own checkpoint, so this is just the cursor.
/// </summary>
public sealed class HotStoreApplier : ILogApplier
{
    private readonly IHotStore _store;

    public HotStoreApplier(IHotStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
    }

    public string Name => "hot-store";

    public ulong AppliedLsn => _store.AppliedLsn;

    public void Apply(CommitRecord record) => _store.Apply(record);
}
