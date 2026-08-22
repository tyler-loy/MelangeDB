namespace MelangeDB.Core.Tests;

/// <summary>
/// An <see cref="IHotStore"/> that deliberately does <b>not</b> implement
/// <see cref="IReadViewSource"/>: a delegating wrapper whose only distinguishing feature is the
/// capability it withholds. Both shipped stores offer pinned reads, so without this there is no way
/// to exercise the path a third-party or future store would take — and "the fallback is untested"
/// is how a fallback becomes a crash.
/// </summary>
internal sealed class ReadViewlessStore(IHotStore inner) : IHotStore
{
    public static IHotStoreProvider Provider { get; } = new StoreProvider();

    public ulong AppliedLsn => inner.AppliedLsn;

    public void Apply(CommitRecord record) => inner.Apply(record);

    public HotStoreStatistics Statistics() => inner.Statistics();

    public void LoadSnapshot(ulong lsn, IEnumerable<SnapshotRow> rows) => inner.LoadSnapshot(lsn, rows);

    public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row) =>
        inner.TryGetRow(table, key, out row);

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table) => inner.Scan(table);

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value) =>
        inner.ScanIndex(table, column, value);

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high) =>
        inner.ScanIndexRange(table, column, low, high);

    public long Count(TableId table) => inner.Count(table);

    public IEnumerable<RowKey> ScanKeys(TableId table) => inner.ScanKeys(table);

    public IEnumerable<RowKey> ScanKeyRange(TableId table, RowKey low, RowKey high) =>
        inner.ScanKeyRange(table, low, high);

    private sealed class StoreProvider : IHotStoreProvider
    {
        public HotStoreEngine Engine => HotStoreEngine.InMemory;

        public IHotStore Create(HotStoreContext context) => new ReadViewlessStore(new InMemoryHotStore(context.Schema));
    }
}
