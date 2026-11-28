namespace MelangeDB.Core;

/// <summary>One row of an <see cref="InspectProjection"/>: its stored bytes, and the LSN that wrote them.</summary>
/// <param name="WrittenAt">
/// The record that last wrote the row, which is also the LSN whose shape decodes it; a row taken from
/// the snapshot carries the snapshot's LSN, since the snapshot does not say which record wrote it.
/// </param>
public readonly record struct ProjectedRow(TableId Table, RowKey Key, ReadOnlyMemory<byte> Row, ulong WrittenAt);

/// <summary>
/// The world as of one commit, rebuilt in memory from an <see cref="InspectSource"/>: the snapshot
/// replayed forward to the LSN when the snapshot is at or below it, or the log replayed from empty
/// when it still reaches back to the first commit (road-to-0.3 phase 21). It can only run forward:
/// the log holds each row's new value and each delete's key, never what a write replaced, so a
/// state before the snapshot with the log truncated below it cannot be built at all, and
/// <see cref="Build"/> refuses with the reason.
/// <para>
/// It is raw rows, decoded later against the recorded shapes: no indexes, no SQL, no row policies,
/// no residency. Archive-tier tables are never materialized: their rows ride the log and then live
/// only in Postgres, never in a snapshot, so the projection could only ever hold a fragment of them.
/// <see cref="LeftOut"/> names them.
/// </para>
/// <para>
/// <see cref="Apply"/> carries it forward one record at a time, which is how a window of records is
/// shown with each write's previous value: one replay for the whole window, not one per record.
/// </para>
/// </summary>
public sealed class InspectProjection
{
    private readonly InspectSource _source;
    private readonly Dictionary<TableId, Dictionary<RowKey, ProjectedRow>> _tables = [];
    private readonly HashSet<TableId> _leftOut = [];

    private InspectProjection(InspectSource source, ulong startLsn, bool fromSnapshot)
    {
        _source = source;
        StartLsn = startLsn;
        FromSnapshot = fromSnapshot;
        AtLsn = startLsn;
    }

    /// <summary>The LSN the projection stands at: every record up to it, and none after, is applied.</summary>
    public ulong AtLsn { get; private set; }

    /// <summary>Where the replay began: the snapshot's LSN, or 0 for a replay from empty.</summary>
    public ulong StartLsn { get; }

    /// <summary>Whether the replay began from the snapshot rather than from empty.</summary>
    public bool FromSnapshot { get; }

    /// <summary>How many log records have been applied on top of the starting state.</summary>
    public long Replayed { get; private set; }

    /// <summary>The archive-tier tables seen so far, which the projection does not hold.</summary>
    public IReadOnlyList<InspectTable> LeftOut =>
        [.. _leftOut
            .Select(id => _source.TableAt(AtLsn, id) ?? _source.TablesAt(StartLsn).FirstOrDefault(t => t.Id == id))
            .OfType<InspectTable>()
            .OrderBy(static t => t.Name, StringComparer.Ordinal)];

    /// <summary>Every table holding at least one row, with its row count.</summary>
    public IReadOnlyDictionary<TableId, int> Counts =>
        _tables.Where(static t => t.Value.Count > 0).ToDictionary(static t => t.Key, static t => t.Value.Count);

    /// <summary>
    /// Why the world as of <paramref name="lsn"/> cannot be built from <paramref name="source"/>, or
    /// null when it can.
    /// </summary>
    public static string? ExplainUnbuildable(InspectSource source, ulong lsn)
    {
        ArgumentNullException.ThrowIfNull(source);
        var facts = source.Facts;
        if (lsn > facts.HeadLsn)
            return source.ExplainUnreachable(lsn);
        if (facts.SnapshotLsn > 0 && facts.SnapshotLsn <= lsn)
            return null;
        if (facts.LogFloor == 0)
            return null;
        var snapshot = facts.SnapshotLsn > 0
            ? $"its snapshot is at LSN {facts.SnapshotLsn}, after it, "
            : "it has no snapshot, ";
        var floor = facts.Kind == InspectSourceKind.Archive
            ? $"and an archive carries only the log above its snapshot"
            : $"and its log begins after LSN {facts.BaseLsn} (its truncation base)";
        return $"The world as of LSN {lsn} cannot be built from this source: {snapshot}{floor}. The log records each row's new value, " +
            "never what it replaced, so a state can only be rebuilt forward from a snapshot at or before it, or from the first commit. " +
            "A backup taken before LSN " + lsn + " can rebuild it.";
    }

    /// <summary>Builds the world as of <paramref name="lsn"/>.</summary>
    /// <exception cref="InvalidOperationException">It cannot be built from this source; the message says why.</exception>
    public static InspectProjection Build(InspectSource source, ulong lsn)
    {
        if (ExplainUnbuildable(source, lsn) is { } refusal)
            throw new InvalidOperationException(refusal);

        var facts = source.Facts;
        var fromSnapshot = facts.SnapshotLsn > 0 && facts.SnapshotLsn <= lsn;
        var projection = new InspectProjection(source, fromSnapshot ? facts.SnapshotLsn : 0, fromSnapshot);
        if (fromSnapshot)
        {
            foreach (var row in source.SnapshotRows())
            {
                if (projection.IsLeftOut(facts.SnapshotLsn, row.Table))
                    continue;
                projection.TableFor(row.Table)[row.Key] = new ProjectedRow(row.Table, row.Key, row.Row, facts.SnapshotLsn);
            }
        }

        foreach (var record in source.Records(projection.StartLsn + 1, lsn))
            projection.Apply(record);
        projection.AtLsn = lsn;
        return projection;
    }

    /// <summary>
    /// Applies the next record. Records must come in LSN order, each past <see cref="AtLsn"/>;
    /// a record at or below it has already been applied and is refused.
    /// </summary>
    public void Apply(CommitRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.Lsn <= AtLsn)
            throw new InvalidOperationException($"The projection stands at LSN {AtLsn}; record {record.Lsn} is already applied.");
        foreach (var op in record.WriteSet)
        {
            if (IsLeftOut(record.Lsn, op.Table))
                continue;
            var rows = TableFor(op.Table);
            if (op.Kind == RowOpKind.Delete)
                rows.Remove(op.Key);
            else
                rows[op.Key] = new ProjectedRow(op.Table, op.Key, op.Row, record.Lsn);
        }

        AtLsn = record.Lsn;
        Replayed++;
    }

    /// <summary>The row with <paramref name="key"/>, or null when the table holds none as of <see cref="AtLsn"/>.</summary>
    public ProjectedRow? Find(TableId table, in RowKey key) =>
        _tables.TryGetValue(table, out var rows) && rows.TryGetValue(key, out var row) ? row : null;

    /// <summary>The table's rows in primary-key order (the key encoding preserves the order of the values).</summary>
    public IEnumerable<ProjectedRow> Rows(TableId table) =>
        _tables.TryGetValue(table, out var rows)
            ? rows.Values.OrderBy(static r => r.Key)
            : [];

    /// <summary>Whether <paramref name="table"/> is one the projection does not hold: archive-tier under the shape governing <paramref name="lsn"/>.</summary>
    public bool IsLeftOut(TableId table) => _leftOut.Contains(table);

    private bool IsLeftOut(ulong lsn, TableId table)
    {
        if (_leftOut.Contains(table))
            return true;
        if (_source.TableAt(lsn, table) is not { IsArchiveTier: true })
            return false;
        _leftOut.Add(table);
        return true;
    }

    private Dictionary<RowKey, ProjectedRow> TableFor(TableId table)
    {
        if (!_tables.TryGetValue(table, out var rows))
        {
            rows = [];
            _tables[table] = rows;
        }

        return rows;
    }
}
