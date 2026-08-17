namespace MelangeDB.Core;

/// <summary>
/// The verdict of comparing a persisted shape entry against the booting code's schema. Exposed
/// publicly (with <see cref="ShapeCompatibility"/>) so an operator can ask "what would this
/// deploy's boot do" without booting — the dry-run half of hot-tier schema migration.
/// </summary>
public sealed class ShapeDiff
{
    /// <summary>Every table's shape is unchanged — the fast path; the boot writes nothing.</summary>
    public required bool IsIdentical { get; init; }

    /// <summary>
    /// Human-readable reasons the change is destructive — a removed table, a removed column, a
    /// changed kind, a moved key. Empty when the change is identical or additive.
    /// </summary>
    public required IReadOnlyList<string> DestructiveReasons { get; init; }

    /// <summary>
    /// Human-readable additive differences — added tables, added columns, reordered columns.
    /// A boot with any of these (and no destructive reasons) is a migration boot.
    /// </summary>
    public required IReadOnlyList<string> AdditiveChanges { get; init; }

    public bool IsDestructive => DestructiveReasons.Count > 0;

    public bool IsAdditive => !IsIdentical && !IsDestructive;
}

/// <summary>
/// Compares persisted table shapes against the booting code's schema, classifying the difference
/// per the phase 16 rule: a change is <em>additive</em> — mappable without loss — iff every
/// persisted column still exists with the same name and kind and every table's key column is
/// unchanged, wherever the columns moved to. Everything else is destructive and must refuse.
/// </summary>
public static class ShapeCompatibility
{
    public static ShapeDiff Compare(ShapeEntry persisted, SchemaRegistry schema)
    {
        ArgumentNullException.ThrowIfNull(persisted);
        ArgumentNullException.ThrowIfNull(schema);
        List<string> destructive = [];
        List<string> additive = [];

        foreach (var (name, shape) in persisted.Tables.OrderBy(static p => p.Key, StringComparer.Ordinal))
        {
            if (!schema.TryGetByName(name, out var table))
            {
                destructive.Add(
                    $"table '{name}' was removed; its rows are still in the log and dropping a table's " +
                    "declaration would silently abandon them");
                continue;
            }

            var current = TableShape.Of(table);
            if (current.Key != shape.Key)
            {
                destructive.Add(
                    $"table '{name}': the [PrimaryKey] moved from '{shape.Key}' to '{current.Key}'; every stored " +
                    "row key encodes the old key column, so moving the key rewrites the table's identity");
            }

            foreach (var column in shape.Columns)
            {
                var match = current.Columns.FirstOrDefault(c => c.Name == column.Name);
                if (match is null)
                {
                    destructive.Add(
                        $"table '{name}': column '{column.Name}' ({column.Kind}) was removed. If this is a rename, " +
                        "rename it back — a rename is indistinguishable from remove-plus-add and would zero the data");
                }
                else if (match.Kind != column.Kind)
                {
                    destructive.Add(
                        $"table '{name}': column '{column.Name}' changed kind {column.Kind} -> {match.Kind}; stored " +
                        "bytes do not convert between kinds");
                }
            }

            if (destructive.Count == 0 && !current.SameAs(shape))
            {
                var added = current.Columns.Where(c => shape.Columns.All(p => p.Name != c.Name)).ToList();
                additive.Add(added.Count > 0
                    ? $"table '{name}': added column(s) {string.Join(", ", added.Select(c => $"'{c.Name}' ({c.Kind})"))}"
                    : $"table '{name}': columns reordered");
            }
        }

        foreach (var table in schema.Tables)
        {
            if (!persisted.Tables.ContainsKey(table.Name))
                additive.Add($"new table '{table.Name}'");
        }

        return new ShapeDiff
        {
            IsIdentical = destructive.Count == 0 && additive.Count == 0,
            DestructiveReasons = destructive,
            AdditiveChanges = additive,
        };
    }
}

/// <summary>
/// The engine's boot-time shape resolution: the loaded (or adopted) history plus the transform
/// that re-encodes any old-shape row to the booting schema's shape. Recovery routes every
/// snapshot row and tail record through <see cref="TransformRecord"/> /
/// <see cref="TransformSnapshotRows"/>; both are pass-through when the governing shape already
/// matches, so the steady-state boot pays one LSN compare per record and nothing per row.
/// </summary>
internal sealed class ShapeResolution
{
    private readonly SchemaRegistry _schema;
    private readonly Dictionary<TableId, TableShape> _currentByTable;
    private readonly List<Dictionary<TableId, RowShapeMapper?>> _mappersByEntry;

    public ShapeResolution(ShapeHistory history, SchemaRegistry schema, string sidecarPath)
    {
        History = history;
        _schema = schema;
        SidecarPath = sidecarPath;
        _currentByTable = schema.Tables.ToDictionary(t => t.Id, TableShape.Of);
        _mappersByEntry = [.. history.Entries.Select(_ => new Dictionary<TableId, RowShapeMapper?>())];
    }

    public ShapeHistory History { get; }

    public string SidecarPath { get; }

    /// <summary>An additive difference was detected this boot; the marker record and new entry are pending.</summary>
    public bool MigrationPending { get; init; }

    /// <summary>The additive changes driving <see cref="MigrationPending"/>, for the migration log line.</summary>
    public IReadOnlyList<string> Changes { get; init; } = [];

    /// <summary>Re-encodes one record's write-set rows to the current shape; pass-through when nothing differs.</summary>
    public CommitRecord TransformRecord(CommitRecord record)
    {
        List<RowOp>? mapped = null;
        for (var i = 0; i < record.WriteSet.Count; i++)
        {
            var op = record.WriteSet[i];
            var mapper = op.Row.IsEmpty ? null : MapperFor(record.Lsn, op.Table);
            if (mapper is null)
            {
                mapped?.Add(op);
                continue;
            }

            if (mapped is null)
            {
                mapped = new List<RowOp>(record.WriteSet.Count);
                for (var j = 0; j < i; j++)
                    mapped.Add(record.WriteSet[j]);
            }

            mapped.Add(new RowOp(op.Kind, op.Table, op.Key, mapper.Map(op.Row.Span)));
        }

        return mapped is null ? record : new CommitRecord
        {
            Lsn = record.Lsn,
            FormatVersion = record.FormatVersion,
            Timestamp = record.Timestamp,
            Caller = record.Caller,
            ReducerName = record.ReducerName,
            Arguments = record.Arguments,
            WriteSet = mapped,
            Events = record.Events,
            SerializedLength = record.SerializedLength,
        };
    }

    /// <summary>Re-encodes snapshot rows captured at <paramref name="snapshotLsn"/>; pass-through per table when nothing differs.</summary>
    public IEnumerable<SnapshotRow> TransformSnapshotRows(ulong snapshotLsn, IEnumerable<SnapshotRow> rows)
    {
        foreach (var row in rows)
        {
            var mapper = MapperFor(snapshotLsn, row.Table);
            yield return mapper is null ? row : row with { Row = mapper.Map(row.Row.Span) };
        }
    }

    /// <summary>
    /// The mapper for a row of <paramref name="table"/> written at <paramref name="lsn"/>, or null
    /// for pass-through — the governing shape equals the current one, or the table is unknown to
    /// both the history and the schema (whatever wrote it, this boot cannot reinterpret it).
    /// </summary>
    private RowShapeMapper? MapperFor(ulong lsn, TableId table)
    {
        var index = IndexOfEntryAt(lsn);
        var cache = _mappersByEntry[index];
        if (cache.TryGetValue(table, out var cached))
            return cached;

        RowShapeMapper? mapper = null;
        if (_currentByTable.TryGetValue(table, out var current))
        {
            var entry = History.Entries[index];
            var source = entry.Tables
                .Where(p => TableId.FromName(p.Key) == table)
                .Select(p => p.Value)
                .FirstOrDefault();
            if (source is not null && !source.SameAs(current))
                mapper = new RowShapeMapper(source, current);
        }

        cache[table] = mapper;
        return mapper;
    }

    private int IndexOfEntryAt(ulong lsn)
    {
        for (var i = History.Entries.Count - 1; i >= 0; i--)
        {
            if (History.Entries[i].FromLsn <= lsn)
                return i;
        }

        return 0;
    }
}

/// <summary>
/// Boot-time shape governance: loads the <c>melange.shape</c> sidecar (adopting the booting
/// schema exactly once when none exists — the <c>melange.epoch</c> precedent), classifies the
/// difference against the code, refuses destructive changes with every reason named, and hands
/// recovery the transform for everything additive. The engine finishes a migration after
/// recovery: marker record, new entry, sidecar save, immediate snapshot — see
/// <c>MelangeEngine.CompleteShapeMigration</c>.
/// </summary>
internal static class ShapeGuard
{
    public static ShapeResolution Resolve(
        string logDirectory,
        SchemaRegistry schema,
        ulong baseLsn,
        ulong headLsn = 0,
        bool allowAdoption = true,
        Action<ulong>? onAdoptedOverExistingRecords = null)
    {
        var path = Path.Combine(logDirectory, ShapeHistory.FileName);
        var history = ShapeHistory.Load(path);
        if (history is null)
        {
            // Adoption assumes existing records were written by this schema — the only possible
            // reading, which is why the upgrade rule (documented in MIGRATION.md) is: the first
            // boot that creates this sidecar must not also change the schema.
            //
            // Over an *empty* directory that is a new world naming its first shape: uninteresting,
            // and silent. Over a directory that already holds records it is an assumption about
            // bytes a different binary wrote, sound only if the operator followed the rule — which
            // the engine cannot verify. Everything around this step is loud; this one used to be
            // silent, and the cost of the silence is a per-table mis-decode arriving an arbitrary
            // amount of time after a boot that looked perfectly clean (issue #99).
            if (headLsn > 0)
            {
                if (!allowAdoption)
                {
                    throw new SchemaShapeException(
                        $"'{path}' does not exist, so this boot would adopt the running code's schema as the meaning " +
                        $"of every record already in this directory (up to LSN {headLsn}) — and Schema:AllowAdoption " +
                        "is off. That reading is correct only if this deployment booted its previous schema once " +
                        "before changing it (the upgrade rule in docs/MIGRATION.md). If it did, set " +
                        "Schema:AllowAdoption to true for this one boot and turn it back off. If it did not, boot " +
                        "the previous schema first: adopting a changed schema over existing rows does not fail, it " +
                        "silently mis-reads them.",
                        ["no shape sidecar exists and this directory already holds records"]);
                }

                onAdoptedOverExistingRecords?.Invoke(headLsn);
            }

            history = ShapeHistory.Adopt(schema, fromLsn: 1);
            history.Save(path);
            return new ShapeResolution(history, schema, path);
        }

        if (history.Compact(baseLsn))
            history.Save(path);

        var diff = ShapeCompatibility.Compare(history.Current, schema);
        if (diff.IsDestructive)
        {
            throw new SchemaShapeException(
                "The schema is destructively different from the shapes this data directory was written " +
                "under, and destructive disagreement is never automatic:\n  - " +
                string.Join("\n  - ", diff.DestructiveReasons) +
                "\nRestore the declarations as recorded (see the shape sidecar at '" + path + "'), or " +
                "perform a deliberate manual migration; see docs/MIGRATION.md.",
                diff.DestructiveReasons);
        }

        return new ShapeResolution(history, schema, path)
        {
            MigrationPending = diff.IsAdditive,
            Changes = diff.AdditiveChanges,
        };
    }
}

/// <summary>The boot refusal for a destructive schema change; see <see cref="ShapeCompatibility"/>.</summary>
public sealed class SchemaShapeException(string message, IReadOnlyList<string> reasons) : Exception(message)
{
    /// <summary>One entry per destructive difference, as printed in the message.</summary>
    public IReadOnlyList<string> Reasons { get; } = reasons;
}
