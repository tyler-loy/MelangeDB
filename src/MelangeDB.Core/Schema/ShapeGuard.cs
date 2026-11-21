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

    /// <summary>
    /// The <c>[Unique]</c> columns whose existing rows the boot must check before it may enforce
    /// them: columns this boot newly declares unique, and — in a sidecar that predates uniqueness
    /// recording — every unique column, once. Not itself a verdict: the shapes cannot say whether
    /// the rows hold duplicates, so the engine checks after recovery and refuses there. See
    /// <c>UniqueConstraintCheck</c>.
    /// </summary>
    public IReadOnlyList<ShapeUniqueColumn> UniqueToCheck { get; init; } = [];

    public bool IsDestructive => DestructiveReasons.Count > 0;

    public bool IsAdditive => !IsIdentical && !IsDestructive;
}

/// <summary>
/// A <c>[Unique]</c> column whose existing rows must be checked at boot. <see cref="ColumnIsNew"/>
/// is true when the column itself is added by the same boot: every existing row then holds its
/// default, which is a duplicate the moment two rows exist — worth saying in the refusal.
/// </summary>
public sealed record ShapeUniqueColumn(string Table, string Column, bool ColumnIsNew);

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
        List<ShapeUniqueColumn> toCheck = [];

        foreach (var (name, shape) in persisted.Tables.OrderBy(static p => p.Key, StringComparer.Ordinal))
        {
            if (!schema.TryGetByName(name, out var table) && ArchiveKeySet.IsKeySet(name, out var keptFor))
            {
                // A key set is derived from the archive's ops, never the only copy of anything:
                // dropping it loses nothing but the ability to ask.
                additive.Add($"table '{keptFor}': its key set ('{name}') is no longer kept");
                continue;
            }

            if (table is null)
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

            CompareTier(name, shape.Tier, current.Tier!, destructive, additive);
            CompareUnique(name, shape, current, additive, toCheck);
        }

        foreach (var table in schema.Tables)
        {
            if (persisted.Tables.ContainsKey(table.Name))
                continue;
            if (ArchiveKeySet.IsKeySet(table.Name, out var archive) && persisted.Tables.ContainsKey(archive))
            {
                destructive.Add(
                    $"table '{archive}' now declares KeepKeys, but it already existed: the keys it has archived so far live " +
                    "only in Postgres, so its key set would start incomplete and Contains would answer false for them. " +
                    "Declare KeepKeys on a new archive table and move to it, or keep this one without");
                continue;
            }

            // A new table has no rows, so its unique columns have nothing to check.
            additive.Add($"new table '{table.Name}'");
        }

        return new ShapeDiff
        {
            IsIdentical = destructive.Count == 0 && additive.Count == 0,
            DestructiveReasons = destructive,
            AdditiveChanges = additive,
            UniqueToCheck = toCheck,
        };
    }

    /// <summary>
    /// A tier change is classified by whether the rows stay in the hot store across it. Between
    /// <c>Hot</c> and <c>HotWithRelationalBackplane</c> they do — only the Postgres copy appears or
    /// stops being maintained — so the change is additive and recorded as a new reign. A change
    /// into or out of <c>Archive</c> moves rows across the hot store's boundary and is refused,
    /// with the supported path named; a recorded tier this build does not know was written by a
    /// newer one and is refused too. A recorded tier of null predates tier recording and compares
    /// as unchanged.
    /// </summary>
    private static void CompareTier(string name, string? recorded, string current, List<string> destructive, List<string> additive)
    {
        if (recorded is null || recorded == current)
            return;

        if (IsHotResident(recorded) && IsHotResident(current))
        {
            additive.Add(current == ShapeTiers.HotWithRelationalBackplane
                ? $"table '{name}': tier {recorded} -> {current} (Postgres copies its existing rows on its next start)"
                : $"table '{name}': tier {recorded} -> {current} (its Postgres table is no longer maintained and is left as is)");
            return;
        }

        if (current == ShapeTiers.Archive && IsHotResident(recorded))
        {
            destructive.Add(
                $"table '{name}': tier {recorded} -> Archive would drop its rows from the hot store, and Postgres is not " +
                "guaranteed to hold all of them. Declare a new Archive table instead and move rows into it with a reducer " +
                "(Insert into the archive, Delete from this table, in batches) — the move is one write set, so it is atomic");
            return;
        }

        if (recorded == ShapeTiers.Archive && IsHotResident(current))
        {
            destructive.Add(
                $"table '{name}': tier Archive -> {current} would present an empty hot table as the whole truth: an archive " +
                "table's rows live only in Postgres. Keep it Archive; to bring rows back, read them outside a reducer and " +
                "insert them into a hot table through a reducer");
            return;
        }

        destructive.Add(
            $"table '{name}': tier {recorded} -> {current} is not a supported change; the recorded tier " +
            "decides where this table's rows live, and moving them across the hot store's boundary would lose them");
    }

    /// <summary>
    /// A uniqueness change is classified as additive — a recorded reign, like a tier change between
    /// hot tiers — because no stored byte changes; but a column that <em>becomes</em> unique is also
    /// a claim about rows that already exist, so it is queued for the engine to check after
    /// recovery, where a duplicate refuses the boot. Dropping <c>[Unique]</c> only relaxes a claim
    /// and needs no check. A recorded set of null predates uniqueness recording: nothing is reported
    /// as changed, and every unique column is queued, because earlier builds never checked a
    /// <c>[Unique]</c> added over existing rows (issue #192) and this is the one boot that can.
    /// </summary>
    private static void CompareUnique(
        string name, TableShape recorded, TableShape current, List<string> additive, List<ShapeUniqueColumn> toCheck)
    {
        var now = current.Unique ?? [];
        bool IsNew(string column) => recorded.Columns.All(c => c.Name != column);

        if (recorded.Unique is null)
        {
            foreach (var column in now)
                toCheck.Add(new ShapeUniqueColumn(name, column, IsNew(column)));
            return;
        }

        foreach (var column in now.Except(recorded.Unique, StringComparer.Ordinal))
        {
            additive.Add($"table '{name}': column '{column}' is now [Unique] (its existing rows are checked at boot)");
            toCheck.Add(new ShapeUniqueColumn(name, column, IsNew(column)));
        }

        foreach (var column in recorded.Unique.Except(now, StringComparer.Ordinal))
        {
            additive.Add(current.Columns.Any(c => c.Name == column)
                ? $"table '{name}': column '{column}' is no longer [Unique]"
                : $"table '{name}': [Unique] column '{column}' was removed");
        }
    }

    private static bool IsHotResident(string tier) =>
        tier is ShapeTiers.Hot or ShapeTiers.HotWithRelationalBackplane;
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

    /// <summary>
    /// The reigns and their mappers as one snapshot, replaced wholesale rather than mutated. Both
    /// halves must agree: an index derived from one list and used against the other is the bug
    /// this shape is here to make unrepresentable.
    /// </summary>
    private Reigns _reigns;

    public ShapeResolution(ShapeHistory history, SchemaRegistry schema, string sidecarPath)
    {
        History = history;
        _schema = schema;
        SidecarPath = sidecarPath;
        _currentByTable = schema.Tables.ToDictionary(t => t.Id, TableShape.Of);
        _reigns = BuildReigns();
    }

    public ShapeHistory History { get; }

    public string SidecarPath { get; }

    /// <summary>An additive difference was detected this boot; the marker record and new entry are pending.</summary>
    public bool MigrationPending { get; init; }

    /// <summary>The additive changes driving <see cref="MigrationPending"/>, for the migration log line.</summary>
    public IReadOnlyList<string> Changes { get; init; } = [];

    /// <summary>
    /// The unique columns the engine must check against the recovered rows before this boot may
    /// complete; a duplicate refuses it. See <see cref="ShapeDiff.UniqueToCheck"/>.
    /// </summary>
    public IReadOnlyList<ShapeUniqueColumn> UniqueToCheck { get; init; } = [];

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
    /// Rebuilds the snapshot after a reign was appended to the history. Both halves are replaced
    /// together and never mutated in place, because an index taken from the entries and used
    /// against the mappers must always mean the same reign.
    /// </summary>
    public void NoteEntryAppended() => Volatile.Write(ref _reigns, BuildReigns());

    /// <summary>
    /// The mapper for a row of <paramref name="table"/> written at <paramref name="lsn"/>, or null
    /// for pass-through — the governing shape equals the current one, or the table is unknown to
    /// both the history and the schema (whatever wrote it, this boot cannot reinterpret it).
    /// A pure read of an immutable snapshot, which is what makes it safe from the several readers
    /// that reach it concurrently.
    /// </summary>
    private RowShapeMapper? MapperFor(ulong lsn, TableId table)
    {
        var reigns = Volatile.Read(ref _reigns);
        var mappers = reigns.Mappers[IndexOfEntryAt(reigns.Entries, lsn)];
        return mappers.GetValueOrDefault(table);
    }

    /// <summary>
    /// Every reign's mappers, computed once. Only tables that actually changed shape get an entry:
    /// the common case is a table whose shape never moved, and a missing entry is the same
    /// pass-through answer as a null one.
    /// </summary>
    private Reigns BuildReigns()
    {
        var entries = History.Entries.ToArray();
        var mappers = new IReadOnlyDictionary<TableId, RowShapeMapper?>[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            var byTable = new Dictionary<TableId, RowShapeMapper?>();
            foreach (var (name, source) in entries[i].Tables)
            {
                var id = TableId.FromName(name);
                if (_currentByTable.TryGetValue(id, out var current) && !source.SameAs(current))
                    byTable[id] = new RowShapeMapper(source, current);
            }

            mappers[i] = byTable;
        }

        return new Reigns(entries, mappers);
    }

    private static int IndexOfEntryAt(IReadOnlyList<ShapeEntry> entries, ulong lsn)
    {
        for (var i = entries.Count - 1; i >= 0; i--)
        {
            if (entries[i].FromLsn <= lsn)
                return i;
        }

        return 0;
    }

    private sealed record Reigns(
        IReadOnlyList<ShapeEntry> Entries,
        IReadOnlyList<IReadOnlyDictionary<TableId, RowShapeMapper?>> Mappers);
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
        Action<ulong>? onAdoptedOverExistingRecords = null,
        Action<int>? onTiersRecorded = null,
        bool readOnly = false)
    {
        var path = Path.Combine(logDirectory, ShapeHistory.FileName);
        var history = ShapeHistory.Load(path);
        if (history is null && readOnly)
        {
            // A reader of someone else's log (a follower) never names the directory's first shape:
            // that is the owner's boot to do, and it has not happened yet.
            throw new ShapeNotRecordedException($"'{path}' does not exist yet; the log's owner writes it at boot.");
        }

        if (history is null)
        {
            // Adoption assumes existing records were written by this schema — the only possible
            // reading, which is why the upgrade rule (documented in MIGRATION.md) is: the first
            // boot that creates this sidecar must not also change the schema.
            //
            // Over an empty directory that is a new world naming its first shape, and silent.
            // Over one that already holds records it is an assumption about bytes a different
            // binary wrote, sound only if the operator followed the rule — which the engine cannot
            // verify, so it says so instead.
            if (headLsn > 0)
            {
                if (!allowAdoption)
                {
                    throw new SchemaShapeException(
                        $"'{path}' does not exist, so this boot would adopt the running code's schema as the meaning " +
                        $"of every record already in this directory (up to LSN {headLsn}). That reading is correct " +
                        "only if this deployment booted its previous schema once before changing it — the upgrade " +
                        "rule in docs/MIGRATION.md — and nothing in the directory can prove either way, so the boot " +
                        "refuses rather than assume. Two branches:\n" +
                        "  - The schema did NOT change in this deploy: set Schema:AllowAdoption to true, boot once " +
                        "to write the sidecar, and turn it back off.\n" +
                        "  - The schema DID change in this deploy: boot the previous schema first, then change it. " +
                        "Adopting a changed schema over existing rows does not fail — it silently mis-reads them.\n" +
                        "If the sidecar was deleted or lost rather than never written, restore it from backup " +
                        "instead: it records what this directory's row bytes mean.",
                        ["no shape sidecar exists and this directory already holds records"]);
                }

                onAdoptedOverExistingRecords?.Invoke(headLsn);
            }

            history = ShapeHistory.Adopt(schema, fromLsn: 1);
            history.Save(path);

            // Adoption records the code's uniqueness as already holding. Over existing records that
            // is the same unprovable assumption as the shapes themselves, but unlike them it can be
            // proved: check every unique column once, as an old sidecar's are.
            return new ShapeResolution(history, schema, path)
            {
                UniqueToCheck = headLsn == 0
                    ? []
                    : schema.Tables
                        .SelectMany(t => TableShape.UniqueOf(t).Select(c => new ShapeUniqueColumn(t.Name, c, ColumnIsNew: false)))
                        .ToArray(),
            };
        }

        var compacted = history.Compact(baseLsn);
        var tiersRecorded = history.RecordMissingTiers(schema);
        if ((compacted || tiersRecorded > 0) && !readOnly)
            history.Save(path);
        if (tiersRecorded > 0)
            onTiersRecorded?.Invoke(tiersRecorded);

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
            UniqueToCheck = diff.UniqueToCheck,
        };
    }
}

/// <summary>A read-only shape resolution over a directory whose owner has not recorded a shape yet.</summary>
internal sealed class ShapeNotRecordedException(string message) : InvalidOperationException(message);

/// <summary>The boot refusal for a destructive schema change; see <see cref="ShapeCompatibility"/>.</summary>
public sealed class SchemaShapeException(string message, IReadOnlyList<string> reasons) : Exception(message)
{
    /// <summary>One entry per destructive difference, as printed in the message.</summary>
    public IReadOnlyList<string> Reasons { get; } = reasons;
}
