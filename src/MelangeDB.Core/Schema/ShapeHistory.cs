using System.Text.Json;
using System.Text.Json.Serialization;

namespace MelangeDB.Core;

/// <summary>
/// One column of a persisted table shape: the name and wire kind that give row bytes their
/// meaning. Deliberately nothing else — indexes, residency, policies, and enum-ness are
/// projections or process policy, rebuilt or re-read from the booting code; the shape records
/// only what the *bytes* depend on.
/// </summary>
public sealed record ShapeColumn(string Name, ColumnKind Kind);

/// <summary>
/// The persisted shape of one table: its key column's name and its columns in declaration order —
/// the order row format v1 writes them. Two shapes are byte-compatible iff they are equal; a row
/// written under one decodes under another only through <see cref="RowShapeMapper"/>.
/// <para>
/// Also the table's <see cref="Tier"/>, which the bytes do not depend on but where the rows
/// <em>live</em> does: a tier change that crosses the hot store's boundary is a data-loss event,
/// so it has to be detectable at boot like any other change to what the directory holds. It takes
/// no part in <see cref="SameAs"/>, which stays the byte-compatibility question.
/// </para>
/// </summary>
public sealed class TableShape
{
    public required string Key { get; init; }

    public required IReadOnlyList<ShapeColumn> Columns { get; init; }

    /// <summary>
    /// The table's tier by its stable name (<see cref="ShapeTiers"/>), or null in a sidecar written
    /// before tiers were recorded — <see cref="ShapeHistory.RecordMissingTiers"/> fills it in.
    /// </summary>
    public string? Tier { get; init; }

    public bool SameAs(TableShape other) =>
        Key == other.Key && Columns.SequenceEqual(other.Columns);

    /// <summary>Captures a table's current shape from its schema.</summary>
    public static TableShape Of(TableSchema table) => new()
    {
        Key = table.PrimaryKey.Name,
        Columns = table.Columns.Select(c => new ShapeColumn(c.Name, c.Kind)).ToArray(),
        Tier = ShapeTiers.NameOf(table.Tier),
    };
}

/// <summary>
/// The sidecar's names for <see cref="StorageTier"/> values. Explicit rather than the enum's own
/// names on purpose: <c>Relational</c> and <c>HotWithRelationalBackplane</c> share a value, and
/// which of two aliased names <see cref="Enum.ToString()"/> returns is unspecified — a persisted
/// name has to be the same on every runtime that will ever read it.
/// </summary>
public static class ShapeTiers
{
    public const string Hot = "Hot";

    public const string HotWithRelationalBackplane = "HotWithRelationalBackplane";

    public const string Archive = "Archive";

    public static string NameOf(StorageTier tier) => tier switch
    {
        StorageTier.Hot => Hot,
        StorageTier.HotWithRelationalBackplane => HotWithRelationalBackplane,
        StorageTier.Archive => Archive,
        _ => throw new ArgumentOutOfRangeException(nameof(tier), tier, "No sidecar name for this tier."),
    };
}

/// <summary>One reign in the shape history: these table shapes govern records from this LSN on.</summary>
public sealed class ShapeEntry
{
    /// <summary>The first LSN whose records were written under these shapes.</summary>
    public required ulong FromLsn { get; init; }

    public required IReadOnlyDictionary<string, TableShape> Tables { get; init; }
}

/// <summary>
/// The shape history persisted in the <c>melange.shape</c> sidecar: which table shapes governed
/// which LSN ranges. Row format v1 is positional — a row is its columns' bytes in declaration
/// order, with no count and no names — so the bytes alone cannot say what they mean; this sidecar
/// is what says it. Every reader that decodes a stored row picks the entry governing that row's
/// LSN; the booting engine compares the newest entry against the code's schema to detect drift.
/// <para>
/// It is a <em>history</em>, not a single shape, because log records outlive deployments: the
/// tail above the snapshot, and any record a lagging applier still needs, may span shapes. An
/// entry dies only when truncation has removed every record it governed; see
/// <see cref="Compact"/>. A directory without the sidecar adopts the booting code's shape exactly
/// once, governing from LSN 1 — the <c>melange.epoch</c> adoption precedent, and the only sane
/// reading of records that predate the sidecar's existence.
/// </para>
/// </summary>
public sealed class ShapeHistory
{
    /// <summary>The sidecar's file name, beside the log.</summary>
    public const string FileName = "melange.shape";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly List<ShapeEntry> _entries;

    private ShapeHistory(List<ShapeEntry> entries)
    {
        if (entries.Count == 0)
            throw new ArgumentException("A shape history holds at least one entry.", nameof(entries));
        _entries = entries;
    }

    /// <summary>Entries in ascending <see cref="ShapeEntry.FromLsn"/> order.</summary>
    public IReadOnlyList<ShapeEntry> Entries => _entries;

    /// <summary>The newest entry — the shapes governing records written from now on.</summary>
    public ShapeEntry Current => _entries[^1];

    /// <summary>The entry governing the record at <paramref name="lsn"/>.</summary>
    public ShapeEntry At(ulong lsn)
    {
        for (var i = _entries.Count - 1; i >= 0; i--)
        {
            if (_entries[i].FromLsn <= lsn)
                return _entries[i];
        }

        // Below the oldest entry: the oldest entry governs. Reachable only for LSN 0 probes or a
        // hand-edited sidecar; adoption writes FromLsn 1 and appends only ever go forward.
        return _entries[0];
    }

    /// <summary>A single-entry history of the given schema's shapes, governing from <paramref name="fromLsn"/>.</summary>
    public static ShapeHistory Adopt(SchemaRegistry schema, ulong fromLsn) =>
        new([EntryOf(schema, fromLsn)]);

    /// <summary>Captures the schema's current shapes as an entry governing from <paramref name="fromLsn"/>.</summary>
    public static ShapeEntry EntryOf(SchemaRegistry schema, ulong fromLsn) => new()
    {
        FromLsn = fromLsn,
        Tables = schema.Tables.ToDictionary(t => t.Name, TableShape.Of),
    };

    /// <summary>
    /// Fills in the tier of every table in the current entry that has none — a sidecar written
    /// before tiers were recorded — from the booting schema, and returns how many were filled. In
    /// place rather than as a new reign, because the tier never changes what a record's bytes mean.
    /// It is the adoption reading again, for one field: correct when this boot did not also change
    /// a tier, which is the same upgrade rule that governs the sidecar itself.
    /// </summary>
    public int RecordMissingTiers(SchemaRegistry schema)
    {
        var current = Current;
        Dictionary<string, TableShape>? tables = null;
        var filled = 0;
        foreach (var (name, shape) in current.Tables)
        {
            if (shape.Tier is not null || !schema.TryGetByName(name, out var table))
                continue;
            tables ??= new Dictionary<string, TableShape>(current.Tables);
            tables[name] = new TableShape { Key = shape.Key, Columns = shape.Columns, Tier = ShapeTiers.NameOf(table.Tier) };
            filled++;
        }

        if (tables is not null)
            _entries[^1] = new ShapeEntry { FromLsn = current.FromLsn, Tables = tables };
        return filled;
    }

    /// <summary>Appends a new reign. Its <see cref="ShapeEntry.FromLsn"/> must be past the current one's.</summary>
    public void Append(ShapeEntry entry)
    {
        if (entry.FromLsn <= Current.FromLsn)
        {
            throw new InvalidOperationException(
                $"Shape entry from LSN {entry.FromLsn} does not follow the current entry's {Current.FromLsn}.");
        }

        _entries.Add(entry);
    }

    /// <summary>
    /// Drops entries no record can need anymore: an entry is dead when its successor's reign began
    /// at or below the truncation base — every record the dead entry governed has been truncated
    /// away, and no reader can hold a cursor below the base because everything that reads records
    /// (appliers, subscribers, the resume window) registers a truncation floor. The newest entry
    /// never dies.
    /// </summary>
    public bool Compact(ulong baseLsn)
    {
        var dropped = false;
        while (_entries.Count > 1 && _entries[1].FromLsn <= baseLsn + 1)
        {
            _entries.RemoveAt(0);
            dropped = true;
        }

        return dropped;
    }

    /// <summary>Loads the sidecar, or null when none exists. A corrupt sidecar is a loud failure —
    /// guessing what bytes mean is this file's whole job, so a guess about the file itself would
    /// be the vice it exists to prevent.</summary>
    public static ShapeHistory? Load(string path)
    {
        if (!File.Exists(path))
            return null;

        Persisted? persisted;
        try
        {
            persisted = JsonSerializer.Deserialize<Persisted>(File.ReadAllBytes(path), JsonOptions);
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                $"The shape sidecar '{path}' is not valid JSON. It records what this directory's row bytes mean; " +
                "restore it from backup rather than deleting it — a deleted sidecar re-adopts the booting code's " +
                "schema, which silently mis-reads any record written under an older shape.", exception);
        }

        if (persisted?.Entries is not { Count: > 0 } entries)
            throw new InvalidDataException($"The shape sidecar '{path}' holds no entries; restore it from backup.");

        var ordered = entries.OrderBy(e => e.FromLsn).ToList();
        return new ShapeHistory(ordered);
    }

    /// <summary>Writes the sidecar atomically — temp file, then rename over.</summary>
    public void Save(string path)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new Persisted { Entries = _entries }, JsonOptions);
        var temp = path + ".tmp";
        File.WriteAllBytes(temp, bytes);
        File.Move(temp, path, overwrite: true);
    }

    /// <summary>Serializes the history for a caller that stores it elsewhere (the backup archive).</summary>
    public byte[] ToBytes() =>
        JsonSerializer.SerializeToUtf8Bytes(new Persisted { Entries = _entries }, JsonOptions);

    private sealed class Persisted
    {
        public List<ShapeEntry>? Entries { get; init; }
    }
}
