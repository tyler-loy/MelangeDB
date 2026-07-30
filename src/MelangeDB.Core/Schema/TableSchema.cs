namespace MelangeDB.Core;

/// <summary>
/// The schema of one table. Constructed directly (the seam phase 02's generated registration
/// uses) or via <see cref="SchemaRegistry.FromTypes"/>'s reflection path.
/// </summary>
public sealed class TableSchema
{
    public TableSchema(
        Type rowType,
        string name,
        IReadOnlyList<ColumnSchema> columns,
        bool isPublic = false,
        StorageTier tier = StorageTier.Hot,
        Residency residency = Residency.Paged,
        Placement placement = Placement.Partitioned,
        string? shardBy = null,
        string? scheduled = null,
        RowCodec? codec = null)
    {
        ArgumentNullException.ThrowIfNull(rowType);
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0)
            throw new ArgumentException($"Table '{name}' has no columns.", nameof(columns));

        // A table declaring Scheduled holds timer rows: exactly one ScheduleAt column, implicitly
        // private, implicitly Local until clustering gives timer tables a placement.
        var scheduleAtColumns = columns.Count(c => c.Kind == ColumnKind.ScheduleAt);
        if (scheduled is not null)
        {
            if (scheduleAtColumns != 1)
                throw new NotSupportedException($"Table '{name}' declares Scheduled and must declare exactly one ScheduleAt column; found {scheduleAtColumns}.");
            isPublic = false;
            placement = Placement.Local;
        }
        else if (scheduleAtColumns > 0)
        {
            throw new NotSupportedException($"Table '{name}': a ScheduleAt column is only valid on a table declaring Scheduled.");
        }

        var primaryKeys = columns.Where(c => c.IsPrimaryKey).ToList();
        if (primaryKeys.Count != 1)
            throw new NotSupportedException($"Table '{name}' must declare exactly one [PrimaryKey] column; found {primaryKeys.Count}.");

        PrimaryKey = primaryKeys[0];
        if (!KeyCodec.IsKeyEncodable(PrimaryKey.Kind))
            throw new NotSupportedException($"Table '{name}': column '{PrimaryKey.Name}' of kind {PrimaryKey.Kind} cannot be a primary key.");

        foreach (var column in columns)
        {
            if (column.IsAutoInc && column.Kind is not (ColumnKind.Int64 or ColumnKind.UInt64))
                throw new NotSupportedException($"Table '{name}': [AutoInc] column '{column.Name}' must be long or ulong.");
            if ((column.IsIndexed || column.IsUnique) && !KeyCodec.IsKeyEncodable(column.Kind))
                throw new NotSupportedException($"Table '{name}': column '{column.Name}' of kind {column.Kind} cannot be indexed.");
        }

        if (codec is not null && codec.RowType != rowType)
            throw new ArgumentException($"Table '{name}': codec serializes {codec.RowType}, not {rowType}.", nameof(codec));

        RowType = rowType;
        Name = name;
        Id = TableId.FromName(name);
        Columns = columns;
        AutoIncColumns = columns.Where(c => c.IsAutoInc).ToArray();
        Indexes = columns
            .Where(c => (c.IsIndexed || c.IsUnique) && !c.IsPrimaryKey)
            .Select(c => new IndexSchema { Column = c.Name, Unique = c.IsUnique })
            .ToArray();
        IsPublic = isPublic;
        Tier = tier;
        Residency = residency;
        Placement = placement;
        ShardBy = shardBy;
        Scheduled = scheduled;
        Codec = codec;
    }

    public Type RowType { get; }

    public string Name { get; }

    public TableId Id { get; }

    /// <summary>Columns in declaration order — the order the row serializer writes them.</summary>
    public IReadOnlyList<ColumnSchema> Columns { get; }

    public ColumnSchema PrimaryKey { get; }

    public IReadOnlyList<ColumnSchema> AutoIncColumns { get; }

    /// <summary>Secondary indexes, the primary key excluded.</summary>
    public IReadOnlyList<IndexSchema> Indexes { get; }

    public bool IsPublic { get; }

    public StorageTier Tier { get; }

    public Residency Residency { get; }

    public Placement Placement { get; }

    public string? ShardBy { get; }

    public string? Scheduled { get; }

    /// <summary>
    /// The generated per-table serializer, or null on the reflection path. When present, the
    /// engine's typed paths and the hot store's index maintenance use it instead of
    /// <see cref="RowSerializer"/> and the boxed column accessors.
    /// </summary>
    public RowCodec? Codec { get; }

    /// <summary>Finds a column by name, or throws.</summary>
    public ColumnSchema Column(string name) =>
        Columns.FirstOrDefault(c => c.Name == name)
        ?? throw new ArgumentException($"Table '{Name}' has no column '{name}'.", nameof(name));
}
