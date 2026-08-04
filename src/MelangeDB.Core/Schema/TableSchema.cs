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
        // private, and always Local — which on a shard node's per-shard engine means per-shard,
        // since timers are rows in that engine's own log (docs/CLUSTERING.md). Another placement
        // is refused rather than silently overridden; it is compile error MELANGE0022, and this is
        // its runtime mirror for the reflection path. Partitioned reaches here indistinguishable
        // from the parameter's default, so only the compile-time check can catch that one.
        var scheduleAtColumns = columns.Count(c => c.Kind == ColumnKind.ScheduleAt);
        if (scheduled is not null)
        {
            if (scheduleAtColumns != 1)
                throw new NotSupportedException($"Table '{name}' declares Scheduled and must declare exactly one ScheduleAt column; found {scheduleAtColumns}.");
            if (placement is not (Placement.Local or Placement.Partitioned))
            {
                throw new NotSupportedException(
                    $"Table '{name}' declares Scheduled and Placement.{placement}. A scheduled table holds timer rows and " +
                    "is always Placement.Local: a shard node runs one engine per shard, so node-local timer rows are " +
                    "per-shard timer rows. Drop the Placement declaration.");
            }

            if (shardBy is not null)
            {
                throw new NotSupportedException(
                    $"Table '{name}' declares Scheduled and ShardBy = \"{shardBy}\". A scheduled table is Placement.Local " +
                    "and is never sharded by a column — it is already one independent timer set per shard. Drop ShardBy.");
            }

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

        // Handoff re-homes a row by rewriting its ShardBy column while the stored row key — the
        // encoded primary key — stays fixed. A primary-key shard column would silently diverge
        // from its key on the first transfer, so it is refused here and at compile time
        // (MELANGE0018).
        if (shardBy is not null)
        {
            var shardColumn = columns.FirstOrDefault(c => c.Name == shardBy)
                ?? throw new NotSupportedException($"Table '{name}': ShardBy names no column '{shardBy}'.");
            if (shardColumn.IsPrimaryKey)
            {
                throw new NotSupportedException(
                    $"Table '{name}': ShardBy = \"{shardBy}\" names the [PrimaryKey] column. The shard id must be its " +
                    "own column — handoff rewrites it while the row's key stays fixed.");
            }
        }

        PrimaryKey = primaryKeys[0];
        if (!SchemaKeyCodec.IsKeyEncodable(PrimaryKey.Kind))
            throw new NotSupportedException($"Table '{name}': column '{PrimaryKey.Name}' of kind {PrimaryKey.Kind} cannot be a primary key.");

        foreach (var column in columns)
        {
            if (column.IsAutoInc && column.Kind is not (ColumnKind.Int64 or ColumnKind.UInt64))
                throw new NotSupportedException($"Table '{name}': [AutoInc] column '{column.Name}' must be long or ulong.");
            if ((column.IsIndexed || column.IsUnique) && !SchemaKeyCodec.IsKeyEncodable(column.Kind))
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

    /// <summary>
    /// Wraps a serialized row of this table as a <see cref="RowRef"/> for the shard strategy.
    /// Column access deserializes lazily, once, on first read.
    /// </summary>
    public RowRef ToRowRef(ReadOnlyMemory<byte> bytes)
    {
        object? cached = null;
        return new RowRef(bytes, name =>
        {
            cached ??= RowSerializer.Deserialize(this, bytes);
            return Column(name).GetValue(cached);
        });
    }
}
