using System.Reflection;

namespace MelangeDB.Core;

/// <summary>
/// The set of tables an engine knows. Phase 01 builds schemas by reflection over <c>[Table]</c>
/// structs; phase 02 replaces that path with generated registration handing pre-built
/// <see cref="TableSchema"/> instances to the constructor.
/// </summary>
public sealed class SchemaRegistry
{
    private readonly Dictionary<Type, TableSchema> _byType;
    private readonly Dictionary<TableId, TableSchema> _byId;

    /// <summary>Registers pre-built schemas — the generated-registration seam.</summary>
    public SchemaRegistry(IEnumerable<TableSchema> tables)
    {
        ArgumentNullException.ThrowIfNull(tables);
        _byType = [];
        _byId = [];
        foreach (var table in tables.OrderBy(t => t.Name, StringComparer.Ordinal))
        {
            if (_byType.ContainsKey(table.RowType))
                throw new ArgumentException($"Type {table.RowType} is registered twice.");
            if (_byId.TryGetValue(table.Id, out var clash))
                throw new ArgumentException($"Table id collision between '{clash.Name}' and '{table.Name}'. Rename one table.");
            _byType.Add(table.RowType, table);
            _byId.Add(table.Id, table);
        }

        Tables = _byType.Values.OrderBy(t => t.Name, StringComparer.Ordinal).ToArray();
    }

    /// <summary>All tables, ordered by name for determinism.</summary>
    public IReadOnlyList<TableSchema> Tables { get; }

    /// <summary>Builds a registry by reflecting over <c>[Table]</c>-attributed structs.</summary>
    public static SchemaRegistry FromTypes(params Type[] tableTypes)
    {
        ArgumentNullException.ThrowIfNull(tableTypes);
        return new SchemaRegistry(tableTypes.Select(BuildSchema));
    }

    public TableSchema Get(Type rowType) =>
        _byType.TryGetValue(rowType, out var schema)
            ? schema
            : throw new ArgumentException($"Type {rowType} is not a registered table.", nameof(rowType));

    public TableSchema Get(TableId id) =>
        _byId.TryGetValue(id, out var schema)
            ? schema
            : throw new ArgumentException($"No table with id {id} is registered.", nameof(id));

    public bool TryGet(TableId id, out TableSchema schema) => _byId.TryGetValue(id, out schema!);

    private static TableSchema BuildSchema(Type type)
    {
        if (!type.IsValueType)
            throw new NotSupportedException($"Table type {type} must be a struct.");
        var attribute = type.GetCustomAttribute<TableAttribute>()
            ?? throw new NotSupportedException($"Type {type} carries no [Table] attribute.");

        var columns = new List<ColumnSchema>();
        var members = type
            .GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m is FieldInfo or PropertyInfo { CanRead: true, CanWrite: true })
            .OrderBy(m => m.MetadataToken);

        foreach (var member in members)
        {
            var memberType = member switch
            {
                FieldInfo f => f.FieldType,
                PropertyInfo p => p.PropertyType,
                _ => throw new InvalidOperationException(),
            };
            columns.Add(new ColumnSchema
            {
                Name = member.Name,
                ClrType = memberType,
                Kind = KindOf(type, member.Name, memberType),
                IsEnum = memberType.IsEnum,
                IsPrimaryKey = member.GetCustomAttribute<PrimaryKeyAttribute>() is not null,
                IsAutoInc = member.GetCustomAttribute<AutoIncAttribute>() is not null,
                IsUnique = member.GetCustomAttribute<UniqueAttribute>() is not null,
                IsIndexed = member.GetCustomAttribute<IndexAttribute>() is not null,
                GetValue = member switch
                {
                    FieldInfo f => f.GetValue,
                    PropertyInfo p => p.GetValue,
                    _ => throw new InvalidOperationException(),
                },
                SetValue = member switch
                {
                    FieldInfo f => f.SetValue,
                    PropertyInfo p => p.SetValue,
                    _ => throw new InvalidOperationException(),
                },
            });
        }

        return new TableSchema(
            type,
            attribute.Name ?? type.Name,
            columns,
            attribute.Public,
            attribute.Tier,
            attribute.Residency,
            attribute.Placement,
            attribute.ShardBy,
            attribute.Scheduled);
    }

    private static ColumnKind KindOf(Type table, string column, Type type)
    {
        if (type.IsEnum)
            type = Enum.GetUnderlyingType(type);
        if (type == typeof(bool)) return ColumnKind.Bool;
        if (type == typeof(sbyte)) return ColumnKind.Int8;
        if (type == typeof(byte)) return ColumnKind.UInt8;
        if (type == typeof(short)) return ColumnKind.Int16;
        if (type == typeof(ushort)) return ColumnKind.UInt16;
        if (type == typeof(int)) return ColumnKind.Int32;
        if (type == typeof(uint)) return ColumnKind.UInt32;
        if (type == typeof(long)) return ColumnKind.Int64;
        if (type == typeof(ulong)) return ColumnKind.UInt64;
        if (type == typeof(float)) return ColumnKind.Float32;
        if (type == typeof(double)) return ColumnKind.Float64;
        if (type == typeof(string)) return ColumnKind.String;
        if (type == typeof(byte[])) return ColumnKind.Bytes;
        if (type == typeof(Identity)) return ColumnKind.Identity;
        if (type == typeof(Timestamp)) return ColumnKind.Timestamp;
        throw new NotSupportedException(
            $"Table '{table.Name}': column '{column}' has unsupported type {type}. " +
            "Supported: integers, floats, bool, string, byte[], Identity, Timestamp, enums.");
    }
}
