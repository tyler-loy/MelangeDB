namespace MelangeDB.CodeGen;

/// <summary>
/// The client generator's view of one manifest — the parsed, validated form of
/// <c>melange-schema.json</c>. Mirrors what <see cref="ManifestEmitter"/> writes; both live in
/// this assembly precisely so the writer and the reader cannot drift apart unnoticed.
/// </summary>
internal sealed record ClientSchemaModel(
    string SchemaHash,
    string Module,
    EquatableArray<ClientEnumModel> Enums,
    EquatableArray<ClientTableModel> Tables,
    EquatableArray<ClientReducerModel> Reducers);

internal sealed record ClientEnumModel(
    string Name,
    WireKind Underlying,
    EquatableArray<EnumMemberModel> Members);

internal sealed record ClientTableModel(
    string TableName,
    string TypeName,
    EquatableArray<ClientColumnModel> Columns)
{
    public ClientColumnModel PrimaryKey => Columns.Items.First(static c => c.IsPrimaryKey);
}

internal sealed record ClientColumnModel(
    string Name,
    WireKind Kind,
    string? EnumName,
    bool IsPrimaryKey,
    bool IsAutoInc,
    bool IsUnique,
    bool IsIndexed)
{
    /// <summary>Whether the column gets a typed accessor: lookups and subscription helpers.</summary>
    public bool HasAccessor => IsPrimaryKey || IsUnique || IsIndexed;
}

internal sealed record ClientReducerModel(
    string Name,
    EquatableArray<ClientParameterModel> Parameters);

internal sealed record ClientParameterModel(
    string Name,
    WireKind Kind,
    bool IsArray,
    string? EnumName);
