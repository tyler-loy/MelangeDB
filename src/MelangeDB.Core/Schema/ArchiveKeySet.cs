namespace MelangeDB.Core;

/// <summary>
/// The key-only hot table behind an archive table declared <c>KeepKeys = true</c>. The archive's
/// rows live only in Postgres, but its keys are kept here — one key per row, no payload, paged —
/// so a reducer can ask whether a key was ever archived (<c>Contains</c>) and an insert can refuse
/// a duplicate before commit. It is an ordinary hot table, so snapshots, paging, recovery, and
/// backups carry it with no machinery of its own; the engine maintains it from the archive's ops,
/// which is why nothing writes it directly.
/// </summary>
public static class ArchiveKeySet
{
    /// <summary>The reserved name prefix; no declared table may use it.</summary>
    public const string Prefix = "__keys_";

    /// <summary>The key set's table name for an archive table.</summary>
    public static string NameFor(string archiveTable) => Prefix + archiveTable;

    /// <summary>Whether <paramref name="table"/> names a key set, and of which archive table.</summary>
    public static bool IsKeySet(string table, out string archiveTable)
    {
        if (table.StartsWith(Prefix, StringComparison.Ordinal))
        {
            archiveTable = table[Prefix.Length..];
            return true;
        }

        archiveTable = string.Empty;
        return false;
    }

    /// <summary>The key set's schema for an archive table: its primary key column and nothing else.</summary>
    internal static TableSchema SchemaFor(TableSchema archive)
    {
        var rowType = typeof(KeyRow<>).MakeGenericType(archive.RowType);
        var field = rowType.GetField(nameof(KeyRow<int>.Key))!;
        var key = archive.PrimaryKey;
        ColumnSchema[] columns =
        [
            new ColumnSchema
            {
                Name = key.Name,
                ClrType = key.ClrType,
                Kind = key.Kind,
                IsEnum = key.IsEnum,
                IsPrimaryKey = true,
                GetValue = field.GetValue,
                SetValue = field.SetValue,
            },
        ];
        return new TableSchema(rowType, NameFor(archive.Name), columns, placement: archive.Placement);
    }

    /// <summary>The row type of a key set: one boxed key. Generic over the archive's row type so each key set's type is distinct.</summary>
    internal struct KeyRow<TArchive>
    {
        public object? Key;
    }
}
