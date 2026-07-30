using System.Text;

namespace MelangeDB;

/// <summary>
/// The stable 32-bit identifier of a table, derived deterministically from its name (FNV-1a) so it
/// never depends on registration order and survives restarts. Write-set row ops and log records are
/// keyed by table id, never by CLR type.
/// </summary>
public readonly record struct TableId(uint Value) : IComparable<TableId>
{
    /// <summary>Derives the id for a table name. Collisions are detected at schema registration.</summary>
    public static TableId FromName(string tableName)
    {
        ArgumentException.ThrowIfNullOrEmpty(tableName);
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;
        var hash = offsetBasis;
        foreach (var b in Encoding.UTF8.GetBytes(tableName))
        {
            hash ^= b;
            hash *= prime;
        }

        return new TableId(hash);
    }

    public int CompareTo(TableId other) => Value.CompareTo(other.Value);

    public override string ToString() => $"0x{Value:x8}";
}
