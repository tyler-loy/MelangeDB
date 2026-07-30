namespace MelangeDB;

/// <summary>
/// The transactional view a reducer mutates through (<c>ctx.Db</c>). Reads resolve the overlay —
/// the transaction's own uncommitted write set — before the store, so a reducer reads its own
/// writes with no I/O. Phase 02's generated typed accessors sit on top of this surface.
/// </summary>
public interface IDbView
{
    /// <summary>
    /// Inserts a row, allocating any zero-valued <c>[AutoInc]</c> columns, and returns the row as
    /// inserted. Throws if a row with the same primary key already exists.
    /// </summary>
    TRow Insert<TRow>(TRow row)
        where TRow : struct;

    /// <summary>Replaces the row with the same primary key. Throws if no such row exists.</summary>
    void Update<TRow>(TRow row)
        where TRow : struct;

    /// <summary>Deletes the row with the given primary key; returns false if no such row exists.</summary>
    bool Delete<TRow>(object primaryKey)
        where TRow : struct;

    /// <summary>Finds a row by primary key, or null.</summary>
    TRow? Find<TRow>(object primaryKey)
        where TRow : struct;

    /// <summary>Enumerates all rows of a table in primary-key order.</summary>
    IEnumerable<TRow> Scan<TRow>()
        where TRow : struct;

    /// <summary>
    /// Enumerates rows whose indexed column equals <paramref name="value"/>. The column must carry
    /// <c>[Index]</c> or <c>[Unique]</c>.
    /// </summary>
    IEnumerable<TRow> Filter<TRow>(string column, object value)
        where TRow : struct;

    /// <summary>
    /// Enumerates rows whose indexed column falls within [<paramref name="low"/>,
    /// <paramref name="high"/>], both inclusive. The column must carry <c>[Index]</c> or
    /// <c>[Unique]</c>; range comparison follows the column's order-preserving key encoding.
    /// </summary>
    IEnumerable<TRow> FilterRange<TRow>(string column, object low, object high)
        where TRow : struct;

    /// <summary>
    /// Whether the table has any row. An existence check, not a scan: the engine's views answer
    /// from the store's row count and the overlay, so no row is materialized and nothing pages in.
    /// </summary>
    bool Any<TRow>()
        where TRow : struct
    {
        foreach (var _ in Scan<TRow>())
            return true;
        return false;
    }

    /// <summary>Counts the table's rows without materializing them; see <see cref="Any{TRow}"/>.</summary>
    long Count<TRow>()
        where TRow : struct
    {
        long count = 0;
        foreach (var _ in Scan<TRow>())
            count++;
        return count;
    }

    /// <summary>
    /// The first row in primary-key order, or null for an empty table. Materializes exactly one
    /// row, never the table.
    /// </summary>
    TRow? First<TRow>()
        where TRow : struct
    {
        foreach (var row in Scan<TRow>())
            return row;
        return null;
    }
}
