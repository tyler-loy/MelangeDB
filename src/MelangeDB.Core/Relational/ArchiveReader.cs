namespace MelangeDB.Core;

/// <summary>
/// One page of an archive table's rows, filtered on one indexed column (or the key) and ordered by
/// primary key. Keyset-paginated: pass the previous page's <see cref="ArchivePage{TRow}.Next"/> as
/// <see cref="After"/>. Values are the columns' own CLR types (an <see cref="Identity"/> for an
/// Identity column, a <see cref="Timestamp"/> for a Timestamp one).
/// </summary>
public sealed record ArchiveQuery
{
    /// <summary>The filtered column: indexed, unique, or the primary key. Null reads the whole table.</summary>
    public string? Column { get; init; }

    /// <summary><see cref="Column"/> equals this value. Exclusive with <see cref="From"/>/<see cref="To"/>.</summary>
    public object? Equal { get; init; }

    /// <summary>Inclusive lower bound on <see cref="Column"/>; with <see cref="To"/>, a range.</summary>
    public object? From { get; init; }

    /// <summary>Inclusive upper bound on <see cref="Column"/>.</summary>
    public object? To { get; init; }

    /// <summary>Order by primary key descending — newest first, for AutoInc keys.</summary>
    public bool Descending { get; init; }

    /// <summary>The primary key of the last row already read; the page starts after it.</summary>
    public object? After { get; init; }

    /// <summary>Rows per page, at most <see cref="MaxLimit"/>.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>The largest page a reader returns.</summary>
    public const int MaxLimit = 1000;
}

/// <summary>A page of archive rows.</summary>
/// <param name="Rows">The rows, in primary-key order.</param>
/// <param name="Next">The cursor for the next page — the last row's primary key — or null on the last page.</param>
/// <param name="Stale">
/// True when the page reflects Postgres at the applier's checkpoint only: the unapplied log tail
/// was too long to overlay (Postgres has been down a while), or the projection is mid-reconcile.
/// False means rows committed but not yet applied are included.
/// </param>
public sealed record ArchivePage<TRow>(IReadOnlyList<TRow> Rows, object? Next, bool Stale)
    where TRow : struct;

/// <summary>
/// Reads archive tables outside a reducer — a reducer body does no I/O. Registered by
/// <c>AddPostgres</c>. Each page is Postgres at the applier's checkpoint with the unapplied log tail
/// overlaid, so a row that just left the hot store for the archive is never briefly invisible to a
/// reader that looks in both.
/// </summary>
public interface IArchiveReader
{
    /// <summary>Reads one page of <typeparamref name="TRow"/>, which must be an archive table.</summary>
    Task<ArchivePage<TRow>> ReadAsync<TRow>(ArchiveQuery query, CancellationToken cancellationToken = default)
        where TRow : struct;
}
