namespace MelangeDB.Core;

/// <summary>
/// A read-only <see cref="IDbView"/> pinned at one committed LSN, for code that reads off the write
/// lock: a producer's whole call, a policy evaluated before its transaction opens. Every read sees
/// the state as of <see cref="Lsn"/> however many commits land meanwhile, and the view is safe to
/// read from several threads at once — each read and each step of a lazy enumeration runs under the
/// view's own lock, because the store's pinned view underneath belongs to one reader at a time.
/// <para>
/// Holding one costs the store whatever it keeps to answer for <see cref="Lsn"/>, so dispose it as
/// soon as the reads are done. A read after disposal throws, including a lazy enumeration started
/// before it: materialize rows before the view goes. Opened by <see cref="MelangeEngine.OpenReadView"/>.
/// </para>
/// </summary>
public sealed class PinnedReadView : IDbView, IDisposable
{
    private readonly IHotStoreReadView _view;
    private readonly TransactionDb _reads;
    private readonly Lock _gate = new();
    private bool _disposed;

    internal PinnedReadView(SchemaRegistry registry, IHotStoreReadView view, TableAccessGuard? guard)
    {
        _view = view;
        _reads = new TransactionDb(registry, view, new WriteSet(), null!, guard);
    }

    /// <summary>The LSN this view answers for: the hot store's applied LSN when it was opened.</summary>
    public ulong Lsn => _view.Lsn;

    public TRow Insert<TRow>(TRow row)
        where TRow : struct
        => throw ReadOnly();

    public void Update<TRow>(TRow row)
        where TRow : struct
        => throw ReadOnly();

    public bool Delete<TRow>(object primaryKey)
        where TRow : struct
        => throw ReadOnly();

    public TRow? Find<TRow>(object primaryKey)
        where TRow : struct
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _reads.Find<TRow>(primaryKey);
        }
    }

    public IEnumerable<TRow> Scan<TRow>()
        where TRow : struct
        => Serialized(() => _reads.Scan<TRow>());

    public IEnumerable<TRow> Filter<TRow>(string column, object value)
        where TRow : struct
        => Serialized(() => _reads.Filter<TRow>(column, value));

    public IEnumerable<TRow> FilterRange<TRow>(string column, object low, object high)
        where TRow : struct
        => Serialized(() => _reads.FilterRange<TRow>(column, low, high));

    public bool ContainsKey<TRow>(object primaryKey)
        where TRow : struct
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _reads.ContainsKey<TRow>(primaryKey);
        }
    }

    public bool Any<TRow>()
        where TRow : struct
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _reads.Any<TRow>();
        }
    }

    public long Count<TRow>()
        where TRow : struct
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _reads.Count<TRow>();
        }
    }

    public TRow? First<TRow>()
        where TRow : struct
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _reads.First<TRow>();
        }
    }

    /// <summary>Releases the pin. Idempotent; reads after this throw.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            _view.Dispose();
        }
    }

    /// <summary>
    /// A lazy enumeration whose every step runs under the gate: the store's iterator is never
    /// advanced by two threads at once, and a step after disposal throws rather than reading a
    /// released pin.
    /// </summary>
    private IEnumerable<TRow> Serialized<TRow>(Func<IEnumerable<TRow>> open)
        where TRow : struct
    {
        IEnumerator<TRow> rows;
        lock (_gate)
        {
            ThrowIfDisposed();
            rows = open().GetEnumerator();
        }

        try
        {
            while (true)
            {
                bool more;
                var current = default(TRow);
                lock (_gate)
                {
                    ThrowIfDisposed();
                    more = rows.MoveNext();
                    if (more)
                        current = rows.Current;
                }

                if (!more)
                    yield break;
                yield return current;
            }
        }
        finally
        {
            lock (_gate)
                rows.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(
                nameof(PinnedReadView),
                "This read view was released. A lazy query over it was read after the call that owned it returned; materialize the rows (ToList) before returning them.");
        }
    }

    private static InvalidOperationException ReadOnly() =>
        new("This view is a read of committed state; it cannot write. Mutate through a reducer.");
}
