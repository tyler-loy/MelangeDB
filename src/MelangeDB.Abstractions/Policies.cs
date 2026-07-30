namespace MelangeDB;

/// <summary>
/// The ambient state a policy decision is given: who is asking and a read-only view of committed
/// state. Policies run in-process with no restricted namespace, so <see cref="Db"/> may freely
/// read private tables — that is the point of policy objects over SQL-string filters.
/// </summary>
public sealed class PolicyContext
{
    public PolicyContext(Identity caller, bool isGuest, IDbView db)
    {
        ArgumentNullException.ThrowIfNull(db);
        Caller = caller;
        IsGuest = isGuest;
        Db = db;
    }

    /// <summary>The identity asking. Stable across reconnects and restarts.</summary>
    public Identity Caller { get; }

    /// <summary>
    /// Whether the caller's token carries the guest role claim (<c>Auth:GuestRole</c>). A guest is
    /// an ordinary identity that policies and caps may treat differently, nothing more.
    /// </summary>
    public bool IsGuest { get; }

    /// <summary>
    /// A read-only view of committed state — never a partially applied write set. During delta
    /// fan-out it observes the pre-transaction state the deltas were computed against; mutation
    /// methods throw.
    /// </summary>
    public IDbView Db { get; }
}

/// <summary>
/// Decides which rows of <typeparamref name="TRow"/> a caller sees. Resolved from DI, so a policy
/// may inject services and read private tables. <b>Multiple row policies on one table compose as a
/// UNION</b>: a row is visible if <em>any</em> policy admits it. (Rows union, columns intersect —
/// see <see cref="IColumnPolicy{TRow}"/>.) Enforced on the initial subscription set and on every
/// delta: a row becoming invisible emits a delete to that client; becoming visible emits an insert.
/// </summary>
/// <remarks>
/// Policies are resolved once per server and must be thread-safe. Evaluation runs on the commit
/// fan-out path under the engine's write lock — keep it cheap, and read only through
/// <see cref="PolicyContext.Db"/> or injected services.
/// </remarks>
public interface IRowPolicy<TRow>
    where TRow : struct
{
    /// <summary>Whether <paramref name="row"/> is visible to the caller in <paramref name="ctx"/>.</summary>
    bool IsVisibleTo(in TRow row, PolicyContext ctx);
}

/// <summary>
/// Decides which columns of <typeparamref name="TRow"/> a caller sees, per row. Resolved from DI.
/// <b>Column rules compose as an INTERSECTION</b>: a column is visible only if <em>every</em>
/// applicable rule admits it. (Rows union, columns intersect — see <see cref="IRowPolicy{TRow}"/>.)
/// For columns nobody should ever see, prefer <c>[ServerOnly]</c> — it is compile-time and free,
/// while a column policy is evaluated per row on the delta path.
/// </summary>
/// <remarks>
/// Policies are resolved once per server and must be thread-safe. Return cached
/// <see cref="ColumnMask"/> instances where possible; the mask is evaluated per row per caller.
/// </remarks>
public interface IColumnPolicy<TRow>
    where TRow : struct
{
    /// <summary>The columns of <paramref name="row"/> visible to the caller in <paramref name="ctx"/>.</summary>
    ColumnMask VisibleTo(in TRow row, PolicyContext ctx);
}

/// <summary>
/// Decides whether a caller may invoke a reducer, named by <c>[Reducer(Policy = typeof(...))]</c>
/// and resolved from the call's DI scope — so it may read private tables through
/// <see cref="PolicyContext.Db"/>. A denial is a single decision made before any transaction
/// opens; nothing is appended.
/// </summary>
public interface IReducerPolicy
{
    /// <summary>Whether the caller in <paramref name="ctx"/> may invoke <paramref name="reducer"/>.</summary>
    bool MayCall(string reducer, PolicyContext ctx);
}

/// <summary>
/// The set of columns visible to a particular caller for a particular row. The default value is
/// <see cref="None"/> — a policy that forgets to decide hides everything rather than leaking it.
/// </summary>
public readonly struct ColumnMask
{
    private const byte ModeNone = 0;
    private const byte ModeAllExcept = 1;
    private const byte ModeOnly = 2;

    private readonly byte _mode;
    private readonly string[]? _columns;

    private ColumnMask(byte mode, string[]? columns)
    {
        _mode = mode;
        _columns = columns;
    }

    /// <summary>Every column visible.</summary>
    public static ColumnMask All { get; } = new(ModeAllExcept, null);

    /// <summary>No column visible — the value of <c>default(ColumnMask)</c>.</summary>
    public static ColumnMask None => default;

    /// <summary>Only the named columns visible.</summary>
    public static ColumnMask Only(params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        return new ColumnMask(ModeOnly, columns);
    }

    /// <summary>This mask with the named columns additionally hidden.</summary>
    public ColumnMask Except(params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        return _mode switch
        {
            ModeNone => None,
            ModeAllExcept => new ColumnMask(ModeAllExcept, _columns is null ? columns : [.. _columns, .. columns]),
            _ => new ColumnMask(ModeOnly, (_columns ?? []).Where(c => !columns.Contains(c, StringComparer.Ordinal)).ToArray()),
        };
    }

    /// <summary>Whether this mask admits <paramref name="column"/>.</summary>
    public bool Admits(string column) => _mode switch
    {
        ModeAllExcept => _columns is null || !_columns.Contains(column, StringComparer.Ordinal),
        ModeOnly => _columns is not null && _columns.Contains(column, StringComparer.Ordinal),
        _ => false,
    };
}
