namespace MelangeDB;

/// <summary>What triggers a reducer.</summary>
public enum ReducerKind
{
    /// <summary>Invoked explicitly, by a client call or in-process dispatch. The default.</summary>
    Standard,

    /// <summary>Fired when a client session begins. Not client-callable.</summary>
    ClientConnected,

    /// <summary>Fired when a client session ends. Not client-callable.</summary>
    ClientDisconnected,
}

/// <summary>
/// Marks a method as a reducer: invoked as a single transaction against the database. A reducer is
/// synchronous, performs no I/O, returns to commit, and throws to abort with nothing appended.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class ReducerAttribute : Attribute
{
    public ReducerAttribute()
    {
    }

    public ReducerAttribute(ReducerKind kind) => Kind = kind;

    public ReducerKind Kind { get; }

    /// <summary>The reducer's public name; defaults to the method name.</summary>
    public string? Name { get; set; }

    /// <summary>
    /// An <see cref="IReducerPolicy"/> type deciding whether a client may call this reducer,
    /// resolved from the call's DI scope — so it may read private tables. Applies to
    /// client-originated calls only; in-process dispatch is the host's own code. A client-callable
    /// reducer with no policy is listed by the unpoliced-reducer report
    /// (<c>Policies:UnpolicedReducerReport</c>).
    /// </summary>
    public Type? Policy { get; set; }
}
