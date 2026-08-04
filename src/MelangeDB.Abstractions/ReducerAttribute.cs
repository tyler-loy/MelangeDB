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

    /// <summary>
    /// Fired once, on an engine that has never committed anything, before its scheduler starts —
    /// the seam an application uses to seed the state a fresh database must already hold, timer
    /// rows above all. Not client-callable.
    /// <para>
    /// <b>Which</b> engine is decided by the reducer's <see cref="ReducerSite"/>, exactly as for
    /// any other reducer: a shard-executed one fires on <em>every</em> per-shard engine as that
    /// shard is first opened (so a world's scheduled tables get their timers in each shard, which
    /// is the only way a lazily-created shard ever ticks), a hub-executed one fires on the hub's
    /// fresh engine, and in a deployment that is not clustered both fire on the single engine.
    /// </para>
    /// <para>
    /// The contract: each fire is its own transaction, so a reducer that throws leaves the others'
    /// writes in place — and a fresh engine whose <em>every</em> init reducer threw stays fresh, so
    /// the next start tries again. Seed idempotently and the retry costs nothing.
    /// </para>
    /// </summary>
    Init,
}

/// <summary>
/// Which node a reducer executes on in a cluster. Single-node deployments ignore it entirely.
/// </summary>
public enum ReducerSite
{
    /// <summary>
    /// Derived at compile time from the tables the reducer body touches: only <c>Global</c> and
    /// <c>Replicated</c> tables means <see cref="Hub"/>, anything else (or a body the analysis
    /// cannot see through, e.g. one passing <c>ctx</c> to a helper) means <see cref="Shard"/>.
    /// The default.
    /// </summary>
    Auto,

    /// <summary>Executes on the hub — the reducer touches only Global and Replicated tables.</summary>
    Hub,

    /// <summary>Executes on the shard node owning the caller's shard attachment.</summary>
    Shard,
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

    /// <summary>
    /// Where the reducer executes in a cluster; see <see cref="ReducerSite"/>. Leave at
    /// <see cref="ReducerSite.Auto"/> unless the compile-time analysis cannot see the body's table
    /// touches (it says so by routing to the shard, where a Global read fails with a placement
    /// error naming this property).
    /// </summary>
    public ReducerSite Site { get; set; }
}
