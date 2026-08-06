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
/// How a reducer body is isolated from other transactions on the same engine.
/// <para>
/// <b>The rule that decides which one you want:</b> snapshot isolation is safe for
/// <em>recompute-from-scratch</em> and unsafe for <em>read-modify-write</em>. A body that reads
/// state, computes a value from it, and writes that value is safe — if the state moved underneath,
/// two concurrent runs each write a defensible answer and the last one wins. A body that reads a
/// value, adds a delta, and writes the sum is not: two runs read the same number and one increment
/// is lost, silently and permanently.
/// </para>
/// <para>
/// Both shapes routinely live in the same reducer, which is why this is opt-in per reducer and
/// never inferred. The compiler cannot tell a recompute from an increment; the module author can.
/// </para>
/// </summary>
public enum Isolation
{
    /// <summary>
    /// The engine's write lock is held across the whole transaction — body, guards, append, and
    /// commit. One global lock around the whole body <em>is</em> serializable, and this is the
    /// default. Time spent in the body is global write latency.
    /// </summary>
    Serialized,

    /// <summary>
    /// The body runs outside the write lock against a read view pinned at one LSN; only reconcile,
    /// the commit guards, and the append serialize. A sweep that spends 200 ms reading and 0.2 ms
    /// writing stops charging the other 199.8 ms to every writer on the engine.
    /// <para>
    /// What you give up: the body's reads are <b>advisory</b>. There is no read-set validation and
    /// no retry — the declaration is the contract. Rows read may have changed by the time the write
    /// set lands, and the write set is reconciled against committed state (an update of a row since
    /// deleted becomes an insert, a delete of a missing row drops) so it applies cleanly. Reconcile
    /// fixes op <em>shape</em>, never op <em>value</em>: it cannot rescue a lost increment.
    /// </para>
    /// <para>
    /// Read-your-writes inside the body is unaffected — the write-set overlay is transaction-local
    /// and has nothing to do with which store view the reads resolve against.
    /// </para>
    /// </summary>
    Snapshot,
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

    /// <summary>
    /// How this reducer's body is isolated; see <see cref="MelangeDB.Isolation"/>. Leave at
    /// <see cref="Isolation.Serialized"/> unless the body is a read-heavy sweep whose reads can be
    /// advisory — and read the eligibility rule before setting it, because the failure mode of
    /// getting it wrong is lost writes with no error anywhere.
    /// </summary>
    public Isolation Isolation { get; set; }
}
