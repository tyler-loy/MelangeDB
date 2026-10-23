namespace MelangeDB;

/// <summary>
/// Marks a method as a producer: a typed, read-only call a client makes for data. Where a reducer
/// writes and returns nothing, a producer writes nothing and returns a value — so it may be async
/// and do I/O (await Postgres through <c>IArchiveReader</c>, call another service), because nothing
/// it does is replayed and nothing it does holds the write lock.
/// <para>
/// The signature is <c>T Name(ProducerContext ctx, ...)</c>, <c>Task&lt;T&gt;</c>, or
/// <c>ValueTask&lt;T&gt;</c>. Parameters are encoded as reducer arguments are, and may have default
/// values. <c>T</c> is the client contract: scalars, strings, <c>byte[]</c>, <c>Identity</c>,
/// <c>Timestamp</c>, enums, lists and arrays, and records, classes, or structs of those — never a
/// <c>[Table]</c> struct, so a table's shape is published only by the author mapping it to a type.
/// </para>
/// <para>
/// <c>ctx.Db</c> is the hot store pinned at <c>ctx.Lsn</c> for the whole call; see
/// <see cref="ProducerContext"/>. A producer cannot write and cannot invoke a reducer.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class ProducerAttribute : Attribute
{
    /// <summary>The producer's public name; defaults to the method name. Shares one namespace with reducers.</summary>
    public string? Name { get; set; }

    /// <summary>
    /// An <see cref="IReducerPolicy"/> type deciding whether a client may call this producer — the
    /// same interface reducers use, asked with the producer's name. Resolved from the call's DI scope.
    /// A producer with no policy is listed by the unpoliced report beside reducers: an unpoliced
    /// producer is an open read of whatever it returns.
    /// </summary>
    public Type? Policy { get; set; }

    /// <summary>
    /// Where the producer runs in a cluster, by the reducer rules; see <see cref="ReducerSite"/>.
    /// Leave at <see cref="ReducerSite.Auto"/> unless the compile-time analysis cannot see the
    /// body's table touches.
    /// </summary>
    public ReducerSite Site { get; set; }
}
