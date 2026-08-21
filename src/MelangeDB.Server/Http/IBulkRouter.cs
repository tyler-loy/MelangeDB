using MelangeDB.Core;

namespace MelangeDB.Server;

/// <summary>
/// One engine's share of a bulk batch: the shard whose engine took the rows (null for a
/// node-local engine, which is not any shard's), the LSN of the single transaction it committed
/// them as, and how many rows that was.
/// </summary>
/// <remarks>
/// An LSN is meaningful only inside one log, which is why this is per engine rather than per
/// batch. Bulk's atomicity guarantee is stated the same way: one transaction <em>per engine</em>,
/// and a single-node deployment has exactly one engine.
/// </remarks>
public readonly record struct BulkResult(ulong? Shard, ulong Lsn, int Rows);

/// <summary>
/// The seam <c>{path}/bulk</c> writes through when this node is not the one holding the rows.
/// Absent on a single-node deployment, where the endpoint writes to the DI-registered engine
/// directly; supplied by the cluster package on a hub, where a batch has to be fanned out to the
/// shard engines that actually own its rows.
/// <para>
/// It exists so that a loader keeps posting one batch to one endpoint and never learns the
/// topology — the same hiding the gateway does for reducer calls and subscriptions. A per-shard
/// bulk endpoint would work too, and would put the deployment's sharding function into every tool
/// that loads data.
/// </para>
/// </summary>
public interface IBulkRouter
{
    /// <summary>
    /// Writes <paramref name="rows"/> to whichever engines own them, and reports what each one
    /// took. Throws <see cref="ArgumentException"/> for a batch this router will not route — an
    /// unknown table, or a destination shard that does not exist — <b>before</b> writing anything,
    /// so a refused batch is a batch that did not half-land.
    /// </summary>
    Task<IReadOnlyList<BulkResult>> RouteAsync(
        Identity caller, IReadOnlyList<BulkRow> rows, CancellationToken cancellationToken = default);
}
