using System.Diagnostics;
using System.Reflection;
using MelangeDB.Core;

namespace MelangeDB.Cluster;

/// <summary>
/// Thrown when a node writes a shard it no longer holds a live lease for — the self-fencing half
/// of failure handling. A node that cannot reach the hub within <c>Cluster:FailureTimeoutMs</c>
/// must assume its shards were reassigned and stop writing them, because the hub assumed exactly
/// that about the node.
/// </summary>
public sealed class ShardFencedException(string message) : InvalidOperationException(message);

/// <summary>
/// Thrown when a transaction's write set spans shard keys — the one contract the developer must
/// uphold that MelangeDB cannot verify statically, so the debug check fails loudly instead of the
/// violation surfacing as mysterious latency under load.
/// </summary>
public sealed class ShardSpanException(string message) : InvalidOperationException(message);

/// <summary>
/// Thrown when a reducer writes a border-band copy — a row this node holds read-only because a
/// neighbouring shard owns it. Always enforced, never debug-only: a violated read-only invariant
/// is silent state divergence between the copy and its owner, which no test would ever see.
/// </summary>
public sealed class BorderReadOnlyException(string message) : InvalidOperationException(message);

/// <summary>Resolves whether the shard-span check runs, per <c>Cluster:ShardSpanCheck</c>.</summary>
public static class ShardSpanCheck
{
    public static bool IsEnabled(ShardSpanCheckMode mode) => mode switch
    {
        ShardSpanCheckMode.Always => true,
        ShardSpanCheckMode.Off => false,
        _ => EntryAssemblyIsDebugBuild(),
    };

    /// <summary>
    /// The <c>DebugOnly</c> probe: whether the entry assembly was compiled Debug (JIT tracking
    /// enabled) — the host's build configuration, not MelangeDB's.
    /// </summary>
    public static bool EntryAssemblyIsDebugBuild() =>
        Assembly.GetEntryAssembly()?.GetCustomAttribute<DebuggableAttribute>()?.IsJITTrackingEnabled ?? false;
}

/// <summary>The placement visibility rules each node role installs on its engines.</summary>
internal static class PlacementGuards
{
    /// <summary>The hub engine: Global, Replicated, and Local tables; Partitioned lives on shards.</summary>
    public static TableAccessGuard HubAccess() => static (table, access) =>
    {
        if (table.Placement == Placement.Partitioned)
        {
            throw new InvalidOperationException(
                $"Table '{table.Name}' is Placement.Partitioned and lives on shard nodes; the hub cannot {AccessWord(access)} it. " +
                "A hub-executed reducer may touch only Global, Replicated, and Local tables — mark the reducer " +
                "[Reducer(Site = ReducerSite.Shard)] or change the table's Placement.");
        }
    };

    /// <summary>
    /// A shard engine: Partitioned and Local tables plus read-only Replicated copies. A Global
    /// touch or a Replicated write is a placement mistake, and the error says the fix.
    /// </summary>
    public static TableAccessGuard ShardAccess(ShardKey shard, string nodeName) => (table, access) =>
    {
        switch (table.Placement)
        {
            case Placement.Global:
                throw new InvalidOperationException(
                    $"Table '{table.Name}' is Placement.Global and lives on the hub; this code is executing on shard node " +
                    $"'{nodeName}' ({shard}), which cannot {AccessWord(access)} it. Hub-execute the reducer (touch only " +
                    "Global/Replicated tables, or declare [Reducer(Site = ReducerSite.Hub)]), or make the table Replicated " +
                    "if shard-side reads are all it needs — row policies evaluated on shard nodes may only read Replicated, " +
                    "Partitioned, and Local tables (see docs/CLUSTERING.md).");
            case Placement.Replicated when access == TableAccess.Write:
                throw new InvalidOperationException(
                    $"Table '{table.Name}' is Placement.Replicated: every node holds a read-only copy and only the hub " +
                    "writes it. Route the write through a hub-executed reducer.");
        }
    };

    /// <summary>
    /// A shard node's node-local engine (the DI-registered one): it owns no shard, so only Local
    /// tables are accessible. Shard state lives in the per-shard engines.
    /// </summary>
    public static TableAccessGuard NodeLocalAccess(string nodeName) => (table, access) =>
    {
        if (table.Placement != Placement.Local)
        {
            throw new InvalidOperationException(
                $"Table '{table.Name}' is Placement.{table.Placement}, but this is shard node '{nodeName}''s node-local " +
                $"engine, which holds only Local tables. Partitioned tables live in the node's per-shard engines; Global " +
                "and Replicated tables are written on the hub.");
        }
    };

    private static string AccessWord(TableAccess access) => access == TableAccess.Read ? "read" : "write";
}

/// <summary>
/// The hub engine's commit-point guard: catches Partitioned writes arriving through paths the
/// access guard does not see (bulk ingestion). Internal applies are exempt by definition.
/// </summary>
internal sealed class HubCommitGuard : ICommitGuard
{
    private readonly SchemaRegistry _schema;

    public HubCommitGuard(SchemaRegistry schema) => _schema = schema;

    public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin)
    {
        if (origin == CommitOrigin.Internal)
            return;
        foreach (var op in writeSet)
        {
            if (_schema.TryGet(op.Table, out var table) && table.Placement == Placement.Partitioned)
            {
                throw new InvalidOperationException(
                    $"'{reducerName}' writes Partitioned table '{table.Name}' on the hub; Partitioned rows are written " +
                    "only by the shard node owning their shard.");
            }
        }
    }
}

/// <summary>
/// A shard engine's commit-point guard, checked in order of blast radius: the fencing lease
/// (a node that lost the hub must not write at all), frozen handoff rows (a mid-transfer player
/// is writable nowhere), placement (bulk-path Global/Replicated writes), and the shard-span
/// check. Internal applies (replication, handoff import) skip everything but run under the same
/// append path.
/// </summary>
internal sealed class ShardCommitGuard : ICommitGuard
{
    private readonly SchemaRegistry _schema;
    private readonly Func<IHotStore> _store;
    private readonly ShardKey _shard;
    private readonly IShardStrategy? _strategy;
    private readonly Func<bool> _spanCheckEnabled;
    private readonly Func<bool> _leaseValid;
    private readonly Func<IReadOnlySet<(TableId, RowKey)>> _frozenRows;
    private readonly Func<TableId, RowKey, ulong?> _borrowedOwner;

    public ShardCommitGuard(
        SchemaRegistry schema,
        Func<IHotStore> store,
        ShardKey shard,
        IShardStrategy? strategy,
        Func<bool> spanCheckEnabled,
        Func<bool> leaseValid,
        Func<IReadOnlySet<(TableId, RowKey)>> frozenRows,
        Func<TableId, RowKey, ulong?> borrowedOwner)
    {
        _schema = schema;
        _store = store;
        _shard = shard;
        _strategy = strategy;
        _spanCheckEnabled = spanCheckEnabled;
        _leaseValid = leaseValid;
        _frozenRows = frozenRows;
        _borrowedOwner = borrowedOwner;
    }

    public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin)
    {
        if (origin == CommitOrigin.Internal)
            return;

        if (!_leaseValid())
        {
            throw new ShardFencedException(
                $"This node's lease on {_shard} expired (Cluster:FailureTimeoutMs without a hub heartbeat); the hub may " +
                "have reassigned the shard, so writes are fenced until the node re-registers and holds a current " +
                "fencing token. '" + reducerName + "' was not committed.");
        }

        var frozen = _frozenRows();
        if (frozen.Count > 0)
        {
            foreach (var op in writeSet)
            {
                if (frozen.Contains((op.Table, op.Key)))
                {
                    var name = _schema.TryGet(op.Table, out var table) ? table.Name : op.Table.ToString();
                    throw new InvalidOperationException(
                        $"'{reducerName}' writes a row of '{name}' that is frozen mid-handoff; the player is being " +
                        "transferred to another shard and is writable nowhere until the transfer completes.");
                }
            }
        }

        // Always on, not debug-only: a border copy silently diverging from its owner is exactly
        // the failure no test surfaces, so the invariant is enforced at every commit. The lookup
        // is one dictionary probe per written row.
        foreach (var op in writeSet)
        {
            if (_borrowedOwner(op.Table, op.Key) is { } owner)
            {
                var name = _schema.TryGet(op.Table, out var table) ? table.Name : op.Table.ToString();
                throw new BorderReadOnlyException(
                    $"'{reducerName}' writes a row of '{name}' that is a read-only border-band copy owned by " +
                    $"shard:{owner}; only that shard's writer may mutate it. Interact with border entities by " +
                    "transferring ownership (the entity crosses and re-homes) or through a cross-shard saga — " +
                    "see docs/CLUSTERING.md.");
            }
        }

        ValidatePlacementAndSpan(reducerName, writeSet);
    }

    private void ValidatePlacementAndSpan(string reducerName, IReadOnlyList<RowOp> writeSet)
    {
        foreach (var op in writeSet)
        {
            if (!_schema.TryGet(op.Table, out var table))
                continue;
            switch (table.Placement)
            {
                case Placement.Global:
                    throw new InvalidOperationException(
                        $"'{reducerName}' writes Global table '{table.Name}' on a shard node; Global tables are written " +
                        "only on the hub.");
                case Placement.Replicated:
                    throw new InvalidOperationException(
                        $"'{reducerName}' writes Replicated table '{table.Name}' on a shard node; Replicated tables are " +
                        "written only by the hub.");
            }
        }

        if (_strategy is not null && _spanCheckEnabled())
            ShardSpan.Check(_schema, _store(), _strategy, _shard, reducerName, writeSet);
    }
}

/// <summary>
/// The shard-span computation shared by the clustered guard and the single-node debug guard:
/// resolve every Partitioned write's shard through the strategy and fail loudly on more than one.
/// </summary>
internal static class ShardSpan
{
    public static void Check(
        SchemaRegistry schema,
        IHotStore store,
        IShardStrategy strategy,
        ShardKey? executingShard,
        string reducerName,
        IReadOnlyList<RowOp> writeSet)
    {
        SortedSet<ulong>? shards = null;
        foreach (var op in writeSet)
        {
            if (!schema.TryGet(op.Table, out var table) || table.Placement != Placement.Partitioned)
                continue;
            if (RowBytesOf(store, op) is not { } bytes)
                continue;
            var row = table.ToRowRef(bytes);

            // On a shard the question is the strategy's MayCommit — which for a spatial strategy
            // admits the seam (an owned entity standing a band's depth across the line,
            // mid-handoff), while instancing keeps the strict same-shard contract. Single-node has
            // no executing shard, so the strict contract applies unchanged.
            if (executingShard is { } own && !strategy.MayCommit(own, op.Table, row))
                (shards ??= []).Add(strategy.ShardForRow(op.Table, row).Value);
            else if (executingShard is null)
                (shards ??= []).Add(strategy.ShardForRow(op.Table, row).Value);
        }

        if (shards is null)
            return;
        if (executingShard is { } executing)
            shards.Add(executing.Value);
        if (shards.Count > 1)
        {
            throw new ShardSpanException(
                $"Reducer '{reducerName}' writes rows resolving to more than one shard " +
                $"({string.Join(", ", shards.Select(static s => $"shard:{s}"))}" +
                $"{(executingShard is { } shard ? $", executing on {shard}" : string.Empty)}). Rows mutated in one " +
                "transaction must resolve to the same shard — spanning shards means a distributed commit on every " +
                "call. Give the rows the same shard key, or split the work so each transaction stays inside one shard " +
                "(see docs/CLUSTERING.md). This check runs per Cluster:ShardSpanCheck.");
        }
    }

    /// <summary>A delete carries no row, so its shard is judged from the store's pre-image (the guard runs pre-apply).</summary>
    private static ReadOnlyMemory<byte>? RowBytesOf(IHotStore store, in RowOp op)
    {
        if (op.Kind != RowOpKind.Delete)
            return op.Row;
        return store.TryGetRow(op.Table, op.Key, out var stored) ? stored : null;
    }
}

/// <summary>
/// The single-node shape of the shard-span check: a non-clustered deployment with a registered
/// <see cref="IShardStrategy"/> still gets the debug-mode diagnostic, so the placement contract is
/// tested in development long before a second node exists. No placement enforcement, no fencing —
/// single-node behavior is otherwise untouched.
/// </summary>
internal sealed class SingleNodeSpanGuard : ICommitGuard
{
    private readonly SchemaRegistry _schema;
    private readonly Func<IHotStore> _store;
    private readonly IShardStrategy _strategy;
    private readonly Func<bool> _enabled;

    public SingleNodeSpanGuard(SchemaRegistry schema, Func<IHotStore> store, IShardStrategy strategy, Func<bool> enabled)
    {
        _schema = schema;
        _store = store;
        _strategy = strategy;
        _enabled = enabled;
    }

    public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin)
    {
        if (origin != CommitOrigin.Internal && _enabled())
            ShardSpan.Check(_schema, _store(), _strategy, null, reducerName, writeSet);
    }
}
