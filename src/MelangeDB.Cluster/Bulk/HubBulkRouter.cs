using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The hub's bulk fan-out: one posted batch, grouped by the shard that owns each row and forwarded
/// to that shard's node. <c>Global</c>, <c>Replicated</c> and <c>Local</c> rows stay on the hub's
/// own engine, which is where they belong.
///
/// <para>This exists because bulk cannot be refused the way ad-hoc SQL on a <c>Partitioned</c>
/// table could be (#114). Refusing would leave a clustered deployment with no way to seed a world
/// except routed reducer calls, forfeiting the 44x advantage bulk has over per-row transactions
/// that phase 07 measured — a real cost paid on every bake.</para>
/// </summary>
internal sealed class HubBulkRouter : IBulkRouter
{
    private readonly HubRuntime _hub;
    private readonly IShardStrategy _strategy;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    public HubBulkRouter(IServiceProvider services, IOptionsMonitor<MelangeDbOptions> options)
    {
        _hub = services.GetRequiredService<HubRuntime>();
        _options = options;
        _strategy = services.GetService<IShardStrategy>()
            ?? throw new InvalidOperationException(
                "No IShardStrategy is registered; a hub cannot route bulk rows to shards without one.");
    }

    public async Task<IReadOnlyList<BulkResult>> RouteAsync(
        Identity caller, IReadOnlyList<BulkRow> rows, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var schema = _hub.Engine.Schema;
        List<BulkRow> local = [];
        Dictionary<ShardKey, List<BulkRowDto>> groups = [];

        foreach (var row in rows)
        {
            if (!schema.TryGetByName(row.Table, out var table))
            {
                // Let the local engine raise it: one message for an unknown table, from the one
                // place that owns the wording, rather than a second one that drifts from it.
                local.Add(row);
                continue;
            }

            if (table.Placement != Placement.Partitioned)
            {
                local.Add(row);
                continue;
            }

            var (bytes, supplied) = Preimage(table, row);
            var shard = _strategy.ShardForRow(table.Id, table.ToRowRef(bytes));
            (groups.TryGetValue(shard, out var group) ? group : groups[shard] = [])
                .Add(new BulkRowDto(table.Name, bytes, supplied));
        }

        await PrepareDestinationsAsync(groups.Keys, cancellationToken).ConfigureAwait(false);

        List<BulkResult> results = [];
        if (local.Count > 0)
        {
            var record = _hub.Engine.BulkInsert(caller, local);
            results.Add(new BulkResult(Shard: null, record?.Lsn ?? 0, local.Count));
        }

        // Sequential, and shard-ordered so the answer reads the same way twice. The round-trip
        // count is the number of destination shards rather than the number of rows — a group is
        // one large batch, which is the whole point of bulk — and going one at a time keeps a
        // partial failure legible: the results array says exactly what landed. Re-posting the
        // batch is sound, because rows are upserts and [AutoInc] is originator-prefixed.
        foreach (var (shard, group) in groups.OrderBy(static g => g.Key.Value))
        {
            var reply = await _hub.BulkToShardAsync(shard, caller, [.. group], cancellationToken).ConfigureAwait(false);
            results.Add(new BulkResult(shard.Value, reply.Lsn, reply.Rows));
        }

        return results;
    }

    /// <summary>
    /// Encodes the row the way the engine would, and names the columns the caller supplied.
    ///
    /// <para><b>The encode is not an optimization and skipping it is not a shortcut.</b>
    /// <see cref="RowRef"/> carries both <see cref="RowRef.Bytes"/> and a by-name column accessor,
    /// and the bundled spatial strategy reads only columns — so it is tempting to hand the
    /// strategy a <c>RowRef</c> built over the caller's dictionary with an empty
    /// <see cref="RowRef.Bytes"/> and skip this entirely. A strategy that reached for the bytes
    /// would then read <em>empty rather than throw</em>: rows silently routed to the wrong shard
    /// and landing authoritative in the wrong engine, which is the failure class #114 existed to
    /// remove. The cost is an encode the owning shard redoes when it allocates <c>[AutoInc]</c>,
    /// and bulk's advantage is transaction overhead rather than encoding, so paying it twice is
    /// noise.</para>
    /// </summary>
    private static (byte[] Bytes, string[] Supplied) Preimage(TableSchema table, in BulkRow row)
    {
        var values = new Dictionary<string, object?>(row.Columns.Count, StringComparer.Ordinal);
        foreach (var (name, value) in row.Columns)
            values[name] = RowSerializer.CoerceValue(table, table.Column(name), value);
        return (RowSerializer.SerializeValues(table, values), [.. values.Keys]);
    }

    /// <summary>
    /// Refuses the whole batch when a destination shard does not exist and
    /// <c>Bulk:CreateShards</c> has not opted into creating it — before any engine has written,
    /// so a refused batch is one that did not half-land.
    /// <para>
    /// The asymmetry is deliberate. A world generator touching thousands of shard keys would
    /// otherwise turn one POST into thousands of shards, originators, and data directories, and
    /// while #112 gave the cluster a way to reap an empty shard, reaping is a deliberate operator
    /// action rather than something that happens on its own. Code is revertible; durable
    /// directories are not.
    /// </para>
    /// </summary>
    private async Task PrepareDestinationsAsync(IEnumerable<ShardKey> shards, CancellationToken ct)
    {
        var missing = shards.Where(shard => _hub.Membership.GetAssignment(shard) is null).ToList();
        if (missing.Count == 0)
            return;

        if (_options.CurrentValue.Bulk.CreateShards)
        {
            // Created and opened before any write, not lazily on first send: a shard's owner
            // learns of an assignment on its next heartbeat, and a bake creates and writes in the
            // same breath. Doing it up front also keeps the all-or-nothing promise — if a shard
            // cannot be brought up, nothing has been written yet.
            foreach (var shard in missing)
                await _hub.EnsureShardOpenAsync(shard, ct).ConfigureAwait(false);
            return;
        }

        const int Named = 8;
        var names = string.Join(", ", missing.Take(Named).Select(static s => $"shard:{s.Value}"));
        var rest = missing.Count > Named ? $", and {missing.Count - Named} more" : string.Empty;
        throw new ArgumentException(
            $"This batch routes to {missing.Count} shard(s) that do not exist yet ({names}{rest}). Bulk does not "
            + "create shards: a bake spanning thousands of keys would create thousands of shards and their data "
            + "directories from one request. Pre-declare them (MelangeClusterCoordinator.EnsureShard), or set "
            + "Bulk:CreateShards to true to accept that. Nothing was written.");
    }
}
