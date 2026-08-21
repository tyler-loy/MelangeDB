using MelangeDB.Cluster;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// The production membership store: the cluster's ownership registry in the hub's own Postgres —
/// the settled phase 09 answer to cluster membership, taken instead of introducing Raft a phase
/// early. What must survive a hub restart lives here: which fencing tokens were minted (a
/// restarted hub must never re-mint an old token), each shard's originator id (AutoInc ranges),
/// and node registrations. Mutations serialize on an exclusive lock over the shards table —
/// membership changes are rare, and correctness beats concurrency here.
/// </summary>
public sealed class PostgresMembershipStore : IMembershipStore
{
    private readonly PostgresConnectionSource _source;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly Lock _schemaLock = new();
    private bool _schemaReady;

    public PostgresMembershipStore(PostgresConnectionSource source, IOptionsMonitor<MelangeDbOptions> options)
    {
        _source = source;
        _options = options;
    }

    private string Schema => PostgresIdentifier.Quote(_options.CurrentValue.Postgres.Schema);

    private NpgsqlConnection Open()
    {
        EnsureSchema();
        return _source.DataSource.OpenConnection();
    }

    private void EnsureSchema()
    {
        lock (_schemaLock)
        {
            if (_schemaReady)
                return;
            using var connection = _source.DataSource.OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = $"""
                CREATE SCHEMA IF NOT EXISTS {Schema};
                CREATE TABLE IF NOT EXISTS {Schema}.melange_cluster_nodes (
                    node_name text PRIMARY KEY,
                    public_address text NOT NULL,
                    last_seen_unix_ms bigint NOT NULL,
                    alive boolean NOT NULL
                );
                CREATE TABLE IF NOT EXISTS {Schema}.melange_cluster_shards (
                    shard bigint PRIMARY KEY,
                    node_name text NULL,
                    fencing_token bigint NOT NULL,
                    originator int NOT NULL
                );
                CREATE TABLE IF NOT EXISTS {Schema}.melange_cluster_originator (
                    lock_row boolean PRIMARY KEY,
                    next_originator int NOT NULL
                );
                INSERT INTO {Schema}.melange_cluster_originator (lock_row, next_originator)
                VALUES (true, COALESCE((SELECT MAX(originator) FROM {Schema}.melange_cluster_shards), 0) + 1)
                ON CONFLICT (lock_row) DO NOTHING;
                """;
            command.ExecuteNonQuery();
            _schemaReady = true;
        }
    }

    public NodeRecord RegisterNode(string nodeName, string publicAddress, DateTimeOffset now)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeName);
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            INSERT INTO {Schema}.melange_cluster_nodes (node_name, public_address, last_seen_unix_ms, alive)
            VALUES (@name, @address, @seen, true)
            ON CONFLICT (node_name) DO UPDATE
                SET public_address = @address, last_seen_unix_ms = @seen, alive = true
            """;
        command.Parameters.AddWithValue("name", nodeName);
        command.Parameters.AddWithValue("address", publicAddress);
        command.Parameters.AddWithValue("seen", now.ToUnixTimeMilliseconds());
        command.ExecuteNonQuery();
        return new NodeRecord(nodeName, publicAddress, DateTimeOffset.FromUnixTimeMilliseconds(now.ToUnixTimeMilliseconds()), true);
    }

    public bool Heartbeat(string nodeName, DateTimeOffset now)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            UPDATE {Schema}.melange_cluster_nodes
            SET last_seen_unix_ms = @seen, alive = true
            WHERE node_name = @name
            """;
        command.Parameters.AddWithValue("seen", now.ToUnixTimeMilliseconds());
        command.Parameters.AddWithValue("name", nodeName);
        return command.ExecuteNonQuery() == 1;
    }

    public IReadOnlyList<NodeRecord> Nodes()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT node_name, public_address, last_seen_unix_ms, alive
            FROM {Schema}.melange_cluster_nodes ORDER BY node_name
            """;
        using var reader = command.ExecuteReader();
        var nodes = new List<NodeRecord>();
        while (reader.Read())
        {
            nodes.Add(new NodeRecord(
                reader.GetString(0),
                reader.GetString(1),
                DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(2)),
                reader.GetBoolean(3)));
        }

        return nodes;
    }

    public ShardAssignment EnsureShard(ShardKey shard, DateTimeOffset now)
    {
        using var connection = Open();
        using var transaction = connection.BeginTransaction();
        LockShards(connection, transaction);
        if (ReadAssignment(connection, transaction, shard) is { } existing)
            return existing;

        // A high-water mark, never derived from the live rows: ids minted under an originator
        // outlive their shard, so a removed shard's prefix has to retire with it.
        ushort originator;
        using (var next = connection.CreateCommand())
        {
            next.Transaction = transaction;
            next.CommandText = $"""
                UPDATE {Schema}.melange_cluster_originator
                SET next_originator = next_originator + 1
                WHERE lock_row
                RETURNING next_originator - 1
                """;
            // No row means the singleton is missing or corrupt, and EnsureSchema will not seed it
            // again this process. Convert.ToInt32(null) would be 0 — the hub's own originator —
            // so the failure would surface as ids colliding with the hub's rather than as a fault.
            if (next.ExecuteScalar() is not { } scalar || Convert.ToInt32(scalar) is var allocated && allocated <= 0)
            {
                throw new InvalidOperationException(
                    $"The originator high-water row in {Schema}.melange_cluster_originator is missing or invalid; "
                    + "shard originators cannot be allocated. Restore the cluster schema from backup.");
            }

            if (allocated > ushort.MaxValue)
                throw new InvalidOperationException("The 16-bit originator space is exhausted.");
            originator = (ushort)allocated;
        }

        var owner = LeastLoadedNode(connection, transaction, except: null);
        using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = $"""
                INSERT INTO {Schema}.melange_cluster_shards (shard, node_name, fencing_token, originator)
                VALUES (@shard, @node, 1, @originator)
                """;
            insert.Parameters.AddWithValue("shard", unchecked((long)shard.Value));
            insert.Parameters.AddWithValue("node", (object?)owner ?? DBNull.Value);
            insert.Parameters.AddWithValue("originator", (int)originator);
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
        return new ShardAssignment(shard, owner, 1, originator);
    }

    public ShardAssignment? GetAssignment(ShardKey shard)
    {
        using var connection = Open();
        return ReadAssignment(connection, transaction: null, shard);
    }

    public IReadOnlyList<ShardAssignment> AssignmentsFor(string nodeName)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT shard, node_name, fencing_token, originator
            FROM {Schema}.melange_cluster_shards WHERE node_name = @name ORDER BY shard
            """;
        command.Parameters.AddWithValue("name", nodeName);
        return ReadAssignments(command);
    }

    public IReadOnlyList<ShardAssignment> AllAssignments()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT shard, node_name, fencing_token, originator
            FROM {Schema}.melange_cluster_shards ORDER BY shard
            """;
        return ReadAssignments(command);
    }

    public IReadOnlyList<ShardAssignment> MarkDead(string nodeName, DateTimeOffset now)
    {
        using var connection = Open();
        using var transaction = connection.BeginTransaction();
        LockShards(connection, transaction);
        using (var dead = connection.CreateCommand())
        {
            dead.Transaction = transaction;
            dead.CommandText = $"""
                UPDATE {Schema}.melange_cluster_nodes
                SET alive = false, last_seen_unix_ms = @seen WHERE node_name = @name
                """;
            dead.Parameters.AddWithValue("seen", now.ToUnixTimeMilliseconds());
            dead.Parameters.AddWithValue("name", nodeName);
            dead.ExecuteNonQuery();
        }

        var changed = new List<ShardAssignment>();
        foreach (var assignment in AssignmentsOf(connection, transaction, nodeName))
        {
            var next = assignment with
            {
                NodeName = LeastLoadedNode(connection, transaction, except: nodeName),
                FencingToken = assignment.FencingToken + 1,
            };
            WriteAssignment(connection, transaction, next);
            changed.Add(next);
        }

        transaction.Commit();
        return changed;
    }

    public IReadOnlyList<ShardAssignment> AssignUnowned(DateTimeOffset now)
    {
        using var connection = Open();
        using var transaction = connection.BeginTransaction();
        LockShards(connection, transaction);
        var changed = new List<ShardAssignment>();
        foreach (var assignment in AssignmentsOf(connection, transaction, nodeName: null))
        {
            if (LeastLoadedNode(connection, transaction, except: null) is not { } owner)
                break;
            var next = assignment with { NodeName = owner, FencingToken = assignment.FencingToken + 1 };
            WriteAssignment(connection, transaction, next);
            changed.Add(next);
        }

        transaction.Commit();
        return changed;
    }

    public ShardAssignment Reassign(ShardKey shard, string toNode, DateTimeOffset now)
    {
        ArgumentException.ThrowIfNullOrEmpty(toNode);
        using var connection = Open();
        using var transaction = connection.BeginTransaction();
        LockShards(connection, transaction);
        var assignment = ReadAssignment(connection, transaction, shard)
            ?? throw new InvalidOperationException($"{shard} was never created; nothing to reassign.");
        using (var alive = connection.CreateCommand())
        {
            alive.Transaction = transaction;
            alive.CommandText = $"SELECT alive FROM {Schema}.melange_cluster_nodes WHERE node_name = @name";
            alive.Parameters.AddWithValue("name", toNode);
            if (alive.ExecuteScalar() is not true)
                throw new InvalidOperationException($"Node '{toNode}' is not registered and alive; a drain must never assign to a corpse.");
        }

        var next = assignment with { NodeName = toNode, FencingToken = assignment.FencingToken + 1 };
        WriteAssignment(connection, transaction, next);
        transaction.Commit();
        return next;
    }

    public bool RemoveShard(ShardKey shard)
    {
        using var connection = Open();
        using var transaction = connection.BeginTransaction();
        LockShards(connection, transaction);
        using var command = connection.CreateCommand();
        command.Transaction = transaction;

        // Only the ownership row goes. melange_cluster_originator is untouched: it is a
        // high-water mark rather than a free list, so this shard's prefix retires with it and the
        // next shard created gets a number no shard has ever held.
        command.CommandText = $"DELETE FROM {Schema}.melange_cluster_shards WHERE shard = @shard";
        command.Parameters.AddWithValue("shard", unchecked((long)shard.Value));
        var removed = command.ExecuteNonQuery() > 0;
        transaction.Commit();
        return removed;
    }

    private void LockShards(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"LOCK TABLE {Schema}.melange_cluster_shards IN EXCLUSIVE MODE";
        command.ExecuteNonQuery();
    }

    private ShardAssignment? ReadAssignment(NpgsqlConnection connection, NpgsqlTransaction? transaction, ShardKey shard)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT shard, node_name, fencing_token, originator
            FROM {Schema}.melange_cluster_shards WHERE shard = @shard
            """;
        command.Parameters.AddWithValue("shard", unchecked((long)shard.Value));
        var assignments = ReadAssignments(command);
        return assignments.Count == 1 ? assignments[0] : null;
    }

    private List<ShardAssignment> AssignmentsOf(NpgsqlConnection connection, NpgsqlTransaction transaction, string? nodeName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = nodeName is null
            ? $"SELECT shard, node_name, fencing_token, originator FROM {Schema}.melange_cluster_shards WHERE node_name IS NULL ORDER BY shard"
            : $"SELECT shard, node_name, fencing_token, originator FROM {Schema}.melange_cluster_shards WHERE node_name = @name ORDER BY shard";
        if (nodeName is not null)
            command.Parameters.AddWithValue("name", nodeName);
        return ReadAssignments(command);
    }

    private void WriteAssignment(NpgsqlConnection connection, NpgsqlTransaction transaction, ShardAssignment assignment)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            UPDATE {Schema}.melange_cluster_shards
            SET node_name = @node, fencing_token = @token WHERE shard = @shard
            """;
        command.Parameters.AddWithValue("node", (object?)assignment.NodeName ?? DBNull.Value);
        command.Parameters.AddWithValue("token", assignment.FencingToken);
        command.Parameters.AddWithValue("shard", unchecked((long)assignment.Shard.Value));
        command.ExecuteNonQuery();
    }

    private string? LeastLoadedNode(NpgsqlConnection connection, NpgsqlTransaction transaction, string? except)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT n.node_name
            FROM {Schema}.melange_cluster_nodes n
            LEFT JOIN {Schema}.melange_cluster_shards s ON s.node_name = n.node_name
            WHERE n.alive AND (@except IS NULL OR n.node_name <> @except)
            GROUP BY n.node_name
            ORDER BY COUNT(s.shard), n.node_name
            LIMIT 1
            """;
        var parameter = command.Parameters.Add("except", NpgsqlTypes.NpgsqlDbType.Text);
        parameter.Value = (object?)except ?? DBNull.Value;
        return command.ExecuteScalar() as string;
    }

    private static List<ShardAssignment> ReadAssignments(NpgsqlCommand command)
    {
        using var reader = command.ExecuteReader();
        var assignments = new List<ShardAssignment>();
        while (reader.Read())
        {
            assignments.Add(new ShardAssignment(
                new ShardKey(unchecked((ulong)reader.GetInt64(0))),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetInt64(2),
                (ushort)reader.GetInt32(3)));
        }

        return assignments;
    }
}

/// <summary>Registers the Postgres-backed membership store on a hub.</summary>
public static class PostgresMembershipServiceCollectionExtensions
{
    /// <summary>
    /// Uses Postgres for cluster membership, replacing the in-memory default registered by
    /// <c>AddMelangeCluster</c>. Call after it; requires <c>Postgres:ConnectionString</c> — the
    /// hub already has Postgres for its Global tier, which is the whole argument for this store.
    /// </summary>
    public static IServiceCollection AddPostgresClusterMembership(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<PostgresConnectionSource>();
        services.Replace(ServiceDescriptor.Singleton<IMembershipStore>(static provider => new PostgresMembershipStore(
            provider.GetRequiredService<PostgresConnectionSource>(),
            provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>())));
        return services;
    }
}
