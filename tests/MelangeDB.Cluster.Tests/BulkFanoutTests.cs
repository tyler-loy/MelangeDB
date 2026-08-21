using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Bulk ingestion in a cluster: one batch posted to the hub, fanned out to the shard engines that
/// own its rows (#115).
///
/// <para>Bulk is the one write path that could not be answered by refusing the way ad-hoc SQL on a
/// <c>Partitioned</c> table was (#114), because refusing leaves a clustered deployment with no way
/// to seed a world except routed reducer calls — forfeiting the 44x advantage over per-row
/// transactions that phase 07 measured. So the tests here are mostly about the two things that go
/// wrong when a hub is allowed to decide destinations: rows landing in the wrong engine, and one
/// POST quietly bringing thousands of shards into existence.</para>
/// </summary>
public class BulkFanoutTests
{
    private const string BulkOwnerRole = "melange-bulk-owner";

    private static readonly Dictionary<string, string?> BulkEnabled = new()
    {
        ["MelangeDb:Bulk:Enabled"] = "true",
    };

    private static Dictionary<string, string?> BulkEnabledCreatingShards => new(BulkEnabled)
    {
        ["MelangeDb:Bulk:CreateShards"] = "true",
    };

    /// <summary>
    /// The whole feature in one pass: partitioned rows reach the shards owning their keys, hub
    /// tables stay on the hub, and the answer says which engine took what.
    ///
    /// <para>The <c>[AutoInc]</c> assertion is the load-bearing one. Ids are allocated with the
    /// allocating shard's originator prefix, so two shards allocating "the first Mob" produce
    /// different ids. If the hub had finalised the encoding — the obvious way to build a fan-out —
    /// both shards would have received id 0, or worse, the same id, and the two batches would
    /// collide in a way no single-shard test would ever show.</para>
    /// </summary>
    [Fact]
    public async Task A_batch_lands_in_the_shard_engines_that_own_its_rows()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, failureTimeoutMs: 60_000, extraSettings: BulkEnabled);
        await cluster.EnsureShardOwnedAsync(80);
        await cluster.EnsureShardOwnedAsync(81);

        var response = await PostBulkAsync(cluster, """
            {"tables": {
              "Mob": [
                {"InstanceId": 80, "Hp": 10},
                {"InstanceId": 80, "Hp": 11},
                {"InstanceId": 81, "Hp": 12}
              ],
              "ItemDef": [{"Id": 1, "Name": "pick"}]
            }}
            """);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var body = await ReadJsonAsync(response);
        Assert.True(body.GetProperty("ok").GetBoolean());
        Assert.Equal(4, body.GetProperty("rows").GetInt32());

        // Three engines took rows: the hub (ItemDef is Replicated) and both shards.
        var results = body.GetProperty("results").EnumerateArray().ToList();
        Assert.Equal(3, results.Count);
        int RowsFor(ulong? shard) => results
            .Single(r => (r.GetProperty("shard").ValueKind == JsonValueKind.Null
                ? (ulong?)null
                : r.GetProperty("shard").GetUInt64()) == shard)
            .GetProperty("rows").GetInt32();
        Assert.Equal(1, RowsFor(null));
        Assert.Equal(2, RowsFor(80));
        Assert.Equal(1, RowsFor(81));

        // Every result carries its own engine's LSN, which is the reason the shape is an array:
        // there is no such thing as "the LSN" of a batch that spanned three logs.
        Assert.All(results, r => Assert.True(r.GetProperty("lsn").GetUInt64() > 0));

        var in80 = MobsIn(cluster, 80);
        var in81 = MobsIn(cluster, 81);
        Assert.Equal([10, 11], in80.Select(static m => m.Hp).Order().ToList());
        Assert.Equal([12], in81.Select(static m => m.Hp).ToList());

        // Allocated by the shards, under their own originator prefixes — never by the hub.
        Assert.Empty(in80.Select(static m => m.Id).Intersect(in81.Select(static m => m.Id)));
        Assert.DoesNotContain(0UL, in80.Concat(in81).Select(static m => m.Id));
    }

    /// <summary>
    /// The irreversible-mistake guard. A world generator touching thousands of shard keys would
    /// otherwise turn one POST into thousands of shards, originators, and data directories — and
    /// while #112 made an empty shard reapable, reaping is a deliberate operator action, not
    /// something that happens on its own. Refused whole, before any engine wrote: a bake that
    /// half-lands is an investigation, and one that lands nothing is a re-post.
    /// </summary>
    [Fact]
    public async Task A_batch_routing_to_a_shard_that_does_not_exist_is_refused_whole()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, failureTimeoutMs: 60_000, extraSettings: BulkEnabled);
        await cluster.EnsureShardOwnedAsync(80);

        var response = await PostBulkAsync(cluster, """
            {"tables": {"Mob": [
              {"InstanceId": 80, "Hp": 10},
              {"InstanceId": 999, "Hp": 11}
            ]}}
            """);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var error = await ReadJsonAsync(response);
        Assert.Equal("invalid_args", error.GetProperty("error").GetString());
        Assert.Contains("shard:999", error.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.Contains("EnsureShard", error.GetProperty("message").GetString()!, StringComparison.Ordinal);

        // Whole means whole: the row destined for the shard that does exist was not written either.
        Assert.Null(cluster.Hub.Membership.GetAssignment(new ShardKey(999)));
        Assert.Empty(MobsIn(cluster, 80));
    }

    /// <summary>The opt-out, for a deployment that wants the old create-on-first-use behaviour.</summary>
    [Fact]
    public async Task Bulk_creates_destination_shards_when_the_deployment_opts_in()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, failureTimeoutMs: 60_000, extraSettings: BulkEnabledCreatingShards);

        var response = await PostBulkAsync(cluster, """
            {"tables": {"Mob": [{"InstanceId": 77, "Hp": 5}]}}
            """);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(77)));
        Assert.Equal([5], MobsIn(cluster, 77).Select(static m => m.Hp).ToList());
    }

    /// <summary>
    /// The receiver's re-resolution, provoked by giving the hub a strategy that disagrees with the
    /// nodes'. This is the check the fan-out's correctness rests on, and it is not covered by the
    /// existing shard-span check: <c>Cluster:ShardSpanCheck</c> defaults to <c>DebugOnly</c>, so in
    /// a Release cluster — the only kind with a bake to run — that check is off.
    ///
    /// <para>The fixture forces the span check to <c>Always</c>, so this test would pass even
    /// without the receiver's own check; what it is really asserting is the message and the
    /// all-or-nothing. The reason the receiver's check has to exist anyway is what the comment on
    /// <c>ApplyBulkGroup</c> records.</para>
    /// </summary>
    [Fact]
    public async Task A_row_the_hub_routed_to_the_wrong_shard_is_refused_by_the_receiver()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1,
            failureTimeoutMs: 60_000,
            extraSettings: BulkEnabled,
            configureHub: static services => services.AddSingleton<IShardStrategy>(static provider =>
                new MisroutingStrategy(new InstancingShardStrategy(
                    provider.GetRequiredService<SchemaRegistry>(),
                    static session => new ShardKey(
                        session.HubDb.Find<PlayerLocation>(session.Identity)?.InstanceId ?? 1)))));
        await cluster.EnsureShardOwnedAsync(80);
        await cluster.EnsureShardOwnedAsync(81);

        // The hub's strategy sends every Mob to shard 80; this one belongs to 81.
        var response = await PostBulkAsync(cluster, """
            {"tables": {"Mob": [{"InstanceId": 81, "Hp": 12}]}}
            """);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var message = (await ReadJsonAsync(response)).GetProperty("message").GetString()!;
        Assert.Contains("shard:81", message, StringComparison.Ordinal);
        Assert.Contains("shard:80", message, StringComparison.Ordinal);

        Assert.Empty(MobsIn(cluster, 80));
        Assert.Empty(MobsIn(cluster, 81));
    }

    /// <summary>
    /// The silent hole this change closes, at the layer that actually closes it.
    ///
    /// <para>A shard node's own engine holds only <c>Local</c> tables, and
    /// <c>PlacementGuards.NodeLocalAccess</c> has always said so — but that is a <em>table-access</em>
    /// guard, and bulk ingestion never touches a table handle. So a batch of <c>Partitioned</c>
    /// rows reaching that engine committed <em>successfully</em>: authoritative nowhere the game
    /// reads from, invisible to the shard owning the key, and answered as an ok. The hub has had
    /// the commit-point counterpart since clustering landed — its comment names bulk as the path
    /// it exists for — and the node-local engine simply never got one.</para>
    ///
    /// <para>The standard shard-node host maps only the shard websocket, so there is no bulk
    /// endpoint there to post to; a host that also maps the ordinary HTTP surface gets the
    /// endpoint-level refusal. The engine guard is what holds in both cases, and in the case the
    /// endpoint cannot see at all: the host's own code calling <c>BulkInsert</c> directly.</para>
    /// </summary>
    [Fact]
    public async Task A_shard_nodes_own_engine_refuses_partitioned_bulk_rows()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, failureTimeoutMs: 60_000, extraSettings: BulkEnabled);
        var node = cluster.Nodes[0];

        // No bulk endpoint on a shard node's host at all — it maps the shard socket only.
        var response = await PostBulkAsync(
            node.HttpPort,
            """{"tables": {"Mob": [{"InstanceId": 80, "Hp": 10}]}}""");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        var engine = node.App!.Services.GetRequiredService<MelangeEngine>();
        var failure = Assert.Throws<InvalidOperationException>(() => engine.BulkInsert(
            ClusterFixture.Caller,
            [new BulkRow("Mob", new Dictionary<string, object?> { ["InstanceId"] = 80u, ["Hp"] = 10 })]));
        Assert.Contains("node-local engine", failure.Message, StringComparison.Ordinal);

        // A Local table is what this engine is for, and still commits.
        Assert.NotNull(engine.BulkInsert(
            ClusterFixture.Caller,
            [new BulkRow("TickCount", new Dictionary<string, object?> { ["Id"] = 1L, ["Count"] = 3L })]));
    }

    private static IReadOnlyList<Mob> MobsIn(ClusterFixture cluster, ulong shard)
    {
        if (cluster.Hub.Membership.GetAssignment(new ShardKey(shard)) is null)
            return [];
        var engine = cluster.ShardOf(shard).Engine;
        var table = engine.Schema.Get(typeof(Mob));
        return engine.ReadConsistent(_ => engine.HotStore.Scan(table.Id)
            .Select(row => (Mob)RowSerializer.Deserialize(table, row.Value))
            .ToList());
    }

    private static Task<HttpResponseMessage> PostBulkAsync(ClusterFixture cluster, string json) =>
        PostBulkAsync(cluster.GatewayUri.Port, json);

    private static async Task<HttpResponseMessage> PostBulkAsync(int port, string json)
    {
        using var http = new HttpClient();
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Bearer", TestTokens.For("loader", role: BulkOwnerRole));
        return await http.PostAsync(
            new Uri($"http://127.0.0.1:{port}/melange/bulk"),
            new StringContent(json, Encoding.UTF8, "application/json"),
            TestContext.Current.CancellationToken);
    }

    private static async Task<JsonElement> ReadJsonAsync(HttpResponseMessage response) =>
        JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement.Clone();

    /// <summary>
    /// A hub strategy that routes every <c>Mob</c> to shard 80 regardless of its key, delegating
    /// everything else. Stands in for the failure the receiver's re-resolution exists to catch: a
    /// hub whose shard map or strategy has drifted from the nodes'.
    /// </summary>
    private sealed class MisroutingStrategy(IShardStrategy inner) : IShardStrategy
    {
        public ShardKey ShardForRow(TableId table, in RowRef row) => new(80);

        public ShardKey ShardForSession(SessionContext session) => inner.ShardForSession(session);

        public IReadOnlyList<ShardKey> InterestOf(ShardKey shard) => inner.InterestOf(shard);
    }
}
