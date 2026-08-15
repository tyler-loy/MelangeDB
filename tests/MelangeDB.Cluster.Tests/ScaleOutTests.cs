using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Provision-then-reassign (road-to-0.2 phase 14): the loop's second move, taken only when every
/// live node is sustained-hot, bounded by the ceiling and the single-ticket rule, and — on
/// repeated failure — deliberately stopping to tell a human rather than keep spending.
/// </summary>
public sealed class ScaleOutTests
{
    /// <summary>
    /// The seam's test double, wired to the fixture after startup: <see cref="OnRequest"/> plays
    /// the cloud. Left null it throws, which is also a scenario worth having.
    /// </summary>
    private sealed class ScriptedProvisioner : INodeProvisioner
    {
        public Func<CapacityRequest, CancellationToken, Task<ProvisionTicket>>? OnRequest { get; set; }

        public ConcurrentQueue<CapacityRequest> Requests { get; } = new();

        public ConcurrentQueue<string> Decommissions { get; } = new();

        public Task<ProvisionTicket> RequestNodeAsync(CapacityRequest request, CancellationToken ct)
        {
            Requests.Enqueue(request);
            return OnRequest is { } handler
                ? handler(request, ct)
                : throw new InvalidOperationException("This provisioner has no capacity to give.");
        }

        public Task DecommissionAsync(string nodeName, CancellationToken ct)
        {
            Decommissions.Enqueue(nodeName);
            return Task.CompletedTask;
        }
    }

    private static Dictionary<string, string?> Settings(int maxNodes, int ticketTimeoutMs = 60_000) => new()
    {
        ["MelangeDb:Cluster:RebalanceEnabled"] = "true",
        ["MelangeDb:Cluster:RebalanceWindowSeconds"] = "1",
        ["MelangeDb:Cluster:RebalanceHotUtilization"] = "0.01",

        // Long on purpose (the RebalanceLoopTests precedent): after the corrective moves, nothing
        // else may move for the rest of the test, so a flapping loop fails the assertion.
        ["MelangeDb:Cluster:ShardMoveMinIntervalMs"] = "600000",
        ["MelangeDb:Cluster:MaxNodes"] = maxNodes.ToString(),
        ["MelangeDb:Cluster:ProvisionTicketTimeoutMs"] = ticketTimeoutMs.ToString(),
    };

    /// <summary>Pumps reducer calls at the given shards until cancelled, riding out drain windows.</summary>
    private static Task[] Pump(ClusterFixture fixture, ulong[] shards, CancellationToken ct) =>
        shards.Select(shard => Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await fixture.Coordinator.ExecuteOnShardAsync(
                        new ShardKey(shard), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [(uint)shard, 1], ct);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                    // Mid-drain the shard briefly serves nowhere; a live game would retry too.
                }
            }
        }, TestContext.Current.CancellationToken)).ToArray();

    private static async Task DrainPumpsAsync(Task[] pumps)
    {
        try
        {
            await Task.WhenAll(pumps);
        }
        catch (Exception)
        {
        }
    }

    [Fact]
    public async Task An_all_hot_fleet_provisions_one_node_spreads_onto_it_and_respects_the_ceiling()
    {
        var provisioner = new ScriptedProvisioner();
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 1,
            extraSettings: Settings(maxNodes: 2),
            configureHub: services => services.AddSingleton<INodeProvisioner>(provisioner));

        // Fire-and-track, as the contract states: the ticket is the promise, returned at once;
        // the instance comes up on its own and announces itself by registering.
        var testCt = TestContext.Current.CancellationToken;
        provisioner.OnRequest = (_, _) =>
        {
            _ = Task.Run(() => fixture.AddNodeAsync("node-b"), testCt);
            return Task.FromResult(new ProvisionTicket("ticket-1", "node-b"));
        };

        await fixture.EnsureShardOwnedAsync(60);
        await fixture.EnsureShardOwnedAsync(61);

        using var pump = new CancellationTokenSource();
        var pumps = Pump(fixture, [60, 61], pump.Token);
        try
        {
            // The whole second move: all-hot on one node -> ticket -> node-b joins -> the
            // ordinary phase 13 rule spreads a shard onto the empty newcomer.
            await ClusterFixture.WaitUntilAsync(
                () => fixture.Hub.Metrics.ProvisionsFulfilled == 1
                    && fixture.Hub.Metrics.DrainsCompleted == 1
                    && fixture.Hub.Membership.GetAssignment(new ShardKey(60))!.NodeName
                        != fixture.Hub.Membership.GetAssignment(new ShardKey(61))!.NodeName,
                "the fleet grew to two nodes and the busy shards split across them",
                timeoutSeconds: 60);

            // Load stays on, both nodes now hot — but the fleet is at Cluster:MaxNodes, so the
            // loop must not ask again, and one shard per node leaves nothing worth moving.
            await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(3)), TestContext.Current.CancellationToken);
            Assert.Single(provisioner.Requests);
            Assert.Empty(provisioner.Decommissions);
            Assert.Equal(1, fixture.Hub.Metrics.DrainsCompleted);
            Assert.Equal(2, fixture.Hub.Membership.Nodes().Count(static n => n.Alive));
        }
        finally
        {
            pump.Cancel();
            await DrainPumpsAsync(pumps);
        }
    }

    [Fact]
    public async Task A_ticket_that_never_materializes_re_requests_once_then_alerts_and_stops()
    {
        var provisioner = new ScriptedProvisioner();
        var issued = 0;
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 1,
            extraSettings: Settings(maxNodes: 3, ticketTimeoutMs: 1_500),
            configureHub: services =>
            {
                services.AddSingleton<INodeProvisioner>(provisioner);
                services.AddHealthChecks();
            });

        // Tickets are promised and never kept: the cloud accepts the request and no node comes.
        provisioner.OnRequest = (_, _) =>
        {
            var n = Interlocked.Increment(ref issued);
            return Task.FromResult(new ProvisionTicket($"ticket-{n}", $"node-ghost-{n}"));
        };

        await fixture.EnsureShardOwnedAsync(62);
        await fixture.EnsureShardOwnedAsync(63);

        using var pump = new CancellationTokenSource();
        var pumps = Pump(fixture, [62, 63], pump.Token);
        try
        {
            // Exactly one re-request, then the give-up latch (EventId 1738).
            await ClusterFixture.WaitUntilAsync(
                () => fixture.Hub.Metrics.ProvisionsExpired == 2 && fixture.Hub.ProvisionHasGivenUp,
                "two tickets expired and the loop gave up",
                timeoutSeconds: 40);

            // Still all-hot, and the loop has deliberately stopped asking.
            await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(3)), TestContext.Current.CancellationToken);
            Assert.Equal(2, provisioner.Requests.Count);

            // The alert's health-endpoint form: melange-capacity is unhealthy on the hub.
            var health = await fixture.HubApp.Services.GetRequiredService<HealthCheckService>()
                .CheckHealthAsync(TestContext.Current.CancellationToken);
            Assert.Equal(HealthStatus.Unhealthy, health.Entries["melange-capacity"].Status);
        }
        finally
        {
            pump.Cancel();
            await DrainPumpsAsync(pumps);
        }
    }

    [Fact]
    public async Task A_node_arriving_after_its_ticket_expired_is_decommissioned_without_owning_a_shard()
    {
        var provisioner = new ScriptedProvisioner();
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 1,
            extraSettings: Settings(maxNodes: 3, ticketTimeoutMs: 1_500),
            configureHub: services => services.AddSingleton<INodeProvisioner>(provisioner));
        // Only the FIRST ticket names node-late; the retry names a ghost. Otherwise the retry's
        // outstanding ticket would greet the late arrival as fulfillment instead of surplus.
        var issued = 0;
        provisioner.OnRequest = (_, _) =>
        {
            var n = Interlocked.Increment(ref issued);
            return Task.FromResult(n == 1
                ? new ProvisionTicket("ticket-late", "node-late")
                : new ProvisionTicket($"ticket-{n}", $"node-ghost-{n}"));
        };

        await fixture.EnsureShardOwnedAsync(64);
        await fixture.EnsureShardOwnedAsync(65);

        using var pump = new CancellationTokenSource();
        var pumps = Pump(fixture, [64, 65], pump.Token);
        try
        {
            await ClusterFixture.WaitUntilAsync(
                () => fixture.Hub.Metrics.ProvisionsExpired >= 1,
                "the first ticket expired",
                timeoutSeconds: 40);
        }
        finally
        {
            // Load off before the late arrival: the episode is over, nothing is waiting for it.
            pump.Cancel();
            await DrainPumpsAsync(pumps);
        }

        await fixture.AddNodeAsync("node-late");

        await ClusterFixture.WaitUntilAsync(
            () => provisioner.Decommissions.Contains("node-late"),
            "the late arrival was handed back to the provisioner",
            timeoutSeconds: 30);
        Assert.Empty(fixture.Hub.Membership.AssignmentsFor("node-late"));
        Assert.Equal(1, fixture.Hub.Metrics.DecommissionsRequested);
    }
}
