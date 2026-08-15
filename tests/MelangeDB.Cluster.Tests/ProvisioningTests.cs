using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The capacity seam (road-to-0.2 phase 14): an <see cref="INodeProvisioner"/> is a DI-registered
/// component, its ceiling is an explicit decision, and registering one changes nothing until the
/// loop's first move is genuinely unavailable.
/// </summary>
public sealed class ProvisioningTests
{
    /// <summary>
    /// A provisioner that only records what the hub asked of it. Slice-1 tests assert it is never
    /// consulted; later phases hand fulfillment to the fixture.
    /// </summary>
    internal sealed class RecordingProvisioner : INodeProvisioner
    {
        public ConcurrentQueue<CapacityRequest> Requests { get; } = new();

        public ConcurrentQueue<string> Decommissions { get; } = new();

        public Task<ProvisionTicket> RequestNodeAsync(CapacityRequest request, CancellationToken ct)
        {
            Requests.Enqueue(request);
            return Task.FromResult(new ProvisionTicket($"ticket-{Requests.Count}", $"node-provisioned-{Requests.Count}"));
        }

        public Task DecommissionAsync(string nodeName, CancellationToken ct)
        {
            Decommissions.Enqueue(nodeName);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task A_registered_provisioner_without_MaxNodes_is_refused_at_startup()
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => ClusterFixture.StartAsync(
            shardNodes: 0,
            configureHub: static services => services.AddSingleton<INodeProvisioner>(new RecordingProvisioner())));

        // The refusal names the key and the reasoning: the ceiling is a spending decision.
        Assert.Contains("Cluster:MaxNodes", exception.Message);
        Assert.Contains("INodeProvisioner", exception.Message);
    }

    [Fact]
    public async Task A_provisioner_with_a_ceiling_starts_normally_and_registration_alone_never_spends()
    {
        var provisioner = new RecordingProvisioner();
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 2,
            extraSettings: new Dictionary<string, string?> { ["MelangeDb:Cluster:MaxNodes"] = "3" },
            configureHub: services => services.AddSingleton<INodeProvisioner>(provisioner));

        // The cluster is an ordinary fixed fleet: shards assign, open, and serve.
        var owner = await fixture.EnsureShardOwnedAsync(90);
        await fixture.Coordinator.ExecuteOnShardAsync(
            new ShardKey(90), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [90u, 1],
            TestContext.Current.CancellationToken);
        Assert.NotNull(owner.Runtime.TryGetShard(new ShardKey(90)));

        // Registration alone never spends: no scale-out condition, no call.
        Assert.Empty(provisioner.Requests);
        Assert.Empty(provisioner.Decommissions);
    }
}
