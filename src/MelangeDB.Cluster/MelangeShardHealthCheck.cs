using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The <c>melange-shard</c> health check: unhealthy when this shard node's ownership is unknown
/// or contested — its hub lease has expired, so it has self-fenced and the hub may have
/// reassigned its shards. Degraded while registered but owning nothing (valid, but worth seeing).
/// Healthy on the hub and on single-node deployments, where ownership is not a question.
/// </summary>
internal sealed class MelangeShardHealthCheck : IHealthCheck
{
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    public MelangeShardHealthCheck(IServiceProvider services, IOptionsMonitor<MelangeDbOptions> options)
    {
        _services = services;
        _options = options;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (_options.CurrentValue.Cluster.Role != ClusterRole.Shard)
            return Task.FromResult(HealthCheckResult.Healthy("Not a shard node; ownership is not a question here."));

        var node = _services.GetRequiredService<ShardNodeRuntime>();
        if (!node.LeaseValid())
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "The hub lease expired (Cluster:FailureTimeoutMs without a heartbeat); this node has self-fenced " +
                "its shards and their ownership is contested until it re-registers."));
        }

        var owned = node.OwnedShards;
        return Task.FromResult(owned.Count == 0
            ? HealthCheckResult.Degraded("Registered and leased, but owning no shards.")
            : HealthCheckResult.Healthy($"Holding a live lease on {owned.Count} shard(s)."));
    }
}
