using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The <c>melange-capacity</c> health check: the operator-alert half of the provisioning loop's
/// give-up posture (the applier-stall precedent — the log line is EventId 1738, this is its
/// health-endpoint form). Unhealthy once two provision attempts failed or expired, because the
/// loop deliberately stops asking at that point and a human is the escalation path. Degraded
/// while a ticket is outstanding — normal, but worth seeing. Healthy everywhere capacity is not
/// the hub's question.
/// </summary>
internal sealed class MelangeCapacityHealthCheck : IHealthCheck
{
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    public MelangeCapacityHealthCheck(IServiceProvider services, IOptionsMonitor<MelangeDbOptions> options)
    {
        _services = services;
        _options = options;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (_options.CurrentValue.Cluster.Role != ClusterRole.Hub)
            return Task.FromResult(HealthCheckResult.Healthy("Not the hub; capacity is not decided here."));

        var hub = _services.GetRequiredService<HubRuntime>();
        if (!hub.HasProvisioner)
            return Task.FromResult(HealthCheckResult.Healthy("No node provisioner registered; the fleet is fixed."));

        if (hub.ProvisionHasGivenUp)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "Two provision attempts failed or expired (EventId 1738); the loop has stopped asking because money is " +
                "involved. Fix the provisioner or add capacity by hand — a ticket-named node joining membership clears this."));
        }

        return Task.FromResult(hub.OutstandingProvision is { } ticket
            ? HealthCheckResult.Degraded(
                $"Waiting on provision ticket '{ticket.TicketId}' — node '{ticket.NodeName}' has not joined membership yet.")
            : HealthCheckResult.Healthy("No capacity request outstanding."));
    }
}
