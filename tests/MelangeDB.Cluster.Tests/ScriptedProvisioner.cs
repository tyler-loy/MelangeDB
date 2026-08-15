using System.Collections.Concurrent;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The capacity seam's test double, wired to the fixture after startup: <see cref="OnRequest"/>
/// plays the cloud spinning up an instance, <see cref="OnDecommission"/> plays it tearing one
/// down. Left null, <see cref="OnRequest"/> throws — a provisioner with nothing to give is also a
/// scenario worth having.
/// </summary>
internal sealed class ScriptedProvisioner : INodeProvisioner
{
    public Func<CapacityRequest, CancellationToken, Task<ProvisionTicket>>? OnRequest { get; set; }

    public Func<string, CancellationToken, Task>? OnDecommission { get; set; }

    public ConcurrentQueue<CapacityRequest> Requests { get; } = new();

    public ConcurrentQueue<string> Decommissions { get; } = new();

    public Task<ProvisionTicket> RequestNodeAsync(CapacityRequest request, CancellationToken ct)
    {
        Requests.Enqueue(request);
        return OnRequest is { } handler
            ? handler(request, ct)
            : throw new InvalidOperationException("This provisioner has no capacity to give.");
    }

    public async Task DecommissionAsync(string nodeName, CancellationToken ct)
    {
        Decommissions.Enqueue(nodeName);
        if (OnDecommission is { } handler)
            await handler(nodeName, ct);
    }
}
