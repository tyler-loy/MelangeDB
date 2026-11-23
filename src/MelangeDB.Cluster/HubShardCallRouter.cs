using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The hub's <see cref="IShardCallRouter"/>: a client call named by shard goes to that shard's
/// node over the node link, carrying the caller's source and claims, and its failure comes back
/// as the exception the node raised. Registered on every clustered process like the bulk router,
/// and routing only on the hub.
/// </summary>
internal sealed class HubShardCallRouter(IServiceProvider services, IOptionsMonitor<MelangeDbOptions> options) : IShardCallRouter
{
    // Resolved on first use, for the bulk router's reason: this type is constructed on shard
    // nodes too, where a HubRuntime must not be stood up.
    private HubRuntime? _hub;

    private HubRuntime Hub => _hub ??= services.GetRequiredService<HubRuntime>();

    public bool RoutesByShard => options.CurrentValue.Cluster.Role == ClusterRole.Hub;

    public async Task<ulong> CallAsync(
        ulong shard,
        string reducer,
        Identity caller,
        ReadOnlyMemory<byte> arguments,
        CallSource source,
        CallerClaims claims,
        CancellationToken cancellationToken)
    {
        // Looked up, not resolved: resolving creates a shard on first use, and a call naming a
        // shard that does not exist must not bring one into being.
        var assignment = Hub.Membership.GetAssignment(new ShardKey(shard))
            ?? throw new ArgumentException($"Shard {shard} does not exist; a call never creates one.", nameof(shard));
        if (assignment.NodeName is null)
            throw new TransientRejectionException($"Shard {shard} has no owner right now (it is being reassigned); retry.");

        try
        {
            return await Hub.ExecuteOnShardAsync(assignment, reducer, caller, arguments, source, claims, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (NodeLinkException exception) when (ReducerCallErrors.FromCode(exception.Code, exception.Message, reducer) is { } failure)
        {
            throw failure;
        }
        catch (NodeLinkException exception) when (!exception.IsPeerError)
        {
            // A timeout or a dead link: the node may have committed the call before it went quiet.
            throw new CommitOutcomeUnknownException(
                $"The call to shard {shard} lost its node link before an answer arrived, so whether it committed is unknown: {exception.Message}");
        }
        catch (InvalidOperationException exception) when (exception is not NodeLinkException)
        {
            // No live link to the owner: nothing was sent.
            throw new TransientRejectionException(exception.Message);
        }
    }
}
