namespace MelangeDB.Cluster;

/// <summary>
/// Why the hub is asking for capacity: the fleet arithmetic at the moment of the decision.
/// Deliberately minimal — <see cref="LiveNodes"/> and <see cref="MaxNodes"/> are the contract, and
/// <see cref="Reason"/> is an opaque human-readable diagnostic (the load arithmetic, for logs and
/// tickets). A provisioner that parses <see cref="Reason"/> to pick instance types is coupling
/// itself to MelangeDB's metric definitions, which this seam exists to keep apart: size instances
/// from your own deployment's knowledge, not from the hub's utilization numbers.
/// </summary>
public sealed record CapacityRequest(int LiveNodes, int MaxNodes, string Reason);

/// <summary>
/// The provisioner's promise that a node is on its way. <see cref="NodeName"/> is the name the new
/// node will join membership under — the provisioner configures the instance's
/// <c>Cluster:NodeName</c> (plus <c>Cluster:HubAddress</c> and the cluster secret) and tells the
/// hub that name here, which is the entire correlation mechanism: the hub watches membership for
/// exactly this name and never talks to the instance any other way. <see cref="TicketId"/> is the
/// provisioner's own correlation handle (an instance id, a job id), echoed in hub logs.
/// </summary>
public sealed record ProvisionTicket(string TicketId, string NodeName);

/// <summary>
/// The capacity seam (road-to-0.2 phase 14): how the hub obtains one more shard node when every
/// node it has is sustained-hot, and gives the emptiest one back when the fleet is cold. Which
/// cloud, rack, or stack of warm processes supplies the node is the deployment's business, not
/// MelangeDB's — implement this interface and register it as a DI singleton (the membership-store
/// precedent: a provisioner is a component with credentials, not a configuration string). No
/// registration means the fleet is fixed and the rebalance loop only rearranges the nodes it has.
///
/// <para>The contract clauses that do the safety work:</para>
/// <list type="bullet">
/// <item><b>Fire-and-track.</b> The hub records the ticket and moves on; a provisioned node
/// announces itself by connecting and joining membership like any other node. There is no special
/// path to keep correct — a hand-started node and a provisioned one are indistinguishable once
/// registered.</item>
/// <item><b>At-least-once, made safe by fencing.</b> A ticket that expires triggers exactly one
/// re-request, so a slow instance can arrive after its replacement. A node arriving after its
/// ticket expired owns nothing and can write nothing (every shard write carries a fencing token it
/// never held); the hub decommissions the surplus. Duplicate capacity is a cost, never a
/// correctness problem.</item>
/// <item><b>Shared-storage access is part of the contract.</b> The instance must reach the same
/// <c>Cluster:ShardDataPath</c> storage as every other shard node — a node that joins but cannot
/// open the shard-log store fails its first assignment loudly rather than sitting in membership
/// looking assignable.</item>
/// <item><b>Registration alone never spends.</b> The hub calls <see cref="RequestNodeAsync"/> only
/// when the loop's first move (rearranging existing nodes) is unavailable, never past
/// <c>Cluster:MaxNodes</c> — and a registered provisioner with that ceiling unset is refused at
/// startup, because unbounded-by-default is this phase's defining failure mode.</item>
/// </list>
///
/// <para>Both methods are called off the hub's loop thread and bounded by timeouts — a provisioner
/// that blocks or throws degrades the fleet to fixed, never the hub to dead.</para>
/// </summary>
public interface INodeProvisioner
{
    /// <summary>
    /// Starts provisioning one shard node and returns the ticket naming the node that will join.
    /// May take as long as instance creation honestly takes — the hub is not waiting on this call
    /// for the node to be ready, only for the promise; readiness is the node registering itself.
    /// </summary>
    Task<ProvisionTicket> RequestNodeAsync(CapacityRequest request, CancellationToken ct);

    /// <summary>
    /// Releases the instance backing the named node. Called only for a node membership confirms
    /// owns no shards: the emptiest node after scale-in drained it, or a late arrival whose ticket
    /// already expired. Idempotent by expectation — decommissioning an instance that is already
    /// gone should succeed quietly.
    /// </summary>
    Task DecommissionAsync(string nodeName, CancellationToken ct);
}
