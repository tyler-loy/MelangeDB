using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Phase 30 step 2a, the shard's half: the commit guard re-checks the lease after durability. A
/// node that paused between the pre-append check and the acknowledgement may have been declared
/// dead, and the shard's new owner may have cut the log before the record, so a lapsed lease turns
/// the acknowledgement into "outcome unknown" for every origin.
/// </summary>
public class AcknowledgementLeaseTests
{
    private static ShardCommitGuard Guard(Func<bool> leaseValid) => new(
        schema: null!,
        store: null!,
        new ShardKey(7),
        strategy: null,
        spanCheckEnabled: static () => false,
        leaseValid,
        frozenRows: static () => new HashSet<(TableId, RowKey)>(),
        borrowedOwner: static (_, _) => null);

    [Theory]
    [InlineData(CommitOrigin.Reducer)]
    [InlineData(CommitOrigin.Bulk)]
    [InlineData(CommitOrigin.Internal)]
    public void A_lapsed_lease_withholds_the_acknowledgement_for_every_origin(CommitOrigin origin)
    {
        var lease = true;
        var guard = Guard(() => lease);

        guard.ValidateAcknowledgement("SpawnMob", origin); // Live: acknowledged.

        lease = false;
        var withheld = Assert.Throws<CommitOutcomeUnknownException>(() => guard.ValidateAcknowledgement("SpawnMob", origin));
        Assert.Contains("SpawnMob", withheld.Message);
        Assert.Contains("shard:7", withheld.Message, StringComparison.OrdinalIgnoreCase);
    }
}
