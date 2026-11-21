using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The transient rejection contract (#22): a refusal for a condition the system designed and
/// expects to clear — a handoff freeze, a border copy just after the map flips, a fenced node —
/// reaches the client as the typed 'transient' code carrying the precise reason, never as
/// 'internal' with a server error log per occurrence.
/// </summary>
public class TransientRejectionTests
{
    [Fact]
    public async Task A_transient_rejection_reaches_the_client_typed_with_its_precise_reason()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(
            new CallReducerFrame(1, "RefuseTransiently", ReducerArgs.Encode([]), null) { Channel = MelangeChannels.Calls },
            TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.False(result.Ok);
        Assert.Equal(MelangeErrorCodes.Transient, result.ErrorCode);
        // The precise reason travels — not "The reducer failed; see the server logs."
        Assert.Contains("frozen mid-handoff", result.Message);
    }

    [Fact]
    public async Task The_managed_client_names_the_retry_contract()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var thrown = await Assert.ThrowsAsync<MelangeCallException>(
            () => client.CallReducerAsync("RefuseTransiently", null, TestContext.Current.CancellationToken));
        Assert.True(thrown.IsTransient);
        Assert.Equal(MelangeErrorCodes.Transient, thrown.Code);
    }

    private sealed class LostLeaseBeforeAcknowledging : MelangeDB.Core.ICommitGuard
    {
        public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, MelangeDB.Core.CommitOrigin origin)
        {
        }

        public void ValidateAcknowledgement(string reducerName, MelangeDB.Core.CommitOrigin origin) =>
            throw new CommitOutcomeUnknownException($"'{reducerName}' was written, but the lease expired before it could be acknowledged.");
    }

    /// <summary>
    /// Phase 30 step 2a: a commit that became durable after the node lost its lease is neither a
    /// success nor a transient refusal, because a transient refusal promises nothing was applied.
    /// It reaches the client as its own code, and the managed client names it.
    /// </summary>
    [Fact]
    public async Task An_unacknowledgeable_commit_reaches_the_client_as_outcome_unknown_not_transient()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Engine.AddCommitGuard(new LostLeaseBeforeAcknowledging());
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var headBefore = host.Engine.Log.HeadLsn;

        var thrown = await Assert.ThrowsAsync<MelangeCallException>(
            () => client.CallReducerAsync("Spawn", ["Bob", 1], TestContext.Current.CancellationToken));

        Assert.True(thrown.IsOutcomeUnknown);
        Assert.False(thrown.IsTransient);
        Assert.Contains("lease expired", thrown.Message);
        Assert.Equal(headBefore + 1, host.Engine.Log.HeadLsn); // It was written all the same.
    }
}
