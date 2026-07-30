using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Declarative reducer authorization: <c>[Reducer(Policy = ...)]</c> replaces 24 hand-written
/// guard clauses in the reference workload, and the unpoliced-reducer report makes the forgotten
/// annotation a build artifact instead of a code-review question.
/// </summary>
public class ReducerAuthTests
{
    [Fact]
    public async Task A_policied_reducer_denies_non_admins_and_admits_admins_with_no_guard_clause()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("AddAdmin", TestTokens.IdentityOf("admin-1"));
        host.Call("SpawnCreature", 1f, 1UL);
        var headBefore = host.Engine.Log.HeadLsn;

        await using var player = new RawSocketClient();
        await player.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("mallory"));
        await player.SendAsync(new CallReducerFrame(1, "ClearCreatures", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
        var denied = await player.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.False(denied.Ok);
        Assert.Equal(MelangeErrorCodes.Denied, denied.ErrorCode);

        // Denied before a transaction opened: no log record exists for the attempt.
        Assert.Equal(headBefore, host.Engine.Log.HeadLsn);

        await using var admin = new RawSocketClient();
        await admin.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("admin-1"));
        await admin.SendAsync(new CallReducerFrame(1, "ClearCreatures", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
        var allowed = await admin.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.True(allowed.Ok, allowed.Message);
        Assert.Equal(headBefore + 1, host.Engine.Log.HeadLsn);
    }

    [Fact]
    public async Task Every_client_callable_reducer_has_a_policy_or_appears_in_the_report()
    {
        // The report asserted, so it cannot silently regress: the set of unpoliced reducers is
        // exactly the client-callable descriptors with no Policy attached — nothing more (a
        // policied or scheduled reducer must not be listed, since neither is client-callable)
        // and nothing less (no reducer escapes both).
        await using var host = await TransportTestHost.StartAsync();
        var scheduled = host.Engine.Schema.Tables
            .Where(t => t.Scheduled is not null)
            .Select(t => t.Scheduled!)
            .ToHashSet(StringComparer.Ordinal);
        var expected = host.Reducers.Reducers
            .Where(r => r.Kind == ReducerKind.Standard && r.Policy is null && !scheduled.Contains(r.Name))
            .Select(r => r.Name)
            .OrderBy(n => n, StringComparer.Ordinal);

        Assert.Equal(expected, host.Reducers.UnpolicedReducers.OrderBy(n => n, StringComparer.Ordinal));
        Assert.DoesNotContain("ClearCreatures", host.Reducers.UnpolicedReducers);
        Assert.DoesNotContain("Respawn", host.Reducers.UnpolicedReducers);
        Assert.Contains("Spawn", host.Reducers.UnpolicedReducers);
        foreach (var reducer in host.Reducers.Reducers.Where(r => r.Kind == ReducerKind.Standard && !scheduled.Contains(r.Name)))
            Assert.True(reducer.Policy is not null || host.Reducers.UnpolicedReducers.Contains(reducer.Name));
    }

    [Fact]
    public async Task Fail_mode_refuses_to_start_while_unpoliced_reducers_exist()
    {
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Policies:UnpolicedReducerReport"] = "Fail",
        }));
        Assert.Contains("Spawn", thrown.Message);
    }

    [Fact]
    public async Task Deny_posture_blocks_unpoliced_reducers_for_clients_but_not_in_process_dispatch()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Policies:DefaultReducerPosture"] = "Deny",
        });

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new CallReducerFrame(1, "Noop", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
        var denied = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.False(denied.Ok);
        Assert.Equal(MelangeErrorCodes.Denied, denied.ErrorCode);

        // In-process dispatch is the host's own code; the posture governs clients only.
        host.Call("Noop");
    }

    [Fact]
    public async Task Lifecycle_reducers_answer_unknown_to_clients()
    {
        // ClientConnected/ClientDisconnected exist server-side only. "Unknown" rather than
        // "forbidden", so a probing client cannot even confirm they exist.
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new CallReducerFrame(1, "OnConnect", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.False(result.Ok);
        Assert.Equal(MelangeErrorCodes.UnknownReducer, result.ErrorCode);
    }
}
