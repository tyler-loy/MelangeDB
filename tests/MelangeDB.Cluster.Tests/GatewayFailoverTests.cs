using System.Net;
using System.Net.Sockets;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// #200: a shard owner whose machine dies, as the gateway sees it. A dead process refuses
/// connections; a dead machine answers nothing, so a connect to it hangs until the OS gives up
/// (about 21 s on Windows, about two minutes on stock Linux). The gateway's upstream connect had no
/// bound, and the attempt held the client's attachment lock, so every shard-bound call that client
/// made hung behind it — past the reassignment, which the gateway never got to retry towards.
/// </summary>
public class GatewayFailoverTests
{
    [Fact]
    public async Task A_call_to_a_shard_whose_machine_died_completes_on_the_new_owner_instead_of_hanging()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 1_500);
        var ct = TestContext.Current.CancellationToken;
        var owner = await cluster.EnsureShardOwnedAsync(14);
        await using var client = cluster.CreateClient(TestTokens.For("caller-on-a-dead-machine"));
        await client.ConnectAsync(ct);
        await client.CallReducerAsync("SetLocation", [14u], ct);
        await client.CallReducerAsync("SpawnMob", [14u, 1], ct); // Attached to the owner now.

        // The machine dies: the process goes, and in its place on the same port is a listener that
        // completes TCP handshakes (the kernel's backlog does that) and never says another word —
        // the connect succeeds and the websocket upgrade waits forever, which is what an unbounded
        // client sees from a host that has stopped answering.
        await cluster.StopNodeAsync(owner.Name);
        using var silent = new TcpListener(IPAddress.Loopback, owner.HttpPort);
        silent.Start(backlog: 64);

        var call = client.CallReducerAsync("SpawnMob", [14u, 2], ct);
        var finished = await Task.WhenAny(call, Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(30)), ct));
        Assert.True(ReferenceEquals(call, finished), "the call hung on the dead owner's address instead of following the reassignment");
        Assert.True(await call > 0);

        var newOwner = cluster.Hub.Membership.GetAssignment(new ShardKey(14))!.NodeName;
        Assert.NotEqual(owner.Name, newOwner);
        Assert.Equal(2, cluster.ShardOf(14).Engine.CommittedView.Count<Mob>());
    }
}
