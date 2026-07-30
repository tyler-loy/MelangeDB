using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Resume, not refetch: a reconnecting client names its log epoch and last acked LSN and receives
/// only the deltas it missed — asserted by measuring bytes, since the saving is the whole point.
/// Every path where the gap cannot be served answers an explicit full resync, never a partial or
/// guessed answer.
/// </summary>
public class ResumeTests
{
    [Fact]
    public async Task Reconnect_resumes_from_the_acked_lsn_without_refetching_the_initial_set()
    {
        await using var host = await TransportTestHost.StartAsync();
        for (var i = 0L; i < 50; i++)
            host.Call("SetChunk", i, i % 16, new byte[4096]);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 0L, ["hi"] = 15L },
            TestContext.Current.CancellationToken);
        Assert.Equal(50, subscription.Count);
        var initialSetBytes = client.BytesReceived;

        // A network blip: no close frame, just death. Writes continue while the client is gone.
        client.Abort();
        host.Call("SetChunk", 1L, 1L, new byte[] { 0xAA });
        host.Call("SetChunk", 2L, 2L, new byte[] { 0xBB });
        var head = host.Call("DeleteChunk", 3L);

        var bytesBeforeResume = client.BytesReceived;
        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "the server should have served the gap from the log");
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the gap replay to drain");

        Assert.Equal(49, subscription.Count);
        Assert.True(subscription.TryGetRow(RowKeyOf(1L), out var row));
        Assert.Equal(new byte[] { 0xAA }, (byte[])row.Columns["Data"]!);
        Assert.Equal(0, subscription.Inconsistencies);

        // The saving, measured: the gap must cost a tiny fraction of the ~1MB initial set.
        var resumeBytes = client.BytesReceived - bytesBeforeResume;
        Assert.True(
            resumeBytes < initialSetBytes / 10,
            $"resume transferred {resumeBytes} bytes against an initial set of {initialSetBytes}");
    }

    [Fact]
    public async Task Disconnecting_past_the_retention_window_answers_full_resync()
    {
        await using var host = await TransportTestHost.StartAsync(manualTime: true);
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);

        client.Abort();
        host.Call("SetChunk", 2L, 2L, new byte[] { 2 });
        var head = host.Call("SetChunk", 3L, 3L, new byte[] { 3 });

        // The outage outlives Resume:RetentionWindowSeconds (300): the missed records are too old
        // to serve, so the server must demand a fresh initial set rather than silently diverge.
        host.Time!.Advance(TimeSpan.FromSeconds(400));

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.False(resumed, "a gap older than the retention window must degrade to full resync");
        Assert.Equal(3, subscription.Count);
        Assert.True(subscription.TryGetRow(RowKeyOf(3L), out _));
        _ = head;
    }

    [Fact]
    public async Task Resume_within_the_retention_window_is_served_after_a_pause()
    {
        await using var host = await TransportTestHost.StartAsync(manualTime: true);
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);

        client.Abort();
        var head = host.Call("SetChunk", 2L, 2L, new byte[] { 2 });
        host.Time!.Advance(TimeSpan.FromSeconds(100));

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the gap replay to drain");
        Assert.Equal(2, subscription.Count);
    }

    [Fact]
    public async Task A_stale_or_unknown_log_epoch_fails_cleanly_into_full_resync()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        var oldEpoch = client.LogEpochId;
        client.Abort();

        // The log is wiped and re-created: a different incarnation entirely. The client's cursor
        // names an epoch this server has never seen — phase 09's handoff relies on exactly this
        // answer being a clean failure, never a guess.
        await host.RestartAsync(freshLog: true);
        host.Call("SetChunk", 40L, 4L, new byte[] { 40 });
        host.Call("SetChunk", 41L, 4L, new byte[] { 41 });

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.False(resumed, "an unknown epoch must answer full resync");
        Assert.NotEqual(oldEpoch, client.LogEpochId);
        Assert.Equal(2, subscription.Count);
        Assert.True(subscription.TryGetRow(RowKeyOf(40L), out _));
        Assert.False(subscription.TryGetRow(RowKeyOf(1L), out _));
    }

    [Fact]
    public async Task The_epoch_survives_a_server_restart_so_resume_works_across_it()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        var epoch = client.LogEpochId;

        // The server process dies and comes back over the same log. The epoch is durable, so the
        // client's cursor is still meaningful and the gap — including writes committed after the
        // restart — replays from the log.
        await host.RestartAsync(freshLog: false);
        var head = host.Call("SetChunk", 2L, 2L, new byte[] { 2 });

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "the epoch survived, so the gap should be served");
        Assert.Equal(epoch, client.LogEpochId);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the gap replay to drain");
        Assert.Equal(2, subscription.Count);
        Assert.Equal(0, subscription.Inconsistencies);
    }

    [Fact]
    public async Task Killing_and_reconnecting_a_client_restores_subscriptions_and_converges()
    {
        await using var host = await TransportTestHost.StartAsync();
        var alice = Identity.Hash("alice");
        host.Reducers.Call("Spawn", alice, "Alice", 1);
        host.Call("AddSkill", 7L, "mining", 10L, 1);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var players = await client.SubscribeAsync(
            "SELECT * FROM PlayerState WHERE RoomId = :room",
            new Dictionary<string, object?> { ["room"] = 1 },
            TestContext.Current.CancellationToken);
        var skills = await client.SubscribeAsync(
            "SELECT PlayerNum, TotalXp FROM Skill WHERE PlayerNum = :p",
            new Dictionary<string, object?> { ["p"] = 7L },
            TestContext.Current.CancellationToken);

        client.Abort();
        host.Reducers.Call("Spawn", Identity.Hash("bob"), "Bob", 1);
        var head = host.Call("SetSkillXp", 1UL, 500L);

        await client.ReconnectAsync(TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(
            () => players.Count == 2 && skills.Rows.SingleOrDefault() is { } skill && (long)skill.Columns["TotalXp"]! == 500L,
            "both subscriptions to converge");
        Assert.Equal(0, players.Inconsistencies);
        _ = head;
    }

    private static byte[] RowKeyOf(long id)
    {
        var key = Core.KeyCodec.EncodeInt64(id);
        return key.ToArray();
    }
}
