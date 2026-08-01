using System.Diagnostics;
using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The frame-tick pump (issue #26): under <see cref="DispatchMode.Manual"/> the client defers
/// whole data frames — cache mutation and events together, so a handler's world always matches
/// its event — and <see cref="MelangeClient.FrameTick"/> applies them on the caller's own
/// thread. The default stays Immediate, byte-for-byte the pre-pump behaviour.
/// </summary>
public class FrameTickPumpTests
{
    [Fact]
    public async Task Manual_mode_defers_frames_and_a_tick_applies_them_on_the_ticking_thread()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // The initial set arrives on the wire (BytesReceived counts it) yet the subscription
        // completes only once a tick applies it — awaiting it without ticking would hang.
        var bytesBeforeSubscribe = client.BytesReceived;
        var subscribeTask = client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => client.BytesReceived > bytesBeforeSubscribe, "the initial set to arrive");
        Assert.False(subscribeTask.IsCompleted, "the initial set must not apply before a tick");
        var subscription = await TickUntilAsync(client, subscribeTask, "the subscription to apply");

        var events = new List<(long Id, int Thread)>();
        subscription.OnInsert += row => events.Add(((long)row.Columns["Id"]!, Environment.CurrentManagedThreadId));

        // The cursor advances at receive time — the queued frame is retained in-process — which
        // is exactly the observable proof that the frame arrived yet nothing applied.
        var lsn = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delta to be received and retained");
        Assert.Equal(0, subscription.Count);
        Assert.Empty(events);

        var tickThread = Environment.CurrentManagedThreadId;
        Assert.Equal(1, client.FrameTick());
        Assert.Equal(1, subscription.Count);
        var fired = Assert.Single(events);
        Assert.Equal(1L, fired.Id);
        Assert.Equal(tickThread, fired.Thread);
    }

    [Fact]
    public async Task The_default_is_immediate_dispatch_and_FrameTick_refuses_it_loudly()
    {
        await using var host = await TransportTestHost.StartAsync();
        var options = new MelangeClientOptions { Uri = host.WsUri, Token = TestTokens.Default };
        Assert.Equal(DispatchMode.Immediate, options.Dispatch);

        // Unchanged behaviour: events and cache updates arrive with no tick anywhere.
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        var inserts = 0;
        subscription.OnInsert += _ => Interlocked.Increment(ref inserts);
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => subscription.Count == 1 && Volatile.Read(ref inserts) == 1, "the delta to apply immediately");

        // A tick against an Immediate client is a misconfiguration and must be loud.
        Assert.Throws<InvalidOperationException>(() => client.FrameTick());
    }

    [Fact]
    public async Task A_multi_table_commit_is_one_frame_and_is_never_half_applied_by_a_budgeted_tick()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var players = await SubscribeAsync(client, "SELECT * FROM PlayerState");
        var skills = await SubscribeAsync(client, "SELECT * FROM Skill");

        var lsn = host.Call("SpawnWithSkill", "Alice", 1, 7L, "mining");
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the commit to be received");

        // One commit touching two tables is one frame; a tick budgeted to a single frame must
        // deliver the whole transaction — both rows — never a torn half.
        Assert.Equal(1, client.FrameTick(maxFrames: 1));
        Assert.Equal(1, players.Count);
        Assert.Equal(1, skills.Count);
        Assert.Equal(0, client.FrameTick());
    }

    [Fact]
    public async Task The_maxFrames_budget_stops_between_frames_and_a_later_tick_resumes()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await SubscribeAsync(client, "SELECT * FROM Chunk");

        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 2L, new byte[] { 2 });
        var head = host.Call("SetChunk", 3L, 3L, new byte[] { 3 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "all three commits to be received");

        Assert.Equal(2, client.FrameTick(maxFrames: 2));
        Assert.Equal(2, subscription.Count);
        Assert.False(subscription.TryGetRow(RowKeyOf(3L), out _), "the third frame must wait for the next tick");

        Assert.Equal(1, client.FrameTick());
        Assert.Equal(3, subscription.Count);
    }

    /// <summary>
    /// The skew case from the issue: an OnInsert handler doing a cross-table lookup must see a
    /// world consistent with its event — not the newer world of a frame that has already been
    /// received but not yet applied. Deferring events without deferring cache mutation would
    /// fail exactly this.
    /// </summary>
    [Fact]
    public async Task An_insert_handlers_cross_table_lookup_sees_the_world_of_its_own_frame_not_a_newer_one()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var players = await SubscribeAsync(client, "SELECT * FROM PlayerState");
        var skills = await SubscribeAsync(client, "SELECT * FROM Skill");

        var skillsSeenAtSpawn = -1;
        var playersSeenAtSpawn = -1;
        players.OnInsert += _ =>
        {
            skillsSeenAtSpawn = skills.Count;
            playersSeenAtSpawn = players.Count;
        };

        host.Call("Spawn", "Alice", 1);
        var head = host.Call("AddSkill", 7L, "mining", 10L, 1);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "both commits to be received");

        Assert.Equal(2, client.FrameTick());
        Assert.Equal(0, skillsSeenAtSpawn);
        Assert.Equal(1, playersSeenAtSpawn);
        Assert.Equal(1, skills.Count);
    }

    [Fact]
    public async Task Queue_overflow_aborts_loudly_with_the_error_at_the_head_and_resume_recovers_everything()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o =>
        {
            o.Dispatch = DispatchMode.Manual;
            o.DispatchQueueLimit = 8;
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await SubscribeAsync(client, "SELECT * FROM Chunk");

        var order = new List<string>();
        var errors = new List<ErrorFrame>();
        subscription.OnInsert += row => order.Add($"+{(long)row.Columns["Id"]!}");
        client.OnError += error =>
        {
            errors.Add(error);
            order.Add("error");
        };
        client.OnDisconnected += () => order.Add("disconnected");

        // Ten commits against a limit of eight: the ninth received frame overflows, the client
        // synthesizes its own error and aborts the socket — never blocking the receive loop,
        // never dropping a delta silently.
        var lsns = new ulong[10];
        for (var i = 0; i < 10; i++)
            lsns[i] = host.Call("SetChunk", (long)(i + 1), (long)i, new byte[] { (byte)i });
        await TransportTestHost.WaitUntilAsync(() => !client.IsConnected, "the overflow to abort the socket");

        // Nothing applied yet, and the cursor stopped at the last retained frame — the dropped
        // frames were never acked, which is what lets resume replay them.
        Assert.Equal(0, subscription.Count);
        Assert.Equal(lsns[7], client.LastAckedLsn);

        // The synthesized error is the head of the queue: the first tick learns the connection
        // died before spending budget on the retained backlog.
        Assert.Equal(1, client.FrameTick(maxFrames: 1));
        Assert.Equal(["error"], order);
        Assert.Equal(MelangeErrorCodes.DispatchOverflow, Assert.Single(errors).Code);

        // Ticking resumes — the app is back — and the retained backlog drains: the eight kept
        // frames in commit order, then the disconnect that trailed them.
        await TickUntilAsync(client, () => order.Count == 10, "the retained backlog to drain");
        Assert.Equal(["error", "+1", "+2", "+3", "+4", "+5", "+6", "+7", "+8", "disconnected"], order);

        // Recovery is the ordinary resume path: the replayed gap picks up exactly at the first
        // dropped frame — ten inserts total, in commit order, nothing lost and nothing doubled.
        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "the gap should be servable from the log");
        await TickUntilAsync(client, () => subscription.Count == 10, "the resumed gap to drain");
        Assert.Equal(
            ["+1", "+2", "+3", "+4", "+5", "+6", "+7", "+8", "+9", "+10"],
            order.Where(entry => entry.StartsWith('+')));
        Assert.Equal(0, subscription.Inconsistencies);
    }

    [Fact]
    public async Task Resume_keeps_the_queue_so_buffered_frames_apply_before_the_replayed_gap()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await SubscribeAsync(client, "SELECT * FROM Chunk");
        var inserts = new List<long>();
        subscription.OnInsert += row => inserts.Add((long)row.Columns["Id"]!);

        // One commit is received and queued but never ticked; then the socket dies and two more
        // commits happen during the outage.
        var buffered = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= buffered, "the first commit to be queued");
        client.Abort();
        host.Call("SetChunk", 2L, 2L, new byte[] { 2 });
        var head = host.Call("SetChunk", 3L, 3L, new byte[] { 3 });

        // Resume keeps the queue: the buffered frame applies before the replayed gap, so the
        // handler hears history in order with no gap and no duplicate.
        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "the gap should be servable from the log");
        await TickUntilAsync(client, () => client.LastAckedLsn >= head && subscription.Count == 3, "the queue and the gap to drain");
        Assert.Equal([1L, 2L, 3L], inserts);
        Assert.Equal(0, subscription.Inconsistencies);
    }

    [Fact]
    public async Task A_full_resync_clears_the_queue_and_a_stale_era_frame_never_touches_the_fresh_caches()
    {
        await using var host = await TransportTestHost.StartAsync();
        var sawFreshInitialSet = false;
        var armed = false;
        await using var client = host.CreateClient(o =>
        {
            o.Dispatch = DispatchMode.Manual;
            o.FrameInspector = (frame, _) =>
            {
                if (Volatile.Read(ref armed) && frame is SubscriptionAppliedFrame)
                    Volatile.Write(ref sawFreshInitialSet, true);
            };
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await SubscribeAsync(client, "SELECT * FROM Chunk");
        var inserts = new List<long>();
        subscription.OnInsert += row => inserts.Add((long)row.Columns["Id"]!);

        // A commit from the old era is received and queued, never ticked. Then the log itself
        // dies: a fresh incarnation, a different epoch, a different world.
        var stale = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= stale, "the stale commit to be queued");
        client.Abort();
        await host.RestartAsync(freshLog: true);
        host.Call("SetChunk", 40L, 4L, new byte[] { 40 });

        // Hold every tick until the fresh initial set is on the wire — by then the resync has
        // already cleared the queue, so the stale frame provably sat in it when the clear ran.
        Volatile.Write(ref armed, true);
        var reconnectTask = client.ReconnectAsync(TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => Volatile.Read(ref sawFreshInitialSet), "the fresh initial set to arrive");
        await TickUntilAsync(client, () => reconnectTask.IsCompleted, "the re-establishment to apply");
        Assert.False(await reconnectTask, "an unknown epoch must answer full resync");

        // The stale frame from the dead era must never have applied against the reset cache:
        // no insert event ever fired for it, and only the fresh world's row exists.
        while (client.FrameTick() > 0)
        {
        }

        Assert.Empty(inserts);
        Assert.Equal(1, subscription.Count);
        Assert.True(subscription.TryGetRow(RowKeyOf(40L), out _));
        Assert.False(subscription.TryGetRow(RowKeyOf(1L), out _));
        Assert.Equal(0, subscription.Inconsistencies);
    }

    [Fact]
    public async Task Lifecycle_events_join_the_queue_and_fire_in_order_on_the_ticking_thread()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await SubscribeAsync(client, "SELECT * FROM Chunk");

        var tickThread = 0;
        var order = new List<(string Event, bool OnTickThread)>();
        subscription.OnInsert += row =>
            order.Add(($"+{(long)row.Columns["Id"]!}", Environment.CurrentManagedThreadId == Volatile.Read(ref tickThread)));
        client.OnDisconnected += () =>
            order.Add(("disconnected", Environment.CurrentManagedThreadId == Volatile.Read(ref tickThread)));

        // A delta is queued, then the socket dies: the disconnect must be told after the frame
        // that was received before it — a handler never learns of the outage before the state
        // that preceded it.
        var lsn = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delta to be queued");
        client.Abort();
        Assert.Empty(order);

        var deadline = TestTime.Dilated(TimeSpan.FromSeconds(15));
        var stopwatch = Stopwatch.StartNew();
        while (order.Count < 2)
        {
            Assert.True(stopwatch.Elapsed < deadline, "Timed out waiting for the disconnect to be ticked out");
            Volatile.Write(ref tickThread, Environment.CurrentManagedThreadId);
            client.FrameTick();
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Equal(["+1", "disconnected"], order.Select(entry => entry.Event));
        Assert.All(order, entry => Assert.True(entry.OnTickThread, $"{entry.Event} fired off the ticking thread"));
    }

    [Fact]
    public async Task A_concurrent_FrameTick_throws_because_the_pump_is_single_consumer()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Dispatch = DispatchMode.Manual);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await SubscribeAsync(client, "SELECT * FROM Chunk");

        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        subscription.OnInsert += _ =>
        {
            entered.Set();
            release.Wait(TestContext.Current.CancellationToken);
        };

        var lsn = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delta to be queued");

        // One tick is held mid-handler on another thread; a second tick must refuse rather than
        // interleave — the pump is single-consumer by contract.
        var ticking = Task.Run(() => client.FrameTick());
        Assert.True(
            entered.Wait(TestTime.Dilated(TimeSpan.FromSeconds(15)), TestContext.Current.CancellationToken),
            "the held tick never reached the handler");
        Assert.Throws<InvalidOperationException>(() => client.FrameTick());
        release.Set();
        Assert.Equal(1, await ticking);
    }

    /// <summary>Subscribes on a Manual client, ticking until the initial set applies.</summary>
    private static async Task<MelangeSubscription> SubscribeAsync(MelangeClient client, string query)
    {
        var task = client.SubscribeAsync(query, cancellationToken: TestContext.Current.CancellationToken);
        return await TickUntilAsync(client, task, $"the subscription to apply: {query}");
    }

    private static async Task<T> TickUntilAsync<T>(MelangeClient client, Task<T> task, string what)
    {
        await TickUntilAsync(client, () => task.IsCompleted, what);
        return await task;
    }

    /// <summary>Drives the pump from the test thread until <paramref name="condition"/> holds.</summary>
    private static async Task TickUntilAsync(MelangeClient client, Func<bool> condition, string what)
    {
        var deadline = TestTime.Dilated(TimeSpan.FromSeconds(15));
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(stopwatch.Elapsed < deadline, $"Timed out ticking for: {what}");
            client.FrameTick();
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    private static byte[] RowKeyOf(long id)
    {
        var key = KeyCodec.EncodeInt64(id);
        return key.ToArray();
    }
}
