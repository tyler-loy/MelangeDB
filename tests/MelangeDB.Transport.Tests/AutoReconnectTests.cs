using System.Collections.Concurrent;
using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Automatic reconnect (road-to-0.4 phase 28): with a <see cref="ReconnectPolicy"/> set, a dropped
/// connection comes back on its own through the ordinary resume path, its caches and subscriptions
/// intact; it backs off while the server is gone, gives up when told to, stops when disposed, and
/// never comes back as somebody else.
/// </summary>
public class AutoReconnectTests
{
    private static readonly ReconnectPolicy Fast = new()
    {
        InitialDelay = TimeSpan.FromMilliseconds(20),
        MaxDelay = TimeSpan.FromMilliseconds(200),
        Jitter = 0,
        AttemptTimeout = TimeSpan.FromSeconds(5),
    };

    private sealed class ChunkCodec : IClientRowCodec<Chunk>
    {
        public static readonly ChunkCodec Instance = new();

        public string TableName => "Chunk";

        public IReadOnlyList<WireColumn> Columns { get; } =
        [
            new WireColumn("Id", ColumnKind.Int64),
            new WireColumn("X", ColumnKind.Int64),
            new WireColumn("Data", ColumnKind.Bytes),
        ];

        public Chunk DecodeRow(ReadOnlySpan<byte> data)
        {
            var reader = new RowReader(data);
            return new Chunk { Id = reader.ReadInt64(), X = reader.ReadInt64(), Data = reader.ReadBytes()! };
        }

        public byte[] EncodePrimaryKey(in Chunk row) => KeyCodec.EncodeInt64(row.Id).ToArray();
    }

    /// <summary>Every lifecycle event, in order.</summary>
    private sealed class Lifecycle
    {
        public ConcurrentQueue<string> Events { get; } = new();

        public Exception? Failure { get; private set; }

        public void Attach(MelangeClient client)
        {
            client.OnDisconnected += () => Events.Enqueue("disconnected");
            client.OnReconnecting += attempt => Events.Enqueue($"reconnecting {attempt.Attempt}");
            client.OnReconnected += resumed => Events.Enqueue(resumed ? "resumed" : "reestablished");
            client.OnReconnectFailed += failure =>
            {
                Failure = failure;
                Events.Enqueue("failed");
            };
        }

        public int Count(string prefix) => Events.Count(e => e.StartsWith(prefix, StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_dropped_connection_comes_back_on_its_own_with_its_cache_and_subscriptions()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Reconnect = Fast);
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var cache = registry.GetOrAdd(ChunkCodec.Instance);
        await registry.SubscribeAsync(ChunkCodec.Instance, "SELECT * FROM Chunk", null, TestContext.Current.CancellationToken);
        var lsn = host.Call("SetChunk", 1L, 7L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the first row");

        client.Abort();
        await TransportTestHost.WaitUntilAsync(() => lifecycle.Count("resumed") + lifecycle.Count("reestablished") == 1, "the reconnect");

        // The same subscription, still live: a write after the drop reaches the same cache.
        lsn = host.Call("SetChunk", 2L, 8L, new byte[] { 2 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn && cache.Count == 2, "a row written after the reconnect");
        Assert.True(client.IsConnected);
        Assert.False(client.IsReconnecting);
        Assert.Equal(["disconnected", "reconnecting 1", "resumed"], lifecycle.Events);
    }

    [Fact]
    public async Task A_server_restart_is_ridden_out_and_calls_work_after()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Reconnect = Fast);
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Spawn", ["Survivor", 1], TestContext.Current.CancellationToken);

        await host.RestartAsync();
        await TransportTestHost.WaitUntilAsync(() => lifecycle.Count("resumed") + lifecycle.Count("reestablished") == 1, "the reconnect after restart");

        await client.CallReducerAsync("Move", [3f], TestContext.Current.CancellationToken);
        Assert.Equal(1, lifecycle.Count("disconnected"));
        Assert.Equal(0, lifecycle.Count("failed"));
    }

    [Fact]
    public async Task It_gives_up_after_MaxAttempts_when_the_server_stays_gone()
    {
        var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.Reconnect = new ReconnectPolicy
        {
            InitialDelay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(20),
            MaxAttempts = 3,
            AttemptTimeout = TimeSpan.FromSeconds(2),
        });
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await host.DisposeAsync();
        await TransportTestHost.WaitUntilAsync(() => lifecycle.Count("failed") == 1, "the give-up");

        Assert.Equal(3, lifecycle.Count("reconnecting"));
        Assert.Equal(1, lifecycle.Count("disconnected"));
        Assert.False(client.IsReconnecting);
        Assert.False(client.IsConnected);
        Assert.Contains("Gave up reconnecting after 3 attempt(s)", lifecycle.Failure!.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_token_provider_supplies_every_dial()
    {
        await using var host = await TransportTestHost.StartAsync();
        var dials = 0;
        await using var client = host.CreateClient(o =>
        {
            o.Token = null;
            o.Reconnect = Fast;
            o.TokenProvider = _ =>
            {
                Interlocked.Increment(ref dials);
                return ValueTask.FromResult(TestTokens.Default);
            };
        });
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref dials));

        client.Abort();
        await TransportTestHost.WaitUntilAsync(() => client.IsConnected && !client.IsReconnecting, "the reconnect");

        Assert.Equal(2, Volatile.Read(ref dials));
        Assert.Equal(TransportTestHost.Caller, client.Identity);
    }

    [Fact]
    public async Task A_reconnect_never_comes_back_as_somebody_else()
    {
        await using var host = await TransportTestHost.StartAsync();
        var first = true;
        await using var client = host.CreateClient(o =>
        {
            o.Token = null;
            o.Reconnect = Fast;
            o.TokenProvider = _ =>
            {
                var token = first ? TestTokens.Default : TestTokens.For("someone-else");
                first = false;
                return ValueTask.FromResult(token);
            };
        });
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        client.Abort();
        await TransportTestHost.WaitUntilAsync(() => lifecycle.Count("failed") == 1, "the refusal");

        var refusal = Assert.IsType<MelangeCallException>(lifecycle.Failure);
        Assert.Equal(MelangeErrorCodes.IdentityChanged, refusal.Code);
        Assert.Equal(TransportTestHost.Caller, client.Identity);
        Assert.False(client.IsConnected);
        Assert.Equal(1, lifecycle.Count("reconnecting"));
    }

    [Fact]
    public async Task A_refused_attempt_whose_socket_dies_after_the_give_up_starts_nothing()
    {
        // The race CI found: the identity refusal aborts the attempt's socket and gives up, and
        // that socket's teardown runs on its own thread. When the teardown lost the race to the
        // give-up, it saw no reconnect in progress and started a fresh loop — a client that had
        // given up came back to try again. The gate holds that teardown until the give-up has run.
        await using var host = await TransportTestHost.StartAsync();
        var first = true;
        await using var client = host.CreateClient(o =>
        {
            o.Token = null;
            o.Reconnect = Fast;
            o.TokenProvider = _ =>
            {
                var token = first ? TestTokens.Default : TestTokens.For("someone-else");
                first = false;
                return ValueTask.FromResult(token);
            };
        });
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        var gaveUp = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.OnReconnectFailed += _ => gaveUp.TrySetResult();

        // Armed once the original socket's teardown has consumed nothing: the next loop to die is
        // the refused attempt's, and it is held until the give-up is done.
        client.OnReconnecting += attempt =>
        {
            if (attempt.Attempt == 1)
                client.ReceiveTeardownGate = () => gaveUp.Task;
        };
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        client.Abort();
        await gaveUp.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        await Task.Delay(300, TestContext.Current.CancellationToken);

        Assert.Equal(["disconnected", "reconnecting 1", "failed"], lifecycle.Events);
        Assert.False(client.IsReconnecting);
    }

    [Fact]
    public async Task Without_a_policy_nothing_reconnects()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        client.Abort();
        await TransportTestHost.WaitUntilAsync(() => lifecycle.Count("disconnected") == 1, "the disconnect");
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(client.IsConnected);
        Assert.False(client.IsReconnecting);
        Assert.Equal(["disconnected"], lifecycle.Events);
    }

    [Fact]
    public async Task Disposing_stops_a_reconnect_that_would_otherwise_run_forever()
    {
        var host = await TransportTestHost.StartAsync();
        var client = host.CreateClient(o => o.Reconnect = Fast);
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await host.DisposeAsync();
        await TransportTestHost.WaitUntilAsync(() => lifecycle.Count("reconnecting") >= 2, "the retries to start");

        await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.False(client.IsReconnecting);
        var settled = lifecycle.Count("reconnecting");
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.Equal(settled, lifecycle.Count("reconnecting"));
        Assert.Equal(0, lifecycle.Count("failed"));
    }

    [Fact]
    public async Task Under_Manual_dispatch_the_lifecycle_events_fire_from_FrameTick()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o =>
        {
            o.Reconnect = Fast;
            o.Dispatch = DispatchMode.Manual;
        });
        var lifecycle = new Lifecycle();
        lifecycle.Attach(client);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        client.Abort();
        await TransportTestHost.WaitUntilAsync(() => client.IsConnected && !client.IsReconnecting, "the reconnect");
        Assert.Empty(lifecycle.Events);

        client.FrameTick();

        Assert.Equal(["disconnected", "reconnecting 1", "reestablished"], lifecycle.Events);
    }

    [Fact]
    public void The_backoff_grows_by_the_multiplier_to_the_cap_with_jitter_only_taking_time_off()
    {
        var policy = new ReconnectPolicy
        {
            InitialDelay = TimeSpan.FromMilliseconds(100),
            MaxDelay = TimeSpan.FromSeconds(1),
            Multiplier = 2,
            Jitter = 0,
        };

        Assert.Equal(
            [100, 200, 400, 800, 1000, 1000],
            Enumerable.Range(1, 6).Select(a => policy.DelayFor(a, Random.Shared).TotalMilliseconds));

        policy.Jitter = 0.5;
        var jittered = Enumerable.Range(0, 200).Select(_ => policy.DelayFor(3, Random.Shared).TotalMilliseconds).ToArray();
        Assert.All(jittered, d => Assert.InRange(d, 200, 400));
        Assert.True(jittered.Distinct().Count() > 1);
    }
}
