using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The node link's two frame formats. Row data on a binary link rides in an attachment beside the
/// JSON instead of as base64 inside it; everything a message holds must come out the other end
/// identical either way, a reader must take both kinds from the same peer, and a malformed
/// attachment must fail the frame, never decode into different rows.
/// </summary>
public sealed class NodeLinkFramingTests
{
    private static readonly WireOp[] Ops =
    [
        new(1, 7, [1, 2, 3, 4, 5, 6, 7, 8], Bytes(1024, 1)),
        new(2, 7, Bytes(200, 2), Bytes(70_000, 3)),
        new(3, 9, [9], null),
        new(1, uint.MaxValue, [], []),
        new(1, 0, Bytes(64, 4), Bytes(65, 5)),
    ];

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Every_row_byte_arrives_identical_in_either_format_and_across_them(bool senderBinary, bool receiverBinary)
    {
        await using var pair = await LinkPair.OpenAsync(senderBinary, receiverBinary);
        pair.Receiver.Handler = (_, type, body) => Task.FromResult<object?>(type switch
        {
            "border-apply" => body!.Value.Read<BorderBatch>(),
            "replica-batch" => body!.Value.Read<ReplicaBatch>(),
            "events-forward" => body!.Value.Read<EventsForward>(),
            "shard-bulk" => body!.Value.Read<ShardBulk>(),
            _ => null,
        });

        var batch = new BorderBatch(1, 2, "epoch", 42, Ops);
        var echoed = (await pair.Sender.RequestAsync("border-apply", batch, TestContext.Current.CancellationToken))!.Value.Read<BorderBatch>()!;
        AssertOps(Ops, echoed.Ops);
        Assert.Equal((1UL, 2UL, "epoch", 42UL), (echoed.OwnerShard, echoed.ObserverShard, echoed.Epoch, echoed.UpToLsn));

        var replica = new ReplicaBatch([5, 6, 7], [Ops, [], Ops[..2]], PrevLsn: 4);
        var replicaEcho = (await pair.Sender.RequestAsync("replica-batch", replica, TestContext.Current.CancellationToken))!.Value.Read<ReplicaBatch>()!;
        Assert.Equal(replica.Lsns, replicaEcho.Lsns);
        Assert.Equal(3, replicaEcho.Records.Length);
        AssertOps(Ops, replicaEcho.Records[0]);
        Assert.Empty(replicaEcho.Records[1]);
        AssertOps(Ops[..2], replicaEcho.Records[2]);

        var events = new EventsForward(9, 3, [new WireEvent("Gift", 1, Bytes(300, 6)), new WireEvent("Tiny", 0, [])], [10, 11], [8, 9]);
        var eventsEcho = (await pair.Sender.RequestAsync("events-forward", events, TestContext.Current.CancellationToken))!.Value.Read<EventsForward>()!;
        Assert.Equal(events.Events.Select(static e => (e.EventType, e.Depth)), eventsEcho.Events.Select(static e => (e.EventType, e.Depth)));
        Assert.Equal(events.Events[0].Payload, eventsEcho.Events[0].Payload);
        Assert.Empty(eventsEcho.Events[1].Payload);

        var bulk = new ShardBulk(4, 5, "ab", [new BulkRowDto("T", Bytes(500, 7), ["A", "B"])]);
        var bulkEcho = (await pair.Sender.RequestAsync("shard-bulk", bulk, TestContext.Current.CancellationToken))!.Value.Read<ShardBulk>()!;
        Assert.Equal(bulk.Rows[0].Row, bulkEcho.Rows[0].Row);
        Assert.Equal(bulk.Rows[0].Supplied, bulkEcho.Rows[0].Supplied);
    }

    [Fact]
    public async Task A_binary_frame_carries_rows_without_base64()
    {
        await using var json = await LinkPair.OpenAsync(senderBinary: false, receiverBinary: false);
        await using var binary = await LinkPair.OpenAsync(senderBinary: true, receiverBinary: true);
        var rows = Enumerable.Range(0, 500).Select(static i => new WireOp(1, 7, BitConverter.GetBytes((long)i), Bytes(1024, i))).ToArray();
        var batch = new BorderBatch(1, 2, "epoch", 1, rows);

        await json.Sender.NotifyAsync("border-apply", batch, TestContext.Current.CancellationToken);
        await binary.Sender.NotifyAsync("border-apply", batch, TestContext.Current.CancellationToken);

        var raw = 500L * (1024 + 8);
        var jsonBytes = json.SenderMetrics.SentBytesByType["border-apply"];
        var binaryBytes = binary.SenderMetrics.SentBytesByType["border-apply"];
        Assert.True(jsonBytes > raw * 4 / 3, $"JSON frame {jsonBytes} B for {raw} B of rows");
        Assert.InRange(binaryBytes, raw, raw + 500 * 16 + 512);
    }

    [Fact]
    public async Task A_binary_link_still_refuses_a_frame_over_the_limit_and_stays_up()
    {
        await using var pair = await LinkPair.OpenAsync(senderBinary: true, receiverBinary: true);
        pair.Receiver.Handler = static (_, _, body) => Task.FromResult<object?>(body!.Value.Read<int[]>());

        // Without base64 a 49 MiB array fits; 65 MiB does not.
        var refused = await Assert.ThrowsAsync<NodeLinkException>(() =>
            pair.Sender.RequestAsync("echo", new BulkRowDto("T", new byte[65 * 1024 * 1024], []), TestContext.Current.CancellationToken));
        Assert.Contains("exceeds the node link's", refused.Message);

        var echoed = await pair.Sender.RequestAsync("echo", new[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
        Assert.Equal([1, 2, 3], echoed!.Value.Read<int[]>()!);
        Assert.True(pair.Sender.IsAlive && pair.Receiver.IsAlive);
    }

    [Theory]
    [InlineData("""{"OwnerShard":1,"ObserverShard":2,"Epoch":"e","UpToLsn":1,"Ops":999}""", new byte[] { 1, 0 })]
    [InlineData("""{"OwnerShard":1,"ObserverShard":2,"Epoch":"e","UpToLsn":1,"Ops":0}""", new byte[] { 2, 1, 7, 0, 0, 0, 200 })]
    [InlineData("""{"OwnerShard":1,"ObserverShard":2,"Epoch":"e","UpToLsn":1,"Ops":0}""", new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
    [InlineData("""{"OwnerShard":1,"ObserverShard":2,"Epoch":"e","UpToLsn":1,"Ops":-1}""", new byte[] { 0 })]
    public void A_malformed_attachment_fails_the_decode(string json, byte[] attachment)
    {
        var frame = new byte[1 + attachment.Length];
        attachment.CopyTo(frame, 1);
        var body = new LinkBody(JsonDocument.Parse(json).RootElement, frame, attachmentStart: 1);

        Assert.ThrowsAny<JsonException>(() => body.Read<BorderBatch>());
    }

    [Fact]
    public void A_json_body_names_no_attachment_and_cannot_point_into_one()
    {
        var body = new LinkBody(JsonDocument.Parse("""{"OwnerShard":1,"ObserverShard":2,"Epoch":"e","UpToLsn":1,"Ops":0}""").RootElement);

        Assert.Throws<JsonException>(() => body.Read<BorderBatch>());
    }

    private static void AssertOps(WireOp[] expected, WireOp[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Kind, actual[i].Kind);
            Assert.Equal(expected[i].Table, actual[i].Table);
            Assert.Equal(expected[i].Key, actual[i].Key);
            Assert.Equal(expected[i].Row, actual[i].Row);
        }
    }

    private static byte[] Bytes(int length, int seed)
    {
        var bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    private sealed class LinkPair : IAsyncDisposable
    {
        private readonly TcpClient _client;
        private readonly TcpClient _accepted;

        private LinkPair(TcpClient client, TcpClient accepted, bool senderBinary, bool receiverBinary)
        {
            _client = client;
            _accepted = accepted;
            Sender = new NodeLink(client, SenderMetrics) { SendsBinary = senderBinary };
            Receiver = new NodeLink(accepted, new ClusterMetrics()) { SendsBinary = receiverBinary };
            Receiver.Handler = static (_, _, _) => Task.FromResult<object?>(null);
        }

        public ClusterMetrics SenderMetrics { get; } = new();

        public NodeLink Sender { get; }

        public NodeLink Receiver { get; }

        public static async Task<LinkPair> OpenAsync(bool senderBinary, bool receiverBinary)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, TestContext.Current.CancellationToken);
            var accepted = await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken);
            listener.Stop();
            var pair = new LinkPair(client, accepted, senderBinary, receiverBinary);
            pair.Sender.Start();
            pair.Receiver.Start();
            return pair;
        }

        public ValueTask DisposeAsync()
        {
            Sender.Dispose();
            Receiver.Dispose();
            _client.Dispose();
            _accepted.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
