using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using MelangeDB.Cluster;

namespace MelangeDB.LoadTest;

/// <summary>
/// <c>link-bench</c>: the node link alone, over loopback TCP in one process: a sender's requests, a
/// receiver that decodes each body into its message type the way the hub and shard handlers do,
/// and the reply. Per message shape it reports the round trip, the bytes allocated across both
/// ends, the payload bytes on the wire, and the managed heap still held once the run ends and a
/// full collection has run: buffers a pool keeps beside the data, which no budget counts.
/// <para>
/// The shapes are the cluster's own: a border batch of 500 rows of 1 KiB (what a bulk write at a
/// block edge mirrors), one of 50 small rows (a walker band), and a heartbeat. Each runs
/// sequentially (latency) and from 32 concurrent senders (throughput, and what per-thread pooling
/// retains when many pool threads touch the link).
/// </para>
/// </summary>
internal static class LinkBench
{
    private sealed record Shape(string Name, string Type, Func<int, object> Body, int Sequential, int Concurrent);

    public static async Task<int> RunAsync(LoadTestOptions options, TextWriter output, CancellationToken ct)
    {
        Shape[] shapes =
        [
            new("border-500x1KiB", "border-apply", seq => Batch(seq, 500, 1024), 2_000, 4_000),
            new("border-50x64B", "border-apply", seq => Batch(seq, 50, 64), 20_000, 40_000),
            new("heartbeat", "heartbeat", static _ => Heartbeat(), 20_000, 40_000),
        ];

        output.WriteLine($"link-bench: node link over loopback, {options.Repeat} round(s) per shape; process GC: {(System.Runtime.GCSettings.IsServerGC ? "server" : "workstation")}, {Environment.ProcessorCount} cores.");
        for (var round = 1; round <= options.Repeat; round++)
        {
            foreach (var shape in shapes)
            {
                await MeasureAsync(shape, concurrency: 1, shape.Sequential, round, output, ct).ConfigureAwait(false);
                await MeasureAsync(shape, concurrency: 32, shape.Concurrent, round, output, ct).ConfigureAwait(false);
            }
        }

        return 0;
    }

    private static async Task MeasureAsync(Shape shape, int concurrency, int messages, int round, TextWriter output, CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, ct).ConfigureAwait(false);
        using var accepted = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
        listener.Stop();

        var senderMetrics = new ClusterMetrics();
        using var sender = new NodeLink(client, senderMetrics);
        using var receiver = new NodeLink(accepted, new ClusterMetrics());
        long decodedOps = 0;
        receiver.Handler = (_, type, body) =>
        {
            Interlocked.Add(ref decodedOps, Decode(type, body));
            return Task.FromResult<object?>(null);
        };
        sender.Start();
        receiver.Start();

        // Bodies built up front, so the measurement is the link, not the batch construction.
        var bodies = Enumerable.Range(0, Math.Min(messages, 64)).Select(shape.Body).ToArray();
        for (var i = 0; i < Math.Min(messages / 10, 200); i++)
            await sender.RequestAsync(shape.Type, bodies[i % bodies.Length], ct).ConfigureAwait(false);

        var retainedBefore = Retained();
        var sentBefore = senderMetrics.SentBytesByType.GetValueOrDefault(shape.Type);
        var countBefore = senderMetrics.SentByType.GetValueOrDefault(shape.Type);
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        var clock = Stopwatch.StartNew();
        var next = -1;
        var workers = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            int i;
            while ((i = Interlocked.Increment(ref next)) < messages)
                await sender.RequestAsync(shape.Type, bodies[i % bodies.Length], ct).ConfigureAwait(false);
        }, ct)).ToArray();
        await Task.WhenAll(workers).ConfigureAwait(false);
        clock.Stop();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var wire = senderMetrics.SentBytesByType.GetValueOrDefault(shape.Type) - sentBefore;
        var sent = senderMetrics.SentByType.GetValueOrDefault(shape.Type) - countBefore;
        var retained = Retained() - retainedBefore;

        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"LINKBENCH round={round} shape={shape.Name} senders={concurrency} msgs={sent} " +
            $"us_per_msg={clock.Elapsed.TotalMicroseconds / messages:F1} msgs_per_s={messages / clock.Elapsed.TotalSeconds:F0} " +
            $"mib_per_s={wire / clock.Elapsed.TotalSeconds / (1 << 20):F1} wire_bytes_per_msg={wire / Math.Max(1, sent)} " +
            $"alloc_bytes_per_msg={allocated / messages} retained_mib={retained / (double)(1 << 20):F1} decoded_ops={Interlocked.Read(ref decodedOps)}"));
    }

    /// <summary>What a real handler does with the body: decode it into its message type.</summary>
    private static int Decode(string type, System.Text.Json.JsonElement? body) => type switch
    {
        "border-apply" => System.Text.Json.JsonSerializer.Deserialize<BorderBatch>(body!.Value)!.Ops.Length,
        "heartbeat" => System.Text.Json.JsonSerializer.Deserialize<HeartbeatRequest>(body!.Value)!.Loads.Length,
        _ => 0,
    };

    private static long Retained()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: false);
    }

    private static BorderBatch Batch(int seq, int rows, int rowBytes)
    {
        var ops = new WireOp[rows];
        for (var i = 0; i < rows; i++)
        {
            var key = BitConverter.GetBytes((long)seq * rows + i);
            var row = new byte[rowBytes];
            Random.Shared.NextBytes(row);
            ops[i] = new WireOp(1, 7, key, row);
        }

        return new BorderBatch(1UL << 32, 2UL << 32, Guid.NewGuid().ToString("N"), (ulong)seq, ops);
    }

    private static HeartbeatRequest Heartbeat() =>
        new([.. Enumerable.Range(0, 4).Select(static i => new ShardLoadDto((ulong)i, 0.25, 1_000_000UL + (ulong)i, 64L << 20, 1_200, 40_000))]);
}
