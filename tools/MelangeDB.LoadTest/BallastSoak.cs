using System.Diagnostics;
using MelangeDB.Cluster;

namespace MelangeDB.LoadTest;

/// <summary>
/// serve's paged working set: <see cref="LoadTestOptions.BallastRows"/> ballast rows spread over
/// every block, then rewritten at <see cref="LoadTestOptions.BallastChurnPerSecond"/> for as long
/// as the server runs. The walkers' position rows are few and update in place, so on their own
/// they never leave a paged store's memory; this gives a soak a log that outgrows the budget and
/// fills with superseded versions for compaction to reclaim.
/// </summary>
internal sealed class BallastSoak : IAsyncDisposable
{
    private const int Batch = 500;
    private const int ChurnCall = 50;

    private readonly LoadTestServer _server;
    private readonly LoadTestOptions _options;
    private readonly CancellationTokenSource _stop = new();
    private Task _churn = Task.CompletedTask;
    private long _rewritten;
    private long _failed;
    private string? _lastError;

    private BallastSoak(LoadTestServer server, LoadTestOptions options)
    {
        _server = server;
        _options = options;
    }

    public long Rewritten => Interlocked.Read(ref _rewritten);

    public long Failed => Interlocked.Read(ref _failed);

    public string? LastError => Volatile.Read(ref _lastError);

    private int Batches => (_options.BallastRows + Batch - 1) / Batch;

    private int PayloadBytes => Math.Max(16, _options.RowBytes);

    public static async Task<BallastSoak> StartAsync(LoadTestServer server, LoadTestOptions options, TextWriter output, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        var seeder = DevTokens.IdentityOf("ballast-soak");
        var soak = new BallastSoak(server, options);
        for (var batch = 0; batch < soak.Batches; batch++)
        {
            var first = (long)batch * Batch;
            var count = (int)Math.Min(Batch, options.BallastRows - first);
            await soak.CallAsync("SeedBallast", seeder, batch, first, count, ct, null).ConfigureAwait(false);
        }

        output.WriteLine(
            $"Ballast: seeded {options.BallastRows} rows of {soak.PayloadBytes} B over {options.WorldBlocksX * options.WorldBlocksY} block(s) " +
            $"in {clock.Elapsed.TotalSeconds:F1} s; rewriting {options.BallastChurnPerSecond} rows/s.");
        soak.Start();
        return soak;
    }

    private void Start()
    {
        if (_options.BallastChurnPerSecond > 0)
            _churn = Task.Run(ChurnLoop);
    }

    private async Task ChurnLoop()
    {
        var caller = DevTokens.IdentityOf("ballast-soak");
        var random = new Random(1234);
        var clock = Stopwatch.StartNew();
        long version = 0;
        long done = 0;
        while (!_stop.IsCancellationRequested)
        {
            var due = (long)(clock.Elapsed.TotalSeconds * _options.BallastChurnPerSecond);
            if (done >= due)
            {
                try
                {
                    await Task.Delay(10, _stop.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                continue;
            }

            var batch = random.Next(Batches);
            var batchFirst = (long)batch * Batch;
            var batchCount = (int)Math.Min(Batch, _options.BallastRows - batchFirst);
            var offset = random.Next(Math.Max(1, batchCount - ChurnCall + 1));
            var count = Math.Min(ChurnCall, batchCount - offset);
            try
            {
                await CallAsync("ChurnBallast", caller, batch, batchFirst + offset, count, _stop.Token, ++version).ConfigureAwait(false);
                Interlocked.Add(ref _rewritten, count);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                Interlocked.Increment(ref _failed);
                Volatile.Write(ref _lastError, $"{exception.GetType().Name}: {exception.Message}");
            }

            done += count;
        }
    }

    /// <summary>Calls <paramref name="reducer"/> on the shard owning <paramref name="batch"/>'s chunk, waiting out a reassignment.</summary>
    private async Task CallAsync(string reducer, Identity caller, int batch, long firstId, int count, CancellationToken ct, long? version)
    {
        var blocks = _options.WorldBlocksX * _options.WorldBlocksY;
        var block = batch % blocks;
        var (bx, by) = (block % _options.WorldBlocksX, block / _options.WorldBlocksX);
        var chunkInBlock = batch / blocks % (_options.BlockChunksX * _options.BlockChunksY);
        var chunk = Chunks.Id(
            bx * _options.BlockChunksX + chunkInBlock % _options.BlockChunksX,
            by * _options.BlockChunksY + chunkInBlock / _options.BlockChunksX);
        var shard = SpatialShardStrategy.ShardOfBlock(bx, by);

        var waited = Stopwatch.StartNew();
        while (true)
        {
            if (_server.TryGetShard(shard) is { } owner)
            {
                if (version is { } v)
                    owner.ReducerHost.Call(reducer, caller, chunk, firstId, count, PayloadBytes, v);
                else
                    owner.ReducerHost.Call(reducer, caller, chunk, firstId, count, PayloadBytes);
                return;
            }

            if (waited.Elapsed > TimeSpan.FromSeconds(60))
                throw new TimeoutException($"Shard {shard.Value} has had no owner for 60 s.");
            await Task.Delay(100, ct).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        try
        {
            await _churn.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        _stop.Dispose();
    }
}
