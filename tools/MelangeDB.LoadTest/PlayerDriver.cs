using System.Collections.Concurrent;
using System.Diagnostics;
using MelangeDB.Client;
using MelangeDB.Cluster;

namespace MelangeDB.LoadTest;

/// <summary>
/// One simulated player: a real <see cref="MelangeClient"/> through the gateway, the walk-test
/// subscriptions (terrain and positions, scoped by the server to the player's shard plus band),
/// and a biased walk issuing Move calls at the tick rate. Latency is measured call-to-delta: the
/// tick's sequence number is embedded in the Move call, and the clock stops when the row carrying
/// that sequence arrives back on this client's own subscription — the full commit-plus-fan-out
/// path, not just the ack.
/// </summary>
internal sealed class PlayerDriver : IAsyncDisposable
{
    /// <summary>Pending entries older than this are counted lost rather than kept forever.</summary>
    private static readonly TimeSpan PendingTimeout = TimeSpan.FromSeconds(30);

    private readonly LoadTestOptions _options;
    private readonly DriveMetrics _metrics;
    private readonly MelangeClient _client;
    private readonly byte[] _identity;
    private readonly Random _random;
    private readonly bool _seamWalker;
    private readonly ConcurrentDictionary<long, long> _pendingSentAt = new();
    private MelangeSubscription? _terrain;
    private MelangeSubscription? _positions;
    private long _seq;
    private long _crossingSeq = -1;
    private long _crossingSentAt;
    private bool _nextMoveCrosses;
    private volatile bool _stopped;

    // Walk state. Chunk coordinates are non-negative and clamped to the world.
    private int _cx;
    private int _cy;
    private int _dirX;
    private int _dirY;
    private int _dirStepsLeft;
    private readonly int _seamBoundary; // First chunk column (or row) of the far block.
    private readonly bool _seamVertical;

    public PlayerDriver(int index, LoadTestOptions options, DriveMetrics metrics)
    {
        _options = options;
        _metrics = metrics;
        _random = new Random(unchecked(9973 * (index + 1)));
        var subject = $"walker-{index:D5}";
        _identity = DevTokens.IdentityOf(subject).ToByteArray();
        _client = new MelangeClient(new MelangeClientOptions
        {
            Uri = options.Address!,
            Token = DevTokens.For(subject),
        });
        _client.OnDisconnected += () =>
        {
            if (!_stopped)
                _metrics.Disconnected();
        };
        _client.OnError += _ =>
        {
            if (!_stopped)
                _metrics.ResyncError();
        };

        _seamVertical = options.WorldBlocksX > 1;
        _seamWalker = index < options.Players * options.SeamFraction && (options.WorldBlocksX > 1 || options.WorldBlocksY > 1);
        if (_seamWalker)
        {
            // Oscillate across one block boundary, one chunk past the hysteresis margin each
            // way, so transfers keep firing while staying safely inside the border band.
            var boundaries = (_seamVertical ? options.WorldBlocksX : options.WorldBlocksY) - 1;
            _seamBoundary = (_seamVertical ? options.BlockChunksX : options.BlockChunksY) * (1 + index % boundaries);
            _cx = _seamVertical ? _seamBoundary - 1 : _random.Next(options.WorldChunksX);
            _cy = _seamVertical ? _random.Next(options.WorldChunksY) : _seamBoundary - 1;
            _dirX = _seamVertical ? 1 : 0;
            _dirY = _seamVertical ? 0 : 1;
        }
        else
        {
            _cx = _random.Next(options.WorldChunksX);
            _cy = _random.Next(options.WorldChunksY);
            PickRoamDirection();
        }
    }

    public long BytesReceived => _client.BytesReceived;

    public long Inconsistencies => (_terrain?.Inconsistencies ?? 0) + (_positions?.Inconsistencies ?? 0);

    public async Task ConnectAsync(CancellationToken ct)
    {
        await _client.ConnectAsync(ct).ConfigureAwait(false);

        // Place the spawn's shard on the hub first, so the session routes to it and the world
        // starts populated across every shard instead of everyone piling into block (0,0).
        var (bx, by) = (_cx / _options.BlockChunksX, _cy / _options.BlockChunksY);
        await _client.CallReducerAsync(
            "SetPlayerShard", [SpatialShardStrategy.ShardOfBlock(bx, by).Value], ct).ConfigureAwait(false);

        _terrain = await _client.SubscribeAsync("SELECT * FROM Terrain", null, ct).ConfigureAwait(false);
        _positions = await _client.SubscribeAsync("SELECT * FROM PlayerPos", null, ct).ConfigureAwait(false);
        HookDeltaCounters(_terrain);
        HookDeltaCounters(_positions);
        _positions.OnInsert += row => OnSelfCandidate(row);
        _positions.OnUpdate += (_, row) => OnSelfCandidate(row);

        await _client.CallReducerAsync("Move", [Chunks.Id(_cx, _cy), 0L], ct).ConfigureAwait(false);
    }

    /// <summary>The tick loop: one Move per tick, one chunk step every ChunkEveryTicks ticks.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1 / _options.TickHz));
        var tick = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                tick++;
                if (tick % _options.ChunkEveryTicks == 0)
                    AdvanceChunk();
                SendMove();
                if (tick % 64 == 0)
                    PruneStalePending();
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void SendMove()
    {
        var seq = Interlocked.Increment(ref _seq);
        var crossing = _nextMoveCrosses;
        _nextMoveCrosses = false;
        _metrics.CallAttempted();
        if (crossing)
        {
            _metrics.Crossing();
            _crossingSeq = seq;
            _crossingSentAt = Stopwatch.GetTimestamp();
        }

        _pendingSentAt[seq] = Stopwatch.GetTimestamp();
        _ = CallAsync(Chunks.Id(_cx, _cy), seq, _metrics.Epoch);
    }

    private async Task CallAsync(uint chunkId, long seq, int epoch)
    {
        try
        {
            await _client.CallReducerAsync("Move", [chunkId, seq]).ConfigureAwait(false);
            _metrics.CallAcked(epoch);
        }
        catch (MelangeCallException)
        {
            // Rejected — typically a write past the band while a transfer catches up, or the
            // gateway's held-call cap during a wedged transfer. The walk continues; the next
            // tick retries the position.
            _pendingSentAt.TryRemove(seq, out _);
            _metrics.CallRejected(epoch);
        }
        catch (Exception)
        {
            _pendingSentAt.TryRemove(seq, out _);
            if (!_stopped)
                _metrics.TransportError(epoch);
        }
    }

    /// <summary>Runs on the client's receive loop for every PlayerPos delta; cheap by design.</summary>
    private void OnSelfCandidate(MelangeRow row)
    {
        if (row.Columns["PlayerId"] is not byte[] id || !id.AsSpan().SequenceEqual(_identity))
            return;
        var seq = Convert.ToInt64(row.Columns["Seq"], System.Globalization.CultureInfo.InvariantCulture);
        var now = Stopwatch.GetTimestamp();
        if (_pendingSentAt.TryRemove(seq, out var sentAt))
            _metrics.CallToDelta.Record(Stopwatch.GetElapsedTime(sentAt, now).TotalMilliseconds);

        // Anything below the observed sequence arrived inside a swap's replaced initial set
        // rather than as a delta event — the row got there, but the sample is unmeasurable.
        foreach (var pending in _pendingSentAt.Keys)
        {
            if (pending < seq && _pendingSentAt.TryRemove(pending, out _))
                _metrics.AbsorbedSelfDelta();
        }

        var crossingSeq = _crossingSeq;
        if (crossingSeq >= 0 && seq >= crossingSeq)
        {
            _crossingSeq = -1;
            _metrics.CrossingContinuity.Record(Stopwatch.GetElapsedTime(_crossingSentAt, now).TotalMilliseconds);
        }
    }

    private void HookDeltaCounters(MelangeSubscription subscription)
    {
        subscription.OnInsert += _ => _metrics.DeltaRow();
        subscription.OnUpdate += (_, _) => _metrics.DeltaRow();
        subscription.OnDelete += _ => _metrics.DeltaRow();
    }

    private void PruneStalePending()
    {
        var now = Stopwatch.GetTimestamp();
        foreach (var (seq, sentAt) in _pendingSentAt)
        {
            if (Stopwatch.GetElapsedTime(sentAt, now) > PendingTimeout && _pendingSentAt.TryRemove(seq, out _))
                _metrics.LostSelfDelta();
        }
    }

    private void AdvanceChunk()
    {
        var (previousBx, previousBy) = (_cx / _options.BlockChunksX, _cy / _options.BlockChunksY);
        if (_seamWalker)
            AdvanceSeam();
        else
            AdvanceRoam();
        _nextMoveCrosses = (_cx / _options.BlockChunksX, _cy / _options.BlockChunksY) != (previousBx, previousBy);
    }

    /// <summary>
    /// Oscillates across the assigned boundary: one chunk past the trigger depth (margin + 1 into
    /// the far block), turn around, same depth on the other side. Every reversal is a real
    /// handoff candidate, and the depth stays within the border band.
    /// </summary>
    private void AdvanceSeam()
    {
        var depth = _options.MarginChunks + 1;
        if (_seamVertical)
        {
            _cx += _dirX;
            if (_cx >= _seamBoundary + depth - 1 || _cx <= _seamBoundary - depth)
                _dirX = -_dirX;
        }
        else
        {
            _cy += _dirY;
            if (_cy >= _seamBoundary + depth - 1 || _cy <= _seamBoundary - depth)
                _dirY = -_dirY;
        }
    }

    /// <summary>A biased random walk: keep a direction for a while, so seam crossings actually happen.</summary>
    private void AdvanceRoam()
    {
        if (_dirStepsLeft <= 0)
            PickRoamDirection();
        var nx = Math.Clamp(_cx + _dirX, 0, _options.WorldChunksX - 1);
        var ny = Math.Clamp(_cy + _dirY, 0, _options.WorldChunksY - 1);
        if (nx == _cx && ny == _cy)
        {
            PickRoamDirection();
            return; // Against the world edge; step next tick in the new direction.
        }

        (_cx, _cy) = (nx, ny);
        _dirStepsLeft--;
    }

    private void PickRoamDirection()
    {
        do
        {
            _dirX = _random.Next(-1, 2);
            _dirY = _random.Next(-1, 2);
        }
        while (_dirX == 0 && _dirY == 0);
        _dirStepsLeft = _random.Next(6, 17);
    }

    public async ValueTask DisposeAsync()
    {
        _stopped = true;
        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
