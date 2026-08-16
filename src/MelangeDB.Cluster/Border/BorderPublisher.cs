using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Cluster;

/// <summary>
/// The owner half of interest-driven replication, one per owned shard: a log-driven pump that
/// ships this shard's border-relevant Partitioned ops to each subscribed observer shard, in LSN
/// order, via the hub. The observer's durable cursor is the truth (persisted before its ack);
/// this side keeps only in-memory stream cursors, pins nothing, and answers any cursor its log
/// can no longer serve — truncated past, another epoch, a widened band — with a full band reset
/// (EventId 1715) rather than ever silently resuming past a gap.
/// </summary>
internal sealed partial class BorderPublisher : ICommitObserver, IDisposable
{
    private const int MaxRecordsPerBatch = 256;

    private sealed class Stream
    {
        public ulong Cursor;

        public int BandChunks;

        public bool NeedsReset;

        /// <summary>
        /// The rows this stream has shipped and not retracted — what lets an update that moves a
        /// row <em>out</em> of the observer's scope become a delete on the observer, and lets
        /// irrelevant deletes be dropped instead of shipped. In-memory only: a stream that cannot
        /// replay its own history from LSN 0 starts with a reset instead (see
        /// <see cref="BorderPublisher.Subscribe"/>), so the set is always consistent with what the
        /// observer actually holds.
        /// </summary>
        public HashSet<(uint Table, RowKey Key)> Sent = [];
    }

    private readonly MelangeEngine _engine;
    private readonly ShardKey _shard;
    private readonly IShardStrategy _strategy;
    private readonly Func<TableId, RowKey, ulong?> _borrowedOwner;
    private readonly Func<NodeLink?> _link;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<ulong, Stream> _streams = new();
    private readonly SemaphoreSlim _signal = new(0, 1);
    private readonly CancellationTokenSource _stopped = new();
    private Task? _loop;

    public BorderPublisher(
        MelangeEngine engine,
        ShardKey shard,
        IShardStrategy strategy,
        Func<TableId, RowKey, ulong?> borrowedOwner,
        Func<NodeLink?> link,
        ILogger logger)
    {
        _engine = engine;
        _shard = shard;
        _strategy = strategy;
        _borrowedOwner = borrowedOwner;
        _link = link;
        _logger = logger;
    }

    public void Start()
    {
        _engine.AddCommitObserver(this);
        _loop = Task.Run(LoopAsync);
    }

    public void OnCommit(CommitRecord record)
    {
        if (record.WriteSet.Count > 0)
            Kick();
    }

    public void Kick()
    {
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    /// <summary>
    /// Registers (or refreshes) an observer's stream. The cursor only moves forward past the
    /// observer's durable position — records the owner already scanned and found empty for this
    /// observer need no redelivery — and moves <em>back</em> only via the reset path, which
    /// re-sends the whole band.
    /// </summary>
    public void Subscribe(BorderSubscribe subscribe)
    {
        var created = false;
        var stream = _streams.GetOrAdd(subscribe.ObserverShard, _ =>
        {
            created = true;
            return new Stream { Cursor = subscribe.FromLsn, BandChunks = subscribe.BandChunks };
        });
        lock (stream)
        {
            if (subscribe.ForceReset || subscribe.Epoch != _engine.Log.EpochId.ToString())
            {
                // The observer demanded a reset (its band changed relative to what its cursor was
                // taken under), or its cursor counts against another log incarnation. Either way
                // only a full re-send is right.
                stream.NeedsReset = true;
            }
            else if (subscribe.BandChunks > stream.BandChunks)
            {
                stream.NeedsReset = true; // Widened while this stream was live; same reasoning.
            }
            else if (created && subscribe.FromLsn > 0)
            {
                // A brand-new stream resuming mid-log cannot know what a previous owner term
                // already shipped (the sent-set is in-memory), so out-of-scope retractions could
                // be missed. Only a stream replaying from LSN 0 rebuilds the set deterministically;
                // everything else starts from a full reset.
                stream.NeedsReset = true;
            }
            else
            {
                stream.Cursor = Math.Max(stream.Cursor, subscribe.FromLsn);
            }

            stream.BandChunks = subscribe.BandChunks;
        }

        Kick();
    }

    private async Task LoopAsync()
    {
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var link = _link();
                var sent = false;
                if (link is not null)
                {
                    foreach (var (observer, stream) in _streams)
                        sent |= await PumpStreamAsync(link, observer, stream, ct).ConfigureAwait(false);
                }

                if (!sent)
                    await _signal.WaitAsync(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception)
            {
                // Link failure mid-send: no cursor advanced; retry after a beat.
                try
                {
                    await Task.Delay(200, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async Task<bool> PumpStreamAsync(NodeLink link, ulong observer, Stream stream, CancellationToken ct)
    {
        ulong cursor;
        bool needsReset;
        lock (stream)
        {
            cursor = stream.Cursor;

            // A cursor below the truncation base cannot be served from this log — the gap's
            // records are gone, and FileCommitLog.ReadFrom would silently serve from BaseLsn+1,
            // losing every border update in between. Reset instead, loudly.
            if (cursor < _engine.Log.BaseLsn)
                stream.NeedsReset = true;
            needsReset = stream.NeedsReset;
        }

        if (needsReset)
        {
            await SendResetAsync(link, observer, stream, ct).ConfigureAwait(false);
            return true;
        }

        // Durable, not head: ReadFrom serves nothing beyond the durability watermark, so judging
        // availability by the head would report more-to-do through a gap the scan cannot reach yet
        // and drain-until-done would spin through it.
        var head = _engine.Log.DurableLsn;
        if (cursor >= head)
            return false;

        var observerKey = new ShardKey(observer);
        var ops = new List<WireOp>();
        var scanned = cursor;
        var records = 0;
        foreach (var record in _engine.Log.ReadFrom(cursor + 1))
        {
            scanned = record.Lsn;

            // An observer whose border cursor lagged across an additive schema migration must
            // receive current-shape rows — its engine stores borrowed copies verbatim.
            CollectBorderOps(_engine.TransformToCurrentShape(record), observerKey, stream, ops);
            if (++records >= MaxRecordsPerBatch)
                break;
        }

        if (ops.Count > 0)
        {
            await link.RequestAsync(
                "border-batch",
                new BorderBatch(_shard.Value, observer, _engine.Log.EpochId.ToString(), scanned, [.. ops]),
                ct).ConfigureAwait(false);
        }

        // An empty scan advances only the in-memory cursor: no message, so an interior commit
        // stays a purely local commit. The observer's durable cursor lags across such gaps, and a
        // re-subscribe from it is answered by the forward-only cursor rule in Subscribe.
        lock (stream)
        {
            if (!stream.NeedsReset && stream.Cursor == cursor)
                stream.Cursor = scanned;
        }

        return scanned < head;
    }

    /// <summary>
    /// Collects the record's ops relevant to one observer: rows of this shard within the
    /// observer's band, plus rows strayed into the observer's own block (an entity this shard
    /// still owns, standing across the line mid-handoff — the observer's bystanders must see it).
    /// An update that moves a previously shipped row <em>out</em> of scope ships as a delete —
    /// the observer must stop seeing what walked away — and deletes of never-shipped rows are
    /// dropped. Border and replica records are skipped: copies of copies would gossip loops into
    /// the cluster.
    /// </summary>
    private void CollectBorderOps(CommitRecord record, ShardKey observer, Stream stream, List<WireOp> ops)
    {
        if (record.ReducerName is ClusterRecordNames.Border or ClusterRecordNames.BorderReset or ClusterRecordNames.Replica)
            return;
        foreach (var op in record.WriteSet)
        {
            if (!_engine.Schema.TryGet(op.Table, out var table) || table.Placement != Placement.Partitioned)
                continue;
            var sentKey = (op.Table.Value, op.Key);
            if (op.Kind == RowOpKind.Delete)
            {
                if (stream.Sent.Remove(sentKey))
                    ops.Add(WireOp.From(op));
                continue;
            }

            // A row this shard now merely borrows is the true owner's to publish, whatever this
            // shard's log says about its past — publishing it would gossip copies of copies with
            // the wrong owner attribution, and a wrongly attributed copy is how a shard's own
            // strayed entity ends up read-only everywhere.
            if (_borrowedOwner(op.Table, op.Key) is not null)
            {
                if (stream.Sent.Remove(sentKey))
                    ops.Add(new WireOp((byte)RowOpKind.Delete, op.Table.Value, op.Key.ToArray(), null));
                continue;
            }

            var row = table.ToRowRef(op.Row);
            if (RelevantTo(observer, op.Table, row))
            {
                stream.Sent.Add(sentKey);
                ops.Add(WireOp.From(op));
            }
            else if (stream.Sent.Remove(sentKey))
            {
                ops.Add(new WireOp((byte)RowOpKind.Delete, op.Table.Value, op.Key.ToArray(), null));
            }
        }
    }

    /// <summary>In the observer's band of this shard, or strayed into the observer's own block.</summary>
    private bool RelevantTo(ShardKey observer, TableId table, in RowRef row) =>
        _strategy.InterestedInRow(_shard, observer, table, row)
        || (_strategy.ShardForRow(table, row) == observer && _strategy.MayCommit(_shard, table, row));

    private async Task SendResetAsync(NodeLink link, ulong observer, Stream stream, CancellationToken ct)
    {
        var observerKey = new ShardKey(observer);
        ulong resetLsn = 0;
        var tables = new List<ReplicaTableSnapshot>();
        var sent = new HashSet<(uint, RowKey)>();
        _engine.ReadConsistent(head =>
        {
            resetLsn = head;
            foreach (var table in _engine.Schema.Tables)
            {
                if (table.Placement != Placement.Partitioned)
                    continue;
                var slice = new List<WireOp>();
                foreach (var (key, bytes) in _engine.HotStore.Scan(table.Id))
                {
                    // Borrowed rows are the true owner's to publish; see CollectBorderOps.
                    if (_borrowedOwner(table.Id, key) is not null)
                        continue;
                    if (RelevantTo(observerKey, table.Id, table.ToRowRef(bytes)))
                    {
                        slice.Add(new WireOp((byte)RowOpKind.Insert, table.Id.Value, key.ToArray(), bytes.ToArray()));
                        sent.Add((table.Id.Value, key));
                    }
                }

                tables.Add(new ReplicaTableSnapshot(table.Id.Value, [.. slice]));
            }
        });

        await link.RequestAsync(
            "border-reset",
            new BorderReset(_shard.Value, observer, _engine.Log.EpochId.ToString(), resetLsn, [.. tables]),
            ct).ConfigureAwait(false);
        LogBorderReset(_logger, _shard.Value, observer, sent.Count, resetLsn);
        lock (stream)
        {
            stream.NeedsReset = false;
            stream.Cursor = resetLsn;
            stream.Sent = sent;
        }
    }

    public void Dispose()
    {
        _stopped.Cancel();
        Kick();
        try
        {
            _loop?.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException)
        {
        }
    }

    [LoggerMessage(EventId = 1715, EventName = "BorderStreamReset", Level = LogLevel.Information,
        Message = "Shard {OwnerShard}'s border stream to observer shard {ObserverShard} could not resume from the observer's cursor " +
            "(truncated past, another log epoch, or a widened band); the full band ({Rows} row(s) at LSN {ResetLsn}) was sent as a reset " +
            "instead of silently skipping the gap.")]
    private static partial void LogBorderReset(ILogger logger, ulong ownerShard, ulong observerShard, int rows, ulong resetLsn);
}
