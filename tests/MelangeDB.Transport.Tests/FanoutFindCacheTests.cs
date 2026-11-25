using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A row policy that reads the caller's own row (<c>ctx.Db.Find&lt;T&gt;(ctx.Caller)</c>) is judged
/// for every subscriber on both sides of every op. Committed state is fixed while one record fans
/// out, so the policy view caches those lookups for the record (#315): each looked-up row is
/// decoded once per record, and never carried into the next one.
/// </summary>
public class FanoutFindCacheTests : IDisposable
{
    private const int Viewers = 40;
    private const int Room = 7;

    private readonly string _root = Directory.CreateTempSubdirectory("melange-fanout-find-").FullName;
    private readonly SchemaRegistry _schema = CountingCodec<PlayerState>.Registry(typeof(PlayerState));
    private readonly MelangeEngine _engine;
    private readonly SubscriptionEngine _subscriptions;
    private int _records;

    public FanoutFindCacheTests()
    {
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot") },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.OsBuffered },
            Snapshots = { Enabled = false },
        };
        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance);
        _subscriptions = new SubscriptionEngine(_engine, telemetry: null);
    }

    private IDecodeCounter Decodes => CountingCodec<PlayerState>.CounterFor(_schema, typeof(PlayerState));

    public void Dispose()
    {
        _engine.Dispose();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A store handle Windows has not released yet; the temp reaper gets it.
        }
    }

    [Fact]
    public void A_policy_reading_the_callers_row_decodes_it_once_per_record()
    {
        var sinks = SubscribeViewers();
        var movers = Enumerable.Range(0, 3).Select(i => Identity.Hash($"mover{i}")).ToArray();
        foreach (var mover in movers)
            Spawn(mover, Room);

        Decodes.Reset();
        _subscriptions.Fanout(Record(movers.Select((mover, i) => Player(mover, Room, x: i + 1))));

        // Three ops: each op's pre-image and new row (6), plus each viewer's own row once (40).
        // Uncached, every viewer's row was read for both sides of every op: 240 more.
        Assert.InRange(Decodes.Deserializations, 0, 2 * movers.Length + Viewers);
        Assert.All(sinks, sink => Assert.Equal(movers.Length, Assert.Single(Assert.Single(sink.Frames).Updates).Ops.Count));
    }

    [Fact]
    public void A_record_never_sees_a_row_cached_by_the_one_before()
    {
        var sinks = SubscribeViewers();
        var mover = Identity.Hash("mover");
        Spawn(mover, Room);

        _subscriptions.Fanout(Record([Player(mover, Room, x: 1)]));
        Assert.All(sinks, sink => Assert.Single(sink.Frames));

        // Viewer 0 leaves the room between records. Its row was cached for the first record; the
        // second must read it again and judge the mover invisible to it.
        Spawn(Viewer(0), Room + 1, update: true);
        Decodes.Reset();
        _subscriptions.Fanout(Record([Player(mover, Room, x: 2)]));

        Assert.Single(sinks[0].Frames);
        Assert.All(sinks.Skip(1), sink => Assert.Equal(2, sink.Frames.Count));
        Assert.InRange(Decodes.Deserializations, Viewers, 2 + Viewers);
    }

    [Fact]
    public void Only_the_thread_that_began_the_record_reads_through_the_cache()
    {
        var player = Identity.Hash("p");
        Spawn(player, Room);
        var view = (CommittedReadView)_engine.CommittedView;

        Decodes.Reset();
        view.BeginRecord();
        try
        {
            Assert.Equal(Room, view.Find<PlayerState>(player)!.Value.RoomId);
            Assert.Equal(Room, view.Find<PlayerState>(player)!.Value.RoomId);
            Assert.Null(view.Find<PlayerState>(Identity.Hash("nobody")));
            Assert.Null(view.Find<PlayerState>(Identity.Hash("nobody")));
            Assert.Equal(1, Decodes.Deserializations);

            // Another thread is ad-hoc SQL or a handoff reading off the lock: it reads the store.
            var other = new Thread(() =>
            {
                view.Find<PlayerState>(player);
                view.Find<PlayerState>(player);
            });
            other.Start();
            other.Join();
            Assert.Equal(3, Decodes.Deserializations);
        }
        finally
        {
            view.EndRecord();
        }

        view.Find<PlayerState>(player);
        Assert.Equal(4, Decodes.Deserializations);
    }

    private static Identity Viewer(int i) => Identity.Hash($"viewer{i}");

    private static PlayerState Player(Identity id, int room, float x = 0) => new() { Id = id, RoomId = room, Name = "p", X = x };

    private void Spawn(Identity id, int room, bool update = false) =>
        _engine.Invoke("Spawn", id, ctx =>
        {
            if (update)
                ctx.Db.Update(Player(id, room));
            else
                ctx.Db.Insert(Player(id, room));
        });

    /// <summary>Each viewer in <see cref="Room"/>, subscribed to the whole table under <see cref="SameRoomAsCaller"/>.</summary>
    private List<CapturingSink> SubscribeViewers()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IRowPolicy<PlayerState>, SameRoomAsCaller>();
        var policies = new PolicySet(services.BuildServiceProvider(), _schema);

        var sinks = new List<CapturingSink>();
        for (var i = 0; i < Viewers; i++)
        {
            Spawn(Viewer(i), Room);
            var sink = new CapturingSink();
            var context = new PolicyContext(Viewer(i), false, _engine.CommittedView);
            var query = SqlSubsetParser.Parse("SELECT * FROM PlayerState", null);
            var id = (uint)(i + 1);
            _engine.ReadConsistent(head =>
                _subscriptions.Register(sink, id, query, new SubscriptionsOptions(), head, computeInitialSet: false, policies, context));
            sinks.Add(sink);
        }

        return sinks;
    }

    private CommitRecord Record(IEnumerable<PlayerState> rows)
    {
        var table = _schema.Get(typeof(PlayerState));
        var codec = (RowCodec<PlayerState>)table.Codec!;
        return new CommitRecord
        {
            Lsn = _engine.Log.HeadLsn + (ulong)++_records,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("test"),
            ReducerName = "test",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [.. rows.Select(row => new RowOp(RowOpKind.Update, table.Id, codec.EncodePrimaryKey(in row), codec.Serialize(in row)))],
            SerializedLength = 0,
        };
    }

    private sealed class CapturingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }

    /// <summary>The issue's shape: a row is visible to callers standing in the same room, read from their own row.</summary>
    private sealed class SameRoomAsCaller : IRowPolicy<PlayerState>
    {
        public bool IsVisibleTo(in PlayerState row, PolicyContext ctx) =>
            ctx.Db.Find<PlayerState>(ctx.Caller) is { } caller && caller.RoomId == row.RoomId;
    }
}
