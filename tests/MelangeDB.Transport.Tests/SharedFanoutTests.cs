using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Shared fan-out (road-to-0.4 phase 31, step 2): subscriptions that are one query share one
/// evaluation and one encoding of their ops. What must hold: the bytes on the wire are exactly what
/// the per-subscriber path writes, grouping follows the query (operands, re-scopes, departures),
/// and a table with policies is never grouped.
/// </summary>
public sealed class SharedFanoutTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-shared-fanout-").FullName;
    private readonly MelangeEngine _engine;
    private readonly SubscriptionEngine _subscriptions;
    private readonly MessagePackFrameSerializer _serializer = new();
    private uint _nextId;

    public SharedFanoutTests()
    {
        var schema = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables()
            .Where(static t => t.RowType == typeof(PlayerState) || t.RowType == typeof(InventoryItem) || t.RowType == typeof(AdminIdentity)));
        _engine = new MelangeEngine(
            new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(_root, "hot") },
                CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.OsBuffered },
                Snapshots = { Enabled = false },
            },
            schema,
            NullLoggerFactory.Instance);
        _subscriptions = new SubscriptionEngine(_engine, telemetry: null);
    }

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
    public void Shared_frames_are_byte_identical_to_the_per_subscriber_encoding()
    {
        var player = Spawn("p", room: 7);
        var sinks = Enumerable.Range(0, 5).Select(_ => new CapturingSink()).ToList();
        foreach (var sink in sinks)
            Subscribe(sink, "SELECT * FROM PlayerState");
        Assert.Equal(1, _subscriptions.GroupCount);

        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 7, Name = "p", X = 3 }));

        foreach (var sink in sinks)
        {
            var frame = Assert.Single(sink.Frames);
            var update = Assert.Single(frame.Updates);
            Assert.False(update.EncodedOps.IsEmpty);

            // The same frame with the ops left for the serializer to encode, as the per-subscriber
            // path always did: the bytes must match exactly, and so must the measured length the
            // backpressure ledger is debited by.
            var plain = new TransactionUpdateFrame(frame.Lsn, [new SubscriptionUpdate(update.SubscriptionId, update.Ops)]) { Channel = frame.Channel };
            Assert.Equal(_serializer.Serialize(plain), _serializer.Serialize(frame));
            Assert.Equal(_serializer.Serialize(plain).Length, _serializer.Measure(frame));

            // And it decodes to the update every client expects.
            var decoded = (TransactionUpdateFrame)_serializer.Deserialize(_serializer.Serialize(frame));
            Assert.Equal(RowOpKind.Update, Assert.Single(Assert.Single(decoded.Updates).Ops).Kind);
        }

        // One encoding for all of them, not one each.
        Assert.Single(sinks.Select(static s => s.Frames[0].Updates[0].EncodedOps).Distinct(new SameBuffer()));
    }

    [Fact]
    public void A_lone_member_is_encoded_by_its_sender_as_before()
    {
        var player = Spawn("p", room: 7);
        var sink = new CapturingSink();
        Subscribe(sink, "SELECT * FROM PlayerState");

        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 7, Name = "p", X = 3 }));

        // Encoding under the lock only pays when the bytes are shared.
        Assert.True(Assert.Single(Assert.Single(sink.Frames).Updates).EncodedOps.IsEmpty);
    }

    [Fact]
    public void Groups_follow_operands_rescopes_and_departures()
    {
        var player = Spawn("p", room: 7);
        var stays = new CapturingSink();
        var moves = new CapturingSink();
        var other = new CapturingSink();
        var staying = Subscribe(stays, "SELECT * FROM PlayerState WHERE RoomId = 7");
        var moving = Subscribe(moves, "SELECT * FROM PlayerState WHERE RoomId = 7");
        Subscribe(other, "SELECT * FROM PlayerState WHERE RoomId = 8");
        Assert.Equal(2, _subscriptions.GroupCount);

        // Re-scoped to room 8, it joins the room-8 group and stops hearing about room 7.
        _engine.ReadConsistent(_ => _subscriptions.Rescope(moving, SqlSubsetParser.Parse("SELECT * FROM PlayerState WHERE RoomId = 8", null), new SubscriptionsOptions()));
        Assert.Equal(2, _subscriptions.GroupCount);
        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 7, Name = "p", X = 1 }));
        Assert.Single(stays.Frames);
        Assert.Empty(moves.Frames);
        Assert.Empty(other.Frames);

        // The row moving into room 8 leaves the room-7 group (a delete) and enters the room-8 one.
        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 8, Name = "p", X = 1 }, previous: new PlayerState { Id = player, RoomId = 7, Name = "p", X = 1 }));
        Assert.Equal(RowOpKind.Delete, stays.Frames[^1].Updates.Single().Ops[^1].Kind);
        Assert.Equal(RowOpKind.Insert, moves.Frames.Single().Updates.Single().Ops.Single().Kind);
        Assert.Equal(RowOpKind.Insert, other.Frames.Single().Updates.Single().Ops.Single().Kind);

        // The group's first member leaving must not take the others' deltas with it.
        _subscriptions.Unregister(staying);
        Assert.Equal(1, _subscriptions.GroupCount);
        _subscriptions.Unregister(moving);
        Assert.Equal(1, _subscriptions.GroupCount);
        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 8, Name = "p", X = 2 }, previous: new PlayerState { Id = player, RoomId = 8, Name = "p", X = 1 }));
        Assert.Equal(2, other.Frames.Count);
    }

    [Fact]
    public void A_table_with_policies_is_never_grouped()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IRowPolicy<InventoryItem>, InventoryVisibility>();
        var policies = new PolicySet(services.BuildServiceProvider(), _engine.Schema);
        var query = SqlSubsetParser.Parse("SELECT * FROM InventoryItem", null);
        for (var i = 0; i < 3; i++)
        {
            var context = new PolicyContext(Identity.Hash($"viewer{i}"), false, _engine.CommittedView);
            _engine.ReadConsistent(head => _subscriptions.Register(
                new CapturingSink(), ++_nextId, query, new SubscriptionsOptions(), head, computeInitialSet: false, policies, context));
        }

        Assert.Equal(0, _subscriptions.GroupCount);
    }

    [Fact]
    public void A_connection_gets_one_frame_listing_its_subscriptions_in_registration_order()
    {
        var player = Spawn("p", room: 7);
        var both = new CapturingSink();
        var peer = new CapturingSink();
        var first = Subscribe(both, "SELECT * FROM PlayerState WHERE RoomId = 7");
        Subscribe(peer, "SELECT * FROM PlayerState WHERE RoomId = 7");
        var second = Subscribe(both, "SELECT * FROM PlayerState");

        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 7, Name = "p", X = 9 }));

        var frame = Assert.Single(both.Frames);
        Assert.Equal([first.Id, second.Id], frame.Updates.Select(static u => u.SubscriptionId));
    }

    private Identity Spawn(string name, int room)
    {
        var id = Identity.Hash(name);
        _engine.Invoke("Spawn", id, ctx => ctx.Db.Insert(new PlayerState { Id = id, RoomId = room, Name = name, X = 0 }));
        return id;
    }

    private ServerSubscription Subscribe(CapturingSink sink, string sql)
    {
        var query = SqlSubsetParser.Parse(sql, null);
        return _engine.ReadConsistent(head =>
            _subscriptions.Register(sink, ++_nextId, query, new SubscriptionsOptions(), head, computeInitialSet: false).Subscription);
    }

    /// <summary>
    /// An update record as the fan-out sees it. The store holds the spawned row, so the pre-image
    /// is the store's unless <paramref name="previous"/> stands in for it: the record then carries
    /// the previous state first, as a border batch does, so the second op sees it as its old row.
    /// </summary>
    private CommitRecord Update(PlayerState row, PlayerState? previous = null)
    {
        var table = _engine.Schema.Get(typeof(PlayerState));
        var codec = (RowCodec<PlayerState>)table.Codec!;
        var ops = new List<RowOp>();
        if (previous is { } before)
            ops.Add(new RowOp(RowOpKind.Update, table.Id, codec.EncodePrimaryKey(in before), codec.Serialize(in before)));
        ops.Add(new RowOp(RowOpKind.Update, table.Id, codec.EncodePrimaryKey(in row), codec.Serialize(in row)));
        return new CommitRecord
        {
            Lsn = _engine.Log.HeadLsn + 1,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("test"),
            ReducerName = "test",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = ops,
            SerializedLength = 0,
        };
    }

    private sealed class CapturingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }

    private sealed class SameBuffer : IEqualityComparer<ReadOnlyMemory<byte>>
    {
        public bool Equals(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y) => x.Equals(y);

        public int GetHashCode(ReadOnlyMemory<byte> obj) => obj.Length;
    }
}
