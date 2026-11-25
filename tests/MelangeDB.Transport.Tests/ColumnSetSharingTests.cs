using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Column-policy fan-out shares its work between subscribers whose policies answer alike (#315),
/// and still gives every subscriber exactly the columns its own masks admit.
/// <para>
/// Each subscriber's delta is checked against <see cref="ServerSubscription.WireForm"/>, the cold
/// path that still builds a private set per call: sharing may change what the fan-out costs, never
/// what it sends.
/// </para>
/// </summary>
public class ColumnSetSharingTests : IDisposable
{
    private const int Viewers = 40;

    private readonly string _root = Directory.CreateTempSubdirectory("melange-column-sets-").FullName;
    private readonly SchemaRegistry _schema = CountingCodec<PlayerState>.Registry(typeof(PlayerState));
    private readonly MelangeEngine _engine;
    private readonly SubscriptionEngine _subscriptions;
    private readonly Identity _owner = Identity.Hash("owner");
    private int _records;

    public ColumnSetSharingTests()
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
    public void Subscribers_with_equal_masks_share_one_column_set_and_one_projection()
    {
        // HideoutHidesPosition builds its mask per call (ColumnMask.All.Except(nameof(X))), the
        // pattern the docs show: a new array every time, so sharing must key on content.
        var subscribers = Subscribe(services => services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>());
        Spawn(room: HideoutHidesPosition.HideoutRoom, name: "o", x: 0);

        var sets = _subscriptions.ColumnSetsBuilt;
        var projections = _subscriptions.ProjectionsBuilt;
        var next = new PlayerState { Id = _owner, RoomId = HideoutHidesPosition.HideoutRoom, Name = "renamed", X = 5 };
        _subscriptions.Fanout(Record(RowOpKind.Update, next));

        // One set for every viewer (the owner's mask admits everything and keeps its input), and
        // two projections: the owner's full row and the viewers' row without X.
        Assert.Equal(1, _subscriptions.ColumnSetsBuilt - sets);
        Assert.Equal(2, _subscriptions.ProjectionsBuilt - projections);
        AssertEveryDeltaMatchesItsOwnMask(subscribers, next, RowOpKind.Update);
    }

    [Fact]
    public void Masks_of_different_modes_over_one_array_are_not_confused()
    {
        // Half the viewers get Only(shared), half All.Except(shared): one array, opposite answers.
        // Keyed on the array alone, the second half would receive the first half's columns.
        var onlyViewers = Enumerable.Range(0, Viewers).Where(i => i % 2 == 0).Select(Viewer).ToHashSet();
        var subscribers = Subscribe(services =>
            services.AddSingleton<IColumnPolicy<PlayerState>>(new SharedArrayModes(onlyViewers)));
        Spawn(room: 7, name: "o", x: 0);

        var sets = _subscriptions.ColumnSetsBuilt;
        var next = new PlayerState { Id = _owner, RoomId = 7, Name = "renamed", X = 5 };
        _subscriptions.Fanout(Record(RowOpKind.Update, next));

        Assert.Equal(2, _subscriptions.ColumnSetsBuilt - sets);
        AssertEveryDeltaMatchesItsOwnMask(subscribers, next, RowOpKind.Update);
    }

    [Fact]
    public void Several_column_policies_intersect_and_share_each_step()
    {
        var subscribers = Subscribe(services =>
        {
            services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>();
            services.AddSingleton<IColumnPolicy<PlayerState>, NameHiddenFromOthers>();
        });
        Spawn(room: HideoutHidesPosition.HideoutRoom, name: "o", x: 0);

        // Leaving the hideout changes the viewers' mask between the pre-image and the new row:
        // before, X and Name are hidden; after, only Name. The changed mask itself must emit.
        var sets = _subscriptions.ColumnSetsBuilt;
        var next = new PlayerState { Id = _owner, RoomId = 14, Name = "renamed", X = 5 };
        _subscriptions.Fanout(Record(RowOpKind.Update, next));

        // Before: (all, -X) then (-X, -Name). After: (all, -Name). However many viewers.
        Assert.Equal(3, _subscriptions.ColumnSetsBuilt - sets);
        AssertEveryDeltaMatchesItsOwnMask(subscribers, next, RowOpKind.Update);
    }

    [Fact]
    public void A_change_only_to_hidden_columns_still_reaches_only_those_who_see_them()
    {
        var subscribers = Subscribe(services => services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>());
        Spawn(room: HideoutHidesPosition.HideoutRoom, name: "o", x: 0);

        _subscriptions.Fanout(Record(RowOpKind.Update, new PlayerState { Id = _owner, RoomId = HideoutHidesPosition.HideoutRoom, Name = "o", X = 9 }));

        foreach (var (identity, subscriber) in subscribers)
        {
            if (identity == _owner)
                Assert.Single(subscriber.Sink.Frames);
            else
                Assert.Empty(subscriber.Sink.Frames);
        }
    }

    [Fact]
    public void Inserts_share_too_and_match_each_mask()
    {
        var subscribers = Subscribe(services => services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>());

        var sets = _subscriptions.ColumnSetsBuilt;
        var row = new PlayerState { Id = _owner, RoomId = HideoutHidesPosition.HideoutRoom, Name = "o", X = 3 };
        _subscriptions.Fanout(Record(RowOpKind.Insert, row));

        Assert.Equal(1, _subscriptions.ColumnSetsBuilt - sets);
        AssertEveryDeltaMatchesItsOwnMask(subscribers, row, RowOpKind.Insert);
    }

    [Fact]
    public void Each_record_starts_from_an_empty_memo()
    {
        var subscribers = Subscribe(services => services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>());
        Spawn(room: HideoutHidesPosition.HideoutRoom, name: "o", x: 0);

        var sets = _subscriptions.ColumnSetsBuilt;
        for (var i = 1; i <= 3; i++)
            _subscriptions.Fanout(Record(RowOpKind.Update, new PlayerState { Id = _owner, RoomId = HideoutHidesPosition.HideoutRoom, Name = $"n{i}", X = i }));

        // One set per record, not per subscriber and not carried between records.
        Assert.Equal(3, _subscriptions.ColumnSetsBuilt - sets);
        Assert.All(subscribers.Values, subscriber => Assert.Equal(3, subscriber.Sink.Frames.Count));
    }

    [Fact]
    public void Content_equality_tells_modes_and_columns_apart()
    {
        string[] shared = ["X"];
        Assert.True(ColumnMask.All.Except("X").SameAs(ColumnMask.All.Except("X")));
        Assert.Equal(ColumnMask.All.Except("X").ContentHash(), ColumnMask.All.Except("X").ContentHash());
        Assert.False(ColumnMask.Only(shared).SameAs(ColumnMask.All.Except(shared)));
        Assert.False(ColumnMask.All.Except("X").SameAs(ColumnMask.All.Except("Y")));
        Assert.False(ColumnMask.All.SameAs(ColumnMask.None));
        Assert.True(ColumnMask.All.AdmitsAll);
        Assert.True(ColumnMask.All.Except().AdmitsAll);
        Assert.False(ColumnMask.All.Except("X").AdmitsAll);

        // A cached copy is independent of the policy's array.
        var array = new[] { "X" };
        var detached = ColumnMask.Only(array).Detached();
        array[0] = "Y";
        Assert.True(detached.Admits("X"));
        Assert.False(detached.Admits("Y"));
    }

    private static Identity Viewer(int i) => Identity.Hash($"viewer{i}");

    private void Spawn(int room, string name, float x) =>
        _engine.Invoke("Spawn", _owner, ctx => ctx.Db.Insert(new PlayerState { Id = _owner, RoomId = room, Name = name, X = x }));

    /// <summary>The owner and <see cref="Viewers"/> other callers, each subscribed to the whole table.</summary>
    private Dictionary<Identity, Subscriber> Subscribe(Action<ServiceCollection> policies)
    {
        var services = new ServiceCollection();
        policies(services);
        var set = new PolicySet(services.BuildServiceProvider(), _schema);

        var subscribers = new Dictionary<Identity, Subscriber>();
        var callers = Enumerable.Range(0, Viewers).Select(Viewer).Prepend(_owner).ToList();
        for (var i = 0; i < callers.Count; i++)
        {
            var sink = new CapturingSink();
            var context = new PolicyContext(callers[i], false, _engine.CommittedView);
            var query = SqlSubsetParser.Parse("SELECT * FROM PlayerState", null);
            var id = (uint)(i + 1);
            var subscription = _engine.ReadConsistent(head =>
                _subscriptions.Register(sink, id, query, new SubscriptionsOptions(), head, computeInitialSet: false, set, context).Subscription);
            subscribers[callers[i]] = new Subscriber(subscription, sink);
        }

        return subscribers;
    }

    /// <summary>Every subscriber received one op of <paramref name="kind"/>, shaped exactly as the per-call path shapes it.</summary>
    private void AssertEveryDeltaMatchesItsOwnMask(Dictionary<Identity, Subscriber> subscribers, PlayerState row, RowOpKind kind)
    {
        var bytes = (ReadOnlyMemory<byte>)((RowCodec<PlayerState>)_schema.Get(typeof(PlayerState)).Codec!).Serialize(in row);
        foreach (var subscriber in subscribers.Values)
        {
            var op = Assert.Single(Assert.Single(Assert.Single(subscriber.Sink.Frames).Updates).Ops);
            Assert.Equal(kind, op.Kind);
            var (expectedRow, expectedMask) = subscriber.Subscription.WireForm(bytes);
            Assert.Equal(expectedRow.ToArray(), op.Row.ToArray());
            Assert.Equal(expectedMask.ToArray(), op.ColumnMask.ToArray());
        }
    }

    private CommitRecord Record(RowOpKind kind, PlayerState row)
    {
        var table = _schema.Get(typeof(PlayerState));
        var codec = (RowCodec<PlayerState>)table.Codec!;
        return new CommitRecord
        {
            Lsn = _engine.Log.HeadLsn + (ulong)++_records,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = _owner,
            ReducerName = "test",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [new RowOp(kind, table.Id, codec.EncodePrimaryKey(in row), codec.Serialize(in row))],
            SerializedLength = 0,
        };
    }

    private sealed record Subscriber(ServerSubscription Subscription, CapturingSink Sink);

    private sealed class CapturingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }

    /// <summary>One array, two modes: <c>Only</c> for the listed callers, <c>All.Except</c> for the rest.</summary>
    private sealed class SharedArrayModes(HashSet<Identity> onlyCallers) : IColumnPolicy<PlayerState>
    {
        private static readonly string[] Shared = [nameof(PlayerState.Id), nameof(PlayerState.X)];

        public ColumnMask VisibleTo(in PlayerState row, PolicyContext ctx) =>
            onlyCallers.Contains(ctx.Caller) ? ColumnMask.Only(Shared) : ColumnMask.All.Except(Shared);
    }

    /// <summary>A cached mask instance, the shape the policy docs recommend.</summary>
    private sealed class NameHiddenFromOthers : IColumnPolicy<PlayerState>
    {
        private static readonly ColumnMask Others = ColumnMask.All.Except(nameof(PlayerState.Name));

        public ColumnMask VisibleTo(in PlayerState row, PolicyContext ctx) =>
            row.Id == ctx.Caller ? ColumnMask.All : Others;
    }
}
