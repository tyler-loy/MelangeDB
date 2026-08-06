using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The fan-out runs under the engine's write lock, so anything it computes once per subscriber
/// rather than once per row is a global stall that scales with player count. These pin the sharing
/// that keeps it once per row: one decoded column map and one key buffer, handed to every
/// subscriber that receives the op.
/// </summary>
public class FanoutSharingTests
{
    [Fact]
    public async Task Two_subscribers_to_one_table_receive_the_same_decoded_column_map()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        var (first, second) = RegisterPair(host);
        var creatureId = FirstCreatureId(host);

        host.Call("MoveCreature", creatureId, 11f);

        // Same instance, not merely equal contents: proving equality would pass even if the row
        // were decoded once per subscriber, which is the cost this exists to remove.
        Assert.Same(ColumnsOf(first), ColumnsOf(second));
        Assert.Same(KeyOf(first), KeyOf(second));
        Assert.Equal(11f, Convert.ToSingle(ColumnsOf(first)!["X"], System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task Equal_projections_converge_on_one_wire_column_set_at_registration()
    {
        // [ServerOnly] columns give Creature a non-null static wire set, so each subscription
        // compiles its own equal-but-distinct instance. Without the convergence at registration
        // the memo below would key on two different references and decode the row twice.
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        var (first, second) = RegisterPair(host, projection: "Id, X");
        var creatureId = FirstCreatureId(host);
        host.Call("MoveCreature", creatureId, 12f);

        var columns = ColumnsOf(first);
        Assert.Same(columns, ColumnsOf(second));
        Assert.Equal(["Id", "X"], columns!.Keys.OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public async Task A_subscriber_whose_columns_differ_still_gets_its_own_map()
    {
        // The memo keys on the visible column set, so a narrower projection must not be handed the
        // wider subscriber's dictionary — sharing is an optimization, never a widening of what a
        // client is allowed to see.
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        var wide = Register(subscriptions, host, 1, projection: null);
        var narrow = Register(subscriptions, host, 2, projection: "X");

        host.Call("MoveCreature", FirstCreatureId(host), 13f);

        Assert.NotSame(ColumnsOf(wide), ColumnsOf(narrow));
        Assert.Equal(["Id", "X"], ColumnsOf(wide)!.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(["X"], ColumnsOf(narrow)!.Keys);
    }

    private static (CapturingSink First, CapturingSink Second) RegisterPair(TransportTestHost host, string? projection = null)
    {
        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        return (Register(subscriptions, host, 1, projection), Register(subscriptions, host, 2, projection));
    }

    private static CapturingSink Register(SubscriptionEngine subscriptions, TransportTestHost host, uint id, string? projection)
    {
        var sink = new CapturingSink();
        var columns = projection ?? "*";
        var query = SqlSubsetParser.Parse($"SELECT {columns} FROM Creature", null);
        host.Engine.ReadConsistent(head =>
            subscriptions.Register(sink, id, query, new SubscriptionsOptions(), head, computeInitialSet: false));
        return sink;
    }

    private static ulong FirstCreatureId(TransportTestHost host)
    {
        Assert.True(host.Engine.Schema.TryGetByName("Creature", out var schema));
        var row = host.Engine.HotStore.Scan(schema.Id).First();
        var columns = RowWire.ToColumns(schema, row.Value.Span, null);
        return Convert.ToUInt64(columns["Id"], System.Globalization.CultureInfo.InvariantCulture);
    }

    private static IReadOnlyDictionary<string, object?>? ColumnsOf(CapturingSink sink) => OpOf(sink).Columns;

    private static byte[] KeyOf(CapturingSink sink) => OpOf(sink).Key;

    private static WireRowOp OpOf(CapturingSink sink)
    {
        var frame = Assert.Single(sink.Frames);
        return Assert.Single(Assert.Single(frame.Updates).Ops);
    }

    private sealed class FanoutObserver(SubscriptionEngine subscriptions) : ICommitObserver
    {
        public void OnCommit(CommitRecord record) => subscriptions.Fanout(record);
    }

    private sealed class CapturingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }
}
