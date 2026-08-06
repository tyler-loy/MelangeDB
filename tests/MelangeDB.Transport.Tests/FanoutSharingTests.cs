using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The fan-out runs under the engine's write lock, so anything it computes once per subscriber
/// rather than once per row is a global stall that scales with player count. These pin the sharing
/// that keeps it once per row: one wire row and one key buffer, handed to every subscriber that
/// receives the op.
/// <para>
/// Under protocol v2 the shared thing is a span of bytes rather than a decoded dictionary, so
/// "same instance" becomes "same memory" — which is a stronger claim, not a weaker one: it is
/// satisfied only if the second subscriber's row aliases the first's rather than copying it.
/// </para>
/// </summary>
public class FanoutSharingTests
{
    [Fact]
    public async Task Two_subscribers_to_one_table_receive_the_same_wire_row_memory()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        var (first, second) = RegisterPair(host);
        var creatureId = FirstCreatureId(host);

        host.Call("MoveCreature", creatureId, 11f);

        // The same bytes, not merely equal ones: proving equality would pass even if the row were
        // projected once per subscriber, which is the cost this exists to remove.
        AssertSameMemory(RowOf(first.Sink), RowOf(second.Sink));
        Assert.Same(KeyOf(first.Sink), KeyOf(second.Sink));
        Assert.Equal(11f, Decode(first)["X"]);
    }

    [Fact]
    public async Task Equal_projections_converge_on_one_wire_column_set_at_registration()
    {
        // [ServerOnly] columns give Creature a non-null static wire set, so each subscription
        // compiles its own equal-but-distinct instance. Without the convergence at registration
        // the memo below would key on two different references and project the row twice.
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        var (first, second) = RegisterPair(host, projection: "Id, X");
        var creatureId = FirstCreatureId(host);
        host.Call("MoveCreature", creatureId, 12f);

        AssertSameMemory(RowOf(first.Sink), RowOf(second.Sink));
        Assert.Equal(["Id", "X"], ColumnNames(first));
    }

    [Fact]
    public async Task A_subscriber_whose_columns_differ_still_gets_its_own_row()
    {
        // The memo keys on the visible column set, so a narrower projection must not be handed the
        // wider subscriber's bytes — sharing is an optimization, never a widening of what a client
        // is allowed to see.
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        var wide = Register(subscriptions, host, 1, projection: null);
        var narrow = Register(subscriptions, host, 2, projection: "X");

        host.Call("MoveCreature", FirstCreatureId(host), 13f);

        Assert.False(SameMemory(RowOf(wide.Sink), RowOf(narrow.Sink)));
        Assert.Equal(["Id", "X"], ColumnNames(wide));
        Assert.Equal(["X"], ColumnNames(narrow));
        Assert.Equal(["X"], Decode(narrow).Keys);
    }

    [Fact]
    public async Task An_unprojected_subscriber_is_handed_the_committed_row_bytes_untouched()
    {
        // Protocol v2's whole claim on the fan-out path: a table with nothing to hide sends the
        // bytes that were committed, so a full row costs the encoder nothing at all — not a
        // decode, not a dictionary, not even a copy. Creature cannot show this (its [ServerOnly]
        // columns mean it is always projected), so this uses PlayerState, which has none.
        await using var host = await TransportTestHost.StartAsync();
        host.Call("Spawn", "mover", 1);

        var subscriptions = new SubscriptionEngine(host.Engine, null);
        var records = new List<CommitRecord>();
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        host.Engine.AddCommitObserver(new RecordingObserver(records));
        var watcher = Register(subscriptions, host, 1, projection: null, table: "PlayerState");

        host.Call("Move", 5f);

        var committed = Assert.Single(records.Last().WriteSet).Row;
        AssertSameMemory(committed, RowOf(watcher.Sink));
    }

    private static (Registration First, Registration Second) RegisterPair(TransportTestHost host, string? projection = null)
    {
        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        return (Register(subscriptions, host, 1, projection), Register(subscriptions, host, 2, projection));
    }

    private static Registration Register(
        SubscriptionEngine subscriptions,
        TransportTestHost host,
        uint id,
        string? projection,
        string table = "Creature")
    {
        var sink = new CapturingSink();
        var query = SqlSubsetParser.Parse($"SELECT {projection ?? "*"} FROM {table}", null);
        ServerSubscription? subscription = null;
        host.Engine.ReadConsistent(head =>
            subscription = subscriptions.Register(sink, id, query, new SubscriptionsOptions(), head, computeInitialSet: false).Subscription);
        return new Registration(sink, subscription!);
    }

    private static ulong FirstCreatureId(TransportTestHost host)
    {
        Assert.True(host.Engine.Schema.TryGetByName("Creature", out var schema));
        var row = host.Engine.HotStore.Scan(schema.Id).First();
        var columns = RowWire.ToColumns(schema, row.Value.Span, null);
        return Convert.ToUInt64(columns["Id"], System.Globalization.CultureInfo.InvariantCulture);
    }

    private static IReadOnlyList<string> ColumnNames(Registration registration) =>
        [.. registration.Subscription.Descriptor.Columns.Select(c => c.Name)];

    private static Dictionary<string, object?> Decode(Registration registration)
    {
        var op = OpOf(registration.Sink);
        return WireRowValues.ToColumns(registration.Subscription.Descriptor, op.Row.Span, op.ColumnMask.Span);
    }

    private static ReadOnlyMemory<byte> RowOf(CapturingSink sink) => OpOf(sink).Row;

    private static byte[] KeyOf(CapturingSink sink) => OpOf(sink).Key;

    private static WireRowOp OpOf(CapturingSink sink)
    {
        var frame = Assert.Single(sink.Frames);
        return Assert.Single(Assert.Single(frame.Updates).Ops);
    }

    /// <summary>Whether two spans are the very same bytes in memory, not merely equal ones.</summary>
    private static bool SameMemory(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right) =>
        left.Length == right.Length
        && !left.IsEmpty
        && left.Span.Overlaps(right.Span, out var offset)
        && offset == 0;

    private static void AssertSameMemory(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right) =>
        Assert.True(SameMemory(left, right), "The two wire rows are separate buffers; the fan-out projected the row twice.");

    private sealed record Registration(CapturingSink Sink, ServerSubscription Subscription);

    private sealed class FanoutObserver(SubscriptionEngine subscriptions) : ICommitObserver
    {
        public void OnCommit(CommitRecord record) => subscriptions.Fanout(record);
    }

    private sealed class RecordingObserver(List<CommitRecord> records) : ICommitObserver
    {
        public void OnCommit(CommitRecord record) => records.Add(record);
    }

    private sealed class CapturingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }
}
