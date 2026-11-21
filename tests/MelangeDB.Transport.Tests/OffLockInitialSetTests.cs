using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A subscription's initial set is collected off the engine's write lock, from a view pinned at
/// <c>h1</c>, while the subscription engine records the commits after it; under one short lock
/// hold at <c>h2</c> those commits are folded into the set and the subscription registers (#213).
/// What has to hold is that the result is indistinguishable from collecting under the lock at
/// <c>h2</c>: same rows, same bytes, anchored at the LSN the live stream starts after.
/// </summary>
public class OffLockInitialSetTests
{
    private const string RoomOne = "SELECT * FROM PlayerState WHERE RoomId = 1";

    [Fact]
    public async Task Commits_landing_between_the_pin_and_registration_are_folded_in_exactly()
    {
        await using var host = await TransportTestHost.StartAsync();
        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        var limits = new SubscriptionsOptions();
        Assert.True(host.Engine.SupportsReadViews);
        Spawn(host, "p1", 1);
        Spawn(host, "p2", 1);
        Spawn(host, "p3", 2);

        var sink = new CapturingSink();
        var subscription = subscriptions.Compile(sink, 1, SqlSubsetParser.Parse(RoomOne, null), limits, null, null);
        SubscriptionEngine.CatchUp? catchUp = null;
        var collected = host.Engine.ReadAtHead(
            (head, store) =>
            {
                var set = subscriptions.CollectInitialSet(subscription, limits, store, head);

                // From another thread, while the set is in hand: under the lock these would wait
                // for the read to finish, and the read waits for them, so the timeout is the
                // proof the read is off the lock. The pinned read must have seen none of them.
                using var landed = new ManualResetEventSlim();
                Exception? failure = null;
                var writer = new Thread(() =>
                {
                    try
                    {
                        Spawn(host, "p1", 2);           // leaves scope
                        Spawn(host, "p3", 1);           // enters scope
                        Spawn(host, "p4", 1);           // new, in scope
                        host.Reducers.Call("Move", Identity.Hash("p2"), 5f); // updated in scope
                    }
                    catch (Exception exception)
                    {
                        failure = exception;
                    }
                    finally
                    {
                        landed.Set();
                    }
                });
                writer.Start();
                Assert.True(landed.Wait(TimeSpan.FromSeconds(10)), "commits could not land while the initial set was being read");
                Assert.Null(failure);
                return set;
            },
            atPin: _ => catchUp = subscriptions.BeginCatchUp());

        Assert.Equal(2, collected.Rows.Count); // p1, p2 as of the pin.

        var registered = host.Engine.ReadConsistent(head =>
        {
            subscriptions.EndCatchUp(catchUp!);
            var current = subscriptions.CatchUpInitialSet(subscription, limits, collected, catchUp!, head);
            subscriptions.Add(subscription);
            return current;
        });

        // The locked path, at the same head, is the reference.
        var reference = host.Engine.ReadConsistent(head =>
            subscriptions.Register(new CapturingSink(), 2, SqlSubsetParser.Parse(RoomOne, null), limits, head, computeInitialSet: true).InitialSet);

        Assert.Equal(reference.AnchorLsn, registered.AnchorLsn);
        Assert.Equal(Rows(reference), Rows(registered));
        Assert.Equal(reference.Bytes, registered.Bytes);
        Assert.Equal(["p2", "p3", "p4"], Names(host, registered));

        // And the live stream picks up exactly after the anchor.
        host.Reducers.Call("Move", Identity.Hash("p4"), 9f);
        var frame = Assert.Single(sink.Frames);
        Assert.True(frame.Lsn > registered.AnchorLsn);
    }

    [Fact]
    public async Task The_row_ceiling_is_checked_again_after_the_catch_up()
    {
        await using var host = await TransportTestHost.StartAsync();
        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));
        var limits = new SubscriptionsOptions { MaxRowsPerSubscription = 3 };
        Spawn(host, "p1", 1);
        Spawn(host, "p2", 1);

        var subscription = subscriptions.Compile(new CapturingSink(), 1, SqlSubsetParser.Parse(RoomOne, null), limits, null, null);
        SubscriptionEngine.CatchUp? catchUp = null;
        var collected = host.Engine.ReadAtHead(
            (head, store) =>
            {
                var set = subscriptions.CollectInitialSet(subscription, limits, store, head);
                Spawn(host, "p3", 1);
                Spawn(host, "p4", 1);
                return set;
            },
            atPin: _ => catchUp = subscriptions.BeginCatchUp());

        var rejected = Assert.Throws<SubscriptionRejectedException>(() => host.Engine.ReadConsistent(head =>
        {
            subscriptions.EndCatchUp(catchUp!);
            return subscriptions.CatchUpInitialSet(subscription, limits, collected, catchUp!, head);
        }));
        Assert.Equal(MelangeErrorCodes.TooManyRows, rejected.Code);
    }

    [Fact]
    public async Task A_finished_catch_up_records_nothing_further()
    {
        await using var host = await TransportTestHost.StartAsync();
        var subscriptions = new SubscriptionEngine(host.Engine, null);
        host.Engine.AddCommitObserver(new FanoutObserver(subscriptions));

        var catchUp = host.Engine.ReadConsistent(_ => subscriptions.BeginCatchUp());
        Spawn(host, "p1", 1);
        host.Engine.ReadConsistent(_ => subscriptions.EndCatchUp(catchUp));
        Spawn(host, "p2", 1);

        Assert.Single(catchUp.Records);
    }

    private static void Spawn(TransportTestHost host, string name, int room) =>
        host.Reducers.Call("Spawn", Identity.Hash(name), name, room);

    private static Dictionary<string, string> Rows(InitialSet set) =>
        set.Rows.ToDictionary(static pair => Convert.ToHexString(pair.Key.Span), static pair => Convert.ToHexString(pair.Value.Span));

    private static List<string> Names(TransportTestHost host, InitialSet set)
    {
        Assert.True(host.Engine.Schema.TryGetByName("PlayerState", out var schema));
        return set.Rows
            .Select(pair => (string)RowWire.ToColumns(schema, pair.Value.Span, null)["Name"]!)
            .Order(StringComparer.Ordinal)
            .ToList();
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
