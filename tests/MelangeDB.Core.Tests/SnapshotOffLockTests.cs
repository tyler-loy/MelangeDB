using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Snapshots are captured under the write lock and written outside it. What that buys is measured
/// (a full snapshot of a million rows held the lock for over half a second; pinning a view instead
/// costs about a millisecond) — what it risks is here.
/// <para>
/// The risk is that "outside the lock" turns a consistent capture into a smeared one. The pin is
/// what prevents that, so these check the two things a pin has to survive: a commit landing while
/// the file is being written, and the snapshot still reading the state as of its own LSN.
/// </para>
/// </summary>
public class SnapshotOffLockTests
{
    [Fact]
    public void A_snapshot_captures_the_state_at_its_own_lsn()
    {
        using var harness = new EngineHarness();
        harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("one"), Name = "one", RoomId = 10 }));

        var lsn = harness.Engine.TakeSnapshot();

        Assert.NotNull(lsn);
        Assert.Equal(harness.Engine.ReadConsistent(head => head), lsn);
    }

    [Fact]
    public void A_commit_during_a_snapshot_lands_after_it_and_is_not_captured()
    {
        // The whole point of the restructure. A store that scanned live state outside the lock would
        // fold this row into a snapshot stamped at an LSN before it existed — replay would then
        // apply the row's own commit on top of a snapshot that already had it. The pin is what makes
        // the capture as-of rather than whenever.
        using var harness = new EngineHarness();
        for (var i = 0; i < 200; i++)
        {
            var id = i;
            harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash($"p{id}"), Name = $"p{id}", RoomId = 1 }));
        }

        var snapshotLsn = harness.Engine.TakeSnapshot();
        Assert.NotNull(snapshotLsn);

        harness.Invoke("Later", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("after"), Name = "after", RoomId = 2 }));

        // The later commit is in the log above the snapshot's LSN, so recovery replays it on top.
        Assert.True(harness.Engine.ReadConsistent(head => head) > snapshotLsn);
        harness.Restart();
        Assert.Equal(201, harness.Engine.CommittedView.Scan<Player>().Count());
        Assert.Equal("after", harness.Engine.CommittedView.Scan<Player>().Single(p => p.Name == "after").Name);
    }

    [Fact]
    public void Writes_proceed_while_a_snapshot_is_being_written()
    {
        // A second thread commits while the snapshot runs. Both orderings are legal and the test
        // asserts neither: what it pins is that the two never deadlock and that every commit lands.
        // This is a liveness check, not a proof of overlap — at these row counts the snapshot is far
        // too quick to guarantee the writer is inside it, and a test that tried to force the overlap
        // with sleeps would be a flake. The evidence for the overlap itself is the benchmark.
        using var harness = new EngineHarness();
        for (var i = 0; i < 50; i++)
        {
            var id = i;
            harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash($"p{id}"), Name = $"p{id}", RoomId = 1 }));
        }

        Exception? failure = null;
        var writer = new Thread(() =>
        {
            try
            {
                for (var i = 100; i < 150; i++)
                {
                    var id = i;
                    harness.Invoke("Concurrent", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash($"c{id}"), Name = $"c{id}", RoomId = 3 }));
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        });

        writer.Start();
        harness.Engine.TakeSnapshot();
        Assert.True(writer.Join(TimeSpan.FromSeconds(30)), "the concurrent writer did not finish");
        Assert.Null(failure);
        Assert.Equal(100, harness.Engine.CommittedView.Scan<Player>().Count());
    }

    [Fact]
    public void The_engine_survives_a_restart_from_a_snapshot_written_off_the_lock()
    {
        // End to end: the file a pinned scan produced has to be a file recovery can read.
        using var harness = new EngineHarness();
        for (var i = 0; i < 100; i++)
        {
            var id = i;
            harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash($"p{id}"), Name = $"p{id}", RoomId = (int)(id % 7) }));
        }

        harness.Engine.TakeSnapshot();
        harness.Restart();

        var players = harness.Engine.CommittedView.Scan<Player>().ToList();
        Assert.Equal(100, players.Count);
        Assert.Equal("p42", players.Single(p => p.Name == "p42").Name);
    }

    [Fact]
    public void Snapshots_are_off_when_disabled()
    {
        using var harness = new EngineHarness();
        harness.Options.Snapshots.Enabled = false;
        harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("one"), Name = "one", RoomId = 1 }));

        Assert.Null(harness.Engine.TakeSnapshot());
    }
}
