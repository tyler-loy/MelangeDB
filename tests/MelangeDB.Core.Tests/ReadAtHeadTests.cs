using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <see cref="MelangeEngine.ReadAtHead{T}"/> is how a whole-shard scan that ships its result
/// elsewhere (a border reset, a Replicated bootstrap) stays off the write lock. Issue #208: the
/// border reset scanned every row of a paged million-row shard under the lock, so every commit
/// and the node's heartbeat waited minutes behind it and the hub fenced a live node.
/// <para>
/// Two things have to hold: commits proceed while the read runs, and the read still sees exactly
/// the state at the LSN it was handed, because the caller resumes a log stream from that LSN.
/// </para>
/// </summary>
public class ReadAtHeadTests
{
    [Fact]
    public void A_commit_lands_while_the_read_is_still_running()
    {
        using var harness = new EngineHarness();
        harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("one"), Name = "one", RoomId = 1 }));
        var players = harness.Engine.Schema.Get(typeof(Player)).Id;

        using var committed = new ManualResetEventSlim();
        Exception? writerFailure = null;
        ulong readHead = 0;
        var rowsSeen = -1;

        harness.Engine.ReadAtHead((head, store) =>
        {
            readHead = head;

            // A commit from another thread, while this read is still open. Under the write lock
            // this would wait for the read to finish, and the read waits for it: the timeout below
            // is the deadlock.
            var writer = new Thread(() =>
            {
                try
                {
                    harness.Invoke("During", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("two"), Name = "two", RoomId = 2 }));
                }
                catch (Exception exception)
                {
                    writerFailure = exception;
                }
                finally
                {
                    committed.Set();
                }
            });
            writer.Start();
            Assert.True(committed.Wait(TimeSpan.FromSeconds(10)), "a commit could not land while ReadAtHead was reading");

            // The commit has landed, and the read still sees the state at its own LSN.
            rowsSeen = store.Scan(players).Count();
        });

        Assert.Null(writerFailure);
        Assert.Equal(1, rowsSeen);
        Assert.True(harness.Engine.ReadConsistent(head => head) > readHead);
        Assert.Equal(2, harness.Engine.CommittedView.Scan<Player>().Count());
    }

    [Fact]
    public void The_lsn_handed_to_the_read_is_the_head_its_state_reflects()
    {
        using var harness = new EngineHarness();
        for (var i = 0; i < 5; i++)
        {
            var id = i;
            harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash($"p{id}"), Name = $"p{id}", RoomId = 1 }));
        }

        var players = harness.Engine.Schema.Get(typeof(Player)).Id;
        var (head, rows) = harness.Engine.ReadAtHead((lsn, store) => (lsn, store.Scan(players).Count()));

        Assert.Equal(harness.Engine.ReadConsistent(lsn => lsn), head);
        Assert.Equal(5, rows);
    }
}
