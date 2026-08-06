using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <see cref="Isolation.Snapshot"/>: the body runs outside the engine's write lock against a read
/// view pinned at one LSN, and only reconcile, the guards, and the append serialize.
/// <para>
/// Two properties are under test throughout, and they pull in opposite directions on purpose. The
/// feature's <em>benefit</em> is that a long body no longer blocks other writers. The feature's
/// <em>cost</em> is that the body's reads are advisory, so a read-modify-write silently loses an
/// update. Both are asserted here, the second as deliberately as the first — a documented hazard
/// that no test pins down is a hazard one refactor away from becoming a surprise.
/// </para>
/// </summary>
public class SnapshotIsolationTests : IDisposable
{
    private static readonly TableId Players = TableId.FromName("Player");

    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void A_snapshot_body_does_not_hold_the_write_lock()
    {
        Join("resident", room: 1);

        using var bodyEntered = new ManualResetEventSlim();
        using var otherCommitted = new ManualResetEventSlim();
        var otherFailed = default(Exception);

        var other = new Thread(() =>
        {
            // Only starts once the snapshot body is provably mid-flight. Under Serialized this
            // commit cannot happen until the body returns, so the body's wait below would deadlock
            // against it — which is precisely the stall the isolation level removes.
            bodyEntered.Wait(TimeSpan.FromSeconds(10));
            try
            {
                Join("interloper", room: 2);
                otherCommitted.Set();
            }
            catch (Exception exception)
            {
                otherFailed = exception;
            }
        })
        {
            IsBackground = true,
        };
        other.Start();

        _harness.InvokeSnapshot("Sweep", ctx =>
        {
            bodyEntered.Set();
            Assert.True(
                otherCommitted.Wait(TimeSpan.FromSeconds(10)),
                "a serialized transaction could not commit while a snapshot body was running, so the body was holding the write lock");
            ctx.Db.Insert(new Player { Id = Identity.Hash("swept"), RoomId = 3, Name = "swept" });
        });

        Assert.True(other.Join(TimeSpan.FromSeconds(10)), "the writer thread did not stop");
        Assert.Null(otherFailed);
        Assert.Equal(3, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void A_snapshot_body_reads_the_lsn_it_pinned_and_not_a_later_one()
    {
        Join("first", room: 1);

        long observedBefore = 0;
        long observedAfter = 0;
        _harness.InvokeSnapshot("Sweep", ctx =>
        {
            observedBefore = ctx.Db.Count<Player>();
            CommitFromAnotherThread(() => Join("second", room: 1));
            // The pin is the point: the body cannot see the row that landed underneath it, however
            // long it runs and whenever it looks.
            observedAfter = ctx.Db.Count<Player>();
        });

        Assert.Equal(1, observedBefore);
        Assert.Equal(1, observedAfter);
        Assert.Equal(2, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void Read_your_writes_still_works_inside_a_snapshot_body()
    {
        _harness.InvokeSnapshot("Sweep", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("fresh"), RoomId = 4, Name = "fresh" });

            // The write-set overlay is transaction-local and has nothing to do with which store
            // view the reads fall through to, so pinning the store changes none of this.
            Assert.NotNull(ctx.Db.Find<Player>(Identity.Hash("fresh")));
            Assert.Equal(1, ctx.Db.Count<Player>());
            Assert.Single(ctx.Db.Filter<Player>("RoomId", 4));
        });

        Assert.Equal(1, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void An_update_of_a_row_deleted_under_the_body_is_reconciled_into_an_insert()
    {
        Join("doomed", room: 1);

        _harness.InvokeSnapshot("Sweep", ctx =>
        {
            var player = ctx.Db.Find<Player>(Identity.Hash("doomed"))!.Value;
            CommitFromAnotherThread(() =>
                _harness.Invoke("Delete", inner => Assert.True(inner.Db.Delete<Player>(Identity.Hash("doomed")))));

            // Still visible through the pin, so the body updates it in good faith. Committing that
            // update verbatim would apply an update to a row that no longer exists.
            ctx.Db.Update(player with { RoomId = 9 });
        });

        var survivor = _harness.Engine.CommittedView.Find<Player>(Identity.Hash("doomed"));
        Assert.NotNull(survivor);
        Assert.Equal(9, survivor!.Value.RoomId);
        Assert.Equal(RowOpKind.Insert, LastOpKind());
    }

    [Fact]
    public void A_delete_of_a_row_already_deleted_drops_and_commits_nothing()
    {
        Join("doomed", room: 1);
        var lsnBefore = _harness.Engine.Log.HeadLsn;

        _harness.InvokeSnapshot("Sweep", ctx =>
        {
            Assert.NotNull(ctx.Db.Find<Player>(Identity.Hash("doomed")));
            CommitFromAnotherThread(() =>
                _harness.Invoke("Delete", inner => Assert.True(inner.Db.Delete<Player>(Identity.Hash("doomed")))));
            ctx.Db.Delete<Player>(Identity.Hash("doomed"));
        });

        // The interloping delete is the only new record: reconcile dropped the body's op entirely
        // rather than logging a delete of nothing, which subscription fan-out would have reported.
        Assert.Equal(lsnBefore + 1, _harness.Engine.Log.HeadLsn);
        Assert.Equal(0, _harness.Engine.HotStore.Count(Players));
    }

    /// <summary>
    /// The documented hazard, asserted rather than merely warned about. Reconcile fixes op
    /// <em>shape</em>, never op <em>value</em> — so an increment computed against a stale read is
    /// written verbatim and the concurrent increment is gone. This is the whole reason the
    /// eligibility rule leads the feature's documentation and the reason the flag is never inferred.
    /// </summary>
    [Fact]
    public void A_read_modify_write_under_snapshot_isolation_loses_the_concurrent_update()
    {
        _harness.Invoke("Seed", ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash("counter"), RoomId = 0, Name = "counter" }));

        _harness.InvokeSnapshot("Increment", ctx =>
        {
            var player = ctx.Db.Find<Player>(Identity.Hash("counter"))!.Value;

            // A serialized increment lands and commits while this body holds a stale read of 0.
            CommitFromAnotherThread(() => _harness.Invoke("Increment", inner =>
            {
                var concurrent = inner.Db.Find<Player>(Identity.Hash("counter"))!.Value;
                inner.Db.Update(concurrent with { RoomId = concurrent.RoomId + 1 });
            }));

            ctx.Db.Update(player with { RoomId = player.RoomId + 1 });
        });

        // Two increments ran; the counter reads 1. Both wrote a defensible value from what they
        // read, and last-writer-wins kept the wrong one. Nothing errored, and nothing could have.
        Assert.Equal(1, _harness.Engine.CommittedView.Find<Player>(Identity.Hash("counter"))!.Value.RoomId);
    }

    [Fact]
    public void Concurrent_snapshot_bodies_never_allocate_the_same_autoinc_id()
    {
        const int threads = 8;
        const int perThread = 25;
        var ids = new System.Collections.Concurrent.ConcurrentBag<ulong>();
        var failures = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        using var start = new ManualResetEventSlim();

        var workers = Enumerable.Range(0, threads).Select(t => new Thread(() =>
        {
            start.Wait(TimeSpan.FromSeconds(10));
            for (var i = 0; i < perThread; i++)
            {
                try
                {
                    _harness.InvokeSnapshot("Grant", ctx =>
                    {
                        var item = ctx.Db.Insert(new InventoryItem
                        {
                            Owner = EngineHarness.Caller,
                            ItemName = $"t{t}-{i}",
                            Quantity = 1,
                        });
                        ids.Add(item.Id);
                    });
                }
                catch (Exception exception)
                {
                    failures.Add(exception);
                }
            }
        })
        {
            IsBackground = true,
        }).ToList();

        foreach (var worker in workers)
            worker.Start();
        start.Set();
        foreach (var worker in workers)
            Assert.True(worker.Join(TimeSpan.FromSeconds(60)), "an allocating thread did not finish");

        Assert.Empty(failures);
        // Staged allocation would hand the same id to two concurrent bodies here: each peeks the
        // same sequence and neither consumes it until commit. Snapshot transactions reserve on
        // allocation instead, so the ids are unique — gappy under abort, which is the stated
        // contract, but never repeated.
        var allocated = ids.ToList();
        Assert.Equal(threads * perThread, allocated.Count);
        Assert.Equal(allocated.Count, allocated.Distinct().Count());
        Assert.Equal(allocated.Count, _harness.Engine.HotStore.Count(TableId.FromName("InventoryItem")));
    }

    [Fact]
    public void A_snapshot_body_that_throws_appends_nothing_and_leaves_no_trace()
    {
        Join("kept", room: 1);
        var lsnBefore = _harness.Engine.Log.HeadLsn;

        Assert.Throws<InvalidOperationException>(() => _harness.InvokeSnapshot("Sweep", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("discarded"), RoomId = 2, Name = "discarded" });
            throw new InvalidOperationException("no");
        }));

        Assert.Equal(lsnBefore, _harness.Engine.Log.HeadLsn);
        Assert.Equal(1, _harness.Engine.HotStore.Count(Players));
        Assert.Null(_harness.Engine.CommittedView.Find<Player>(Identity.Hash("discarded")));
    }

    [Fact]
    public void A_nested_call_inside_a_snapshot_body_is_still_forbidden()
    {
        var thrown = Assert.Throws<InvalidOperationException>(() => _harness.InvokeSnapshot("Outer", _ =>
            _harness.Invoke("Inner", inner =>
                inner.Db.Insert(new Player { Id = Identity.Hash("nested"), RoomId = 1, Name = "nested" }))));

        Assert.Contains("Nested reducer calls are forbidden", thrown.Message);
        Assert.Equal(0, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void A_snapshot_transaction_commits_a_record_whose_write_set_applies()
    {
        Join("first", room: 1);
        _harness.InvokeSnapshot("Sweep", ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash("second"), RoomId = 2, Name = "second" }));

        // Recovery replays the write set, not the body — so a snapshot transaction has to survive a
        // restart exactly as a serialized one does, or the isolation level would be a durability
        // change wearing a latency change's clothes.
        var before = _harness.Dump();
        _harness.Restart();
        Assert.Equal(before, _harness.Dump());
        Assert.Equal(2, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void A_store_without_pinned_reads_runs_snapshot_reducers_serialized_and_says_so_once()
    {
        using var logs = new LogCapture();
        using var degraded = new EngineHarness(loggerFactory: logs, storeProvider: ReadViewlessStore.Provider);

        degraded.InvokeSnapshot("Sweep", ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash("a"), RoomId = 1, Name = "a" }));
        degraded.InvokeSnapshot("Sweep", ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash("b"), RoomId = 1, Name = "b" }));

        // Correct, just not faster: isolation is a latency property, so degrading beats refusing to
        // start. What it must not be is silent.
        Assert.Equal(2, degraded.Engine.HotStore.Count(Players));
        var entry = logs.Single(1004);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal("SnapshotIsolationUnavailable", entry.EventName);
        Assert.Equal(nameof(ReadViewlessStore), entry.Fields["Store"]);
        Assert.Contains("run serialized", entry.Message);
    }

    [Fact]
    public void The_1003_threshold_is_the_locked_portion_so_a_long_snapshot_body_does_not_warn()
    {
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 50;

        logged.InvokeSnapshot("Sweep", ctx =>
        {
            Thread.Sleep(120);
            ctx.Db.Insert(new Player { Id = Identity.Hash("swept"), RoomId = 1, Name = "swept" });
        });

        // 120 ms of body against a 50 ms threshold, and no warning — because the body blocked
        // nobody. Thresholding on total duration here would page an operator about write latency
        // that did not happen, which is the whole reason the threshold moved.
        Assert.DoesNotContain(logs.Entries, e => e.EventId == 1003);
    }

    [Fact]
    public void A_slow_snapshot_commit_still_warns_and_the_entry_says_which_isolation_it_was()
    {
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 0;

        logged.InvokeSnapshot("Sweep", ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash("swept"), RoomId = 1, Name = "swept" }));

        var entry = logs.Single(1003);
        Assert.Equal("snapshot", entry.Fields["Isolation"]);
        // Both numbers present and distinguishable: LockedMs is global write latency, DurationMs is
        // what the reducer cost. A dashboard that cannot tell them apart cannot tell a 500 ms
        // serialized transaction from a 500 ms snapshot one that stalled nothing.
        Assert.True(entry.Number("LockedMs") <= entry.Number("DurationMs"));
        Assert.Contains("held the write lock", entry.Message);
    }

    private RowOpKind LastOpKind()
    {
        var head = _harness.Engine.Log.HeadLsn;
        var record = _harness.Engine.Log.ReadFrom(head).Single();
        return record.WriteSet.Single().Kind;
    }

    /// <summary>
    /// Runs a committing transaction to completion on another thread and waits for it. Inside a
    /// snapshot body this models a concurrent writer; the same call inside a serialized body would
    /// deadlock, which is itself the distinction being tested.
    /// </summary>
    private static void CommitFromAnotherThread(Action commit)
    {
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            try
            {
                commit();
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        })
        {
            IsBackground = true,
        };
        thread.Start();
        Assert.True(thread.Join(TimeSpan.FromSeconds(10)), "the concurrent transaction never finished");
        Assert.Null(failure);
    }

    private void Join(string name, int room) =>
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player
        {
            Id = Identity.Hash(name),
            RoomId = room,
            Name = name,
        }));
}
