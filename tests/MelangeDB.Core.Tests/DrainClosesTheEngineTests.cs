using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Graceful shutdown's first step is <see cref="MelangeEngine.Drain"/>, and issue #145 is what
/// happens when it only waits. Waiting establishes that nothing is being written *now*; it says
/// nothing about the caller who is already past every check and merely queued on the lock. That
/// caller's commit lands after <see cref="MelangeEngine.Checkpoint"/> flushed and after the host
/// announced the LSN it flushed at — which makes the announcement false, and leaves a writer
/// inside the store while the host disposes it. These pin the drain as a one-way close.
/// </summary>
public class DrainClosesTheEngineTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void A_reducer_arriving_after_the_drain_is_refused_and_appends_nothing()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player
        {
            Id = Identity.Hash("before"), RoomId = 1, X = 0f, Y = 0f, Name = "Before",
        }));

        _harness.Engine.Drain();
        var announced = _harness.Engine.Log.HeadLsn;

        var refused = Assert.Throws<TransientRejectionException>(() =>
            _harness.Invoke("TooLate", ctx => ctx.Db.Insert(new Player
            {
                Id = Identity.Hash("after"), RoomId = 1, X = 0f, Y = 0f, Name = "After",
            })));

        Assert.Contains("TooLate", refused.Message);
        // The announced LSN is final: this is the whole point of the close.
        Assert.Equal(announced, _harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void The_reducer_body_does_not_run_at_all()
    {
        _harness.Engine.Drain();

        var ran = false;
        Assert.Throws<TransientRejectionException>(() => _harness.Invoke("TooLate", _ => ran = true));

        // Refused at the top of the locked region, before the body and before anything is staged —
        // so a reducer with side effects outside the database does not half-happen on the way down.
        Assert.False(ran);
    }

    [Fact]
    public void A_snapshot_isolation_reducer_is_refused_at_its_commit()
    {
        _harness.Engine.Drain();
        var announced = _harness.Engine.Log.HeadLsn;

        // Its body runs against a pinned view outside the lock, so the refusal necessarily comes
        // later than the serialized path's — but it still commits nothing.
        Assert.Throws<TransientRejectionException>(() =>
            _harness.InvokeSnapshot("TooLateSnapshot", ctx => ctx.Db.Insert(new Player
            {
                Id = Identity.Hash("after-snapshot"), RoomId = 1, X = 0f, Y = 0f, Name = "After",
            })));

        Assert.Equal(announced, _harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void Bulk_ingestion_is_refused_too()
    {
        _harness.Engine.Drain();
        var announced = _harness.Engine.Log.HeadLsn;

        Assert.Throws<TransientRejectionException>(() => _harness.Engine.BulkInsert(
            EngineHarness.Caller,
            [new BulkRow(nameof(Player), new Dictionary<string, object?>
            {
                ["Id"] = Identity.Hash("bulk-after"),
                ["RoomId"] = 1L,
                ["X"] = 0d,
                ["Y"] = 0d,
                ["Name"] = "After",
            })]));

        Assert.Equal(announced, _harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void The_refusal_is_transient_so_a_client_is_told_to_retry_rather_than_that_it_faulted()
    {
        _harness.Engine.Drain();

        var refused = Assert.Throws<TransientRejectionException>(() => _harness.Invoke("TooLate", _ => { }));

        // TransientRejectionException maps to the `transient` error code on both the socket and the
        // HTTP endpoints: a shutting-down node is a condition the system designed and that clears
        // on the next process, not a server fault the caller should report.
        Assert.IsAssignableFrom<InvalidOperationException>(refused);
    }

    [Fact]
    public void Reads_still_work_after_the_drain()
    {
        var alice = Identity.Hash("alice");
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player
        {
            Id = alice, RoomId = 1, X = 1f, Y = 2f, Name = "Alice",
        }));

        _harness.Engine.Drain();

        // The close is a write gate, not a general shutter: checkpointing runs after the drain and
        // reads the store, and so does anything reporting final state.
        var head = _harness.Engine.ReadConsistent(lsn => lsn);
        Assert.Equal(_harness.Engine.Log.HeadLsn, head);
        _harness.Engine.Checkpoint();
    }

    [Fact]
    public void Draining_twice_is_harmless()
    {
        _harness.Engine.Drain();
        _harness.Engine.Drain();
        Assert.Throws<TransientRejectionException>(() => _harness.Invoke("TooLate", _ => { }));
    }
}
