using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// A shard node disposes a shard's engine when the shard is released, drained, or the node stops,
/// and client calls are still arriving on that node's sockets. Issue #202: each one hit the
/// engine's disposed nested-call flag and surfaced as an <see cref="ObjectDisposedException"/>,
/// which the transport reports to the client as an internal error and logs at error level, 547
/// times in one failover trial. A disposed engine must refuse the way a drained one does:
/// retryable, with the reducer named, and nothing committed.
/// </summary>
public class DisposedEngineRefusesTests
{
    [Fact]
    public void A_reducer_call_after_dispose_is_a_retryable_refusal()
    {
        var harness = new EngineHarness();
        harness.Engine.Dispose();

        var refused = Assert.Throws<TransientRejectionException>(() =>
            harness.Engine.Invoke("AfterDispose", Identity.Hash("caller"), _ => { }));

        Assert.Contains("AfterDispose", refused.Message);
        Assert.Contains("disposed", refused.Message);
        harness.Dispose();
    }

    [Fact]
    public void The_generated_dispatch_overload_refuses_the_same_way()
    {
        var harness = new EngineHarness();
        harness.Engine.Dispose();

        foreach (var isolation in new[] { Isolation.Serialized, Isolation.Snapshot })
        {
            var refused = Assert.Throws<TransientRejectionException>(() =>
                harness.Engine.Invoke("Dispatched", Identity.Hash("caller"), ReadOnlyMemory<byte>.Empty, _ => { }, isolation: isolation));
            Assert.Contains("Dispatched", refused.Message);
        }

        harness.Dispose();
    }

    [Fact]
    public void An_internal_apply_after_dispose_is_refused_too()
    {
        var harness = new EngineHarness();
        harness.Engine.Dispose();

        Assert.Throws<TransientRejectionException>(() =>
            harness.Engine.ApplyInternal("melange/replica", Identity.Hash("hub"), []));
        harness.Dispose();
    }
}
