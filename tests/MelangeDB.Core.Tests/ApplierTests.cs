using Xunit;

namespace MelangeDB.Core.Tests;

public class ApplierTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    private sealed class RecordingApplier(string name) : ILogApplier
    {
        public List<CommitRecord> Applied { get; } = [];

        public string Name { get; } = name;

        public ulong AppliedLsn { get; private set; }

        public void Apply(CommitRecord record)
        {
            Applied.Add(record);
            AppliedLsn = record.Lsn;
        }
    }

    private void Commit(string reducer, long chunkId) =>
        _harness.Invoke(reducer, ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = chunkId, Data = [1], Kind = ChunkKind.Rock }));

    [Fact]
    public void Appliers_lag_independently_and_resume_from_their_own_checkpoint()
    {
        var slow = new RecordingApplier("slow");
        var steady = new RecordingApplier("steady");
        _harness.Engine.Appliers.Register(slow);
        _harness.Engine.Appliers.Register(steady);

        Commit("One", 1);
        Assert.Equal(1UL, slow.AppliedLsn);

        _harness.Engine.Appliers.Pause("slow");
        Commit("Two", 2);
        Commit("Three", 3);

        // The paused applier holds its checkpoint while the others advance.
        Assert.Equal(1UL, slow.AppliedLsn);
        Assert.Equal(3UL, steady.AppliedLsn);
        Assert.Equal(3UL, _harness.Engine.HotStore.AppliedLsn);

        // Resume catches up from its own position, in order, without re-applying.
        _harness.Engine.Appliers.Resume("slow");
        Assert.Equal(3UL, slow.AppliedLsn);
        Assert.Equal(new ulong[] { 1, 2, 3 }, slow.Applied.Select(r => r.Lsn));
    }

    [Fact]
    public void Late_registered_applier_catches_up_from_zero()
    {
        Commit("One", 1);
        Commit("Two", 2);
        var late = new RecordingApplier("late");
        _harness.Engine.Appliers.Register(late);

        Commit("Three", 3);
        Assert.Equal(new ulong[] { 1, 2, 3 }, late.Applied.Select(r => r.Lsn));
    }

    [Fact]
    public void Store_apply_is_idempotent_below_its_checkpoint()
    {
        Commit("One", 1);
        var record = _harness.Engine.Log.ReadFrom(1).Single();
        var before = _harness.Dump();
        _harness.Engine.HotStore.Apply(record);
        Assert.Equal(before, _harness.Dump());
        Assert.Equal(1UL, _harness.Engine.HotStore.AppliedLsn);
    }
}
