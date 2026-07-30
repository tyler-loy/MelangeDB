using Xunit;

namespace MelangeDB.Core.Tests;

public class CommitLogTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    private void SeedWorkload()
    {
        var owner = Identity.Hash("owner");
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = owner, RoomId = 7, X = 1.5f, Y = -2.5f, Name = "Owner" }));
        _harness.Invoke("Loot", ctx =>
        {
            ctx.Db.Insert(new InventoryItem { Owner = owner, ItemName = "pick", Quantity = 1 });
            ctx.Db.Insert(new InventoryItem { Owner = owner, ItemName = "torch", Quantity = 5 });
            ctx.Db.Insert(new Registration { Email = "owner@example.com", CreatedAt = ctx.Timestamp });
        });
        _harness.Invoke("Move", ctx =>
        {
            var player = ctx.Db.Find<Player>(owner)!.Value;
            ctx.Db.Update(player with { X = 99f, RoomId = 8 });
            ctx.Db.Insert(new TerrainChunk { ChunkId = 12, Data = [9, 9, 9], Kind = ChunkKind.Rock });
        });
        _harness.Invoke("Drop", ctx => Assert.True(ctx.Db.Delete<InventoryItem>(2UL)));
    }

    [Fact]
    public void Restart_rebuilds_identical_state_from_the_log_alone()
    {
        SeedWorkload();
        var before = _harness.Dump();
        Assert.NotEmpty(before);

        _harness.Restart();

        Assert.Equal(before, _harness.Dump());
        Assert.Equal(4UL, _harness.Engine.Log.HeadLsn);
        Assert.Equal(4UL, _harness.Engine.HotStore.AppliedLsn);
    }

    [Fact]
    public void Replaying_the_log_into_a_fresh_store_is_byte_identical()
    {
        SeedWorkload();

        var fresh = new InMemoryHotStore(SchemaRegistry.FromTypes(
            typeof(Player), typeof(InventoryItem), typeof(Registration), typeof(TerrainChunk)));
        var applier = new HotStoreApplier(fresh);
        foreach (var record in _harness.Engine.Log.ReadFrom(1))
            applier.Apply(record);

        Assert.Equal(_harness.Dump(), _harness.Dump(fresh));
        Assert.Equal(_harness.Engine.Log.HeadLsn, fresh.AppliedLsn);
    }

    [Fact]
    public void Records_carry_lsn_caller_reducer_and_arguments_as_metadata()
    {
        _harness.Engine.Invoke(
            "Join",
            EngineHarness.Caller,
            ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 3, Name = "P" }),
            arguments: ["p", 3]);

        var record = _harness.Engine.Log.ReadFrom(1).Single();
        Assert.Equal(1UL, record.Lsn);
        Assert.Equal((ushort)1, record.FormatVersion);
        Assert.Equal(EngineHarness.Caller, record.Caller);
        Assert.Equal("Join", record.ReducerName);
        Assert.False(record.Arguments.IsEmpty);
        Assert.True(record.Timestamp.UnixTimeMicroseconds > 0);
        Assert.True(record.SerializedLength > 0);
    }

    [Fact]
    public void Torn_trailing_record_is_truncated_to_the_last_intact_lsn()
    {
        SeedWorkload();
        var stateAfterThree = CaptureStateAtLsn(3);
        _harness.Engine.Dispose();

        // Cut into the final record, as a crash mid-append would.
        using (var file = new FileStream(_harness.LogFilePath, FileMode.Open, FileAccess.ReadWrite))
        {
            file.SetLength(file.Length - 7);
        }

        _harness.Restart();
        Assert.Equal(3UL, _harness.Engine.Log.HeadLsn);
        Assert.Equal(stateAfterThree, _harness.Dump());

        // The truncated log accepts new appends.
        _harness.Invoke("After", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 99, Data = [1], Kind = ChunkKind.Empty }));
        Assert.Equal(4UL, _harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void Corrupt_trailing_record_is_truncated_not_fatal()
    {
        SeedWorkload();
        _harness.Engine.Dispose();

        // Flip a byte inside the last record's payload: length intact, CRC wrong, at the tail.
        using (var file = new FileStream(_harness.LogFilePath, FileMode.Open, FileAccess.ReadWrite))
        {
            file.Seek(-3, SeekOrigin.End);
            var b = file.ReadByte();
            file.Seek(-1, SeekOrigin.Current);
            file.WriteByte((byte)(b ^ 0xFF));
        }

        _harness.Restart();
        Assert.Equal(3UL, _harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void Corruption_before_the_tail_is_fatal()
    {
        SeedWorkload();
        _harness.Engine.Dispose();

        // Flip a byte inside the first record: intact records follow, so this is real damage.
        using (var file = new FileStream(_harness.LogFilePath, FileMode.Open, FileAccess.ReadWrite))
        {
            file.Seek(20, SeekOrigin.Begin);
            var b = file.ReadByte();
            file.Seek(-1, SeekOrigin.Current);
            file.WriteByte((byte)(b ^ 0xFF));
        }

        Assert.Throws<InvalidDataException>(() => _harness.Restart());
    }

    [Fact]
    public void Every_fsync_policy_commits_and_recovers()
    {
        foreach (var policy in new[] { FsyncPolicy.OnCommit, FsyncPolicy.Interval, FsyncPolicy.OsBuffered })
        {
            using var harness = new EngineHarness(policy);
            harness.Invoke("Insert", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));
            var before = harness.Dump();
            harness.Restart();
            Assert.Equal(before, harness.Dump());
        }
    }

    [Fact]
    public void Fsync_policy_change_takes_effect_on_the_next_commit()
    {
        _harness.Options.CommitLog.FsyncPolicy = FsyncPolicy.OsBuffered;
        _harness.Invoke("Buffered", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("a"), RoomId = 1, Name = "A" }));
        _harness.Options.CommitLog.FsyncPolicy = FsyncPolicy.OnCommit;
        _harness.Invoke("Durable", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("b"), RoomId = 1, Name = "B" }));
        Assert.Equal(2UL, _harness.Engine.Log.HeadLsn);
    }

    private List<string> CaptureStateAtLsn(ulong lsn)
    {
        var fresh = new InMemoryHotStore(SchemaRegistry.FromTypes(
            typeof(Player), typeof(InventoryItem), typeof(Registration), typeof(TerrainChunk)));
        foreach (var record in _harness.Engine.Log.ReadFrom(1))
        {
            if (record.Lsn <= lsn)
                fresh.Apply(record);
        }

        return _harness.Dump(fresh);
    }
}
