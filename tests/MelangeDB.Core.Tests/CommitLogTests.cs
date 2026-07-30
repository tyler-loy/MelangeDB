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

    [Fact]
    public void Failed_append_rolls_back_the_file_and_the_next_append_commits_with_the_correct_lsn()
    {
        var root = Directory.CreateTempSubdirectory("melange-log-fault-").FullName;
        try
        {
            var options = new CommitLogOptions { Path = root };
            using var log = new FileCommitLog(options);
            log.Append(MakeRequest("First"));
            var bytesBefore = ReadAllBytesShared(log.FilePath);

            // A flush failure (disk full, in real life) must leave the file byte-identical:
            // an orphaned record would replay a phantom commit, and an un-advanced head LSN
            // would let the next append re-mint the same LSN and shadow a real transaction.
            log.AppendFaultInjection = _ => throw new IOException("injected: disk full");
            var thrown = Assert.Throws<IOException>(() => log.Append(MakeRequest("Doomed")));
            Assert.Equal("injected: disk full", thrown.Message);
            Assert.Equal(bytesBefore, ReadAllBytesShared(log.FilePath));
            Assert.Equal(1UL, log.HeadLsn);

            log.AppendFaultInjection = null;
            var record = log.Append(MakeRequest("Second"));
            Assert.Equal(2UL, record.Lsn);
            Assert.Equal(new ulong[] { 1, 2 }, log.ReadFrom(1).Select(r => r.Lsn));
            Assert.Equal(new[] { "First", "Second" }, log.ReadFrom(1).Select(r => r.ReducerName));

            log.Dispose();
            using var reopened = new FileCommitLog(options);
            Assert.Equal(2UL, reopened.HeadLsn);
            Assert.Equal(new ulong[] { 1, 2 }, reopened.ReadFrom(1).Select(r => r.Lsn));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Unrollbackable_append_failure_poisons_the_log()
    {
        var root = Directory.CreateTempSubdirectory("melange-log-poison-").FullName;
        try
        {
            var options = new CommitLogOptions { Path = root };
            using var log = new FileCommitLog(options);
            log.Append(MakeRequest("First"));

            // Killing the stream makes both the flush and the rollback truncation fail: the
            // partial record cannot be removed, so the log must refuse all further appends
            // rather than risk making an aborted transaction's record durable.
            log.AppendFaultInjection = stream => stream.Dispose();
            Assert.ThrowsAny<Exception>(() => log.Append(MakeRequest("Doomed")));

            var poisoned = Assert.Throws<InvalidOperationException>(() => log.Append(MakeRequest("After")));
            Assert.Contains("failed state", poisoned.Message);
            Assert.NotNull(poisoned.InnerException);
            Assert.Equal(1UL, log.HeadLsn);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static CommitRequest MakeRequest(string reducerName)
    {
        var op = new RowOp(RowOpKind.Insert, TableId.FromName("Whatever"), new RowKey([1, 2, 3]), new byte[] { 4, 5, 6 });
        return new CommitRequest(new Timestamp(1), EngineHarness.Caller, reducerName, ReadOnlyMemory<byte>.Empty, [op]);
    }

    private static byte[] ReadAllBytesShared(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
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
