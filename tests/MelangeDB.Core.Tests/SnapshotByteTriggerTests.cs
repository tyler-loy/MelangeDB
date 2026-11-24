using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// A node that commits rarely but in bulk never reached <c>Snapshots:IntervalTransactions</c>, so it
/// never snapshotted and never truncated: the overnight soak's out-of-world shards, which only take
/// border-band mirrors, grew their logs by gigabytes an hour while the busy shards' logs held flat.
/// <c>Snapshots:IntervalBytes</c> bounds them by what they append.
/// </summary>
public class SnapshotByteTriggerTests
{
    private const int Rows = 40;
    private const int RowBytes = 64 * 1024;

    [Fact]
    public void A_node_committing_rarely_but_in_bulk_snapshots_by_bytes_and_truncates_its_log()
    {
        using var harness = Harness(intervalBytes: 1L << 20);
        Write(harness);

        // 2.5 MiB appended in 40 commits, far short of the transaction interval: snapshotted by bytes,
        // and the log behind the last snapshot removed.
        Assert.True(File.Exists(Path.Combine(harness.Options.CommitLog.Path, "melange.snapshot")));
        Assert.True(new FileInfo(harness.LogFilePath).Length < 2L * (1 << 20), $"log {new FileInfo(harness.LogFilePath).Length} B");

        harness.Restart();
        Assert.Equal(Rows, harness.Engine.CommittedView.Scan<TerrainChunk>().Count());
    }

    [Fact]
    public void With_the_byte_trigger_off_only_the_transaction_count_snapshots()
    {
        using var harness = Harness(intervalBytes: 0);
        Write(harness);

        Assert.False(File.Exists(Path.Combine(harness.Options.CommitLog.Path, "melange.snapshot")));
        Assert.True(new FileInfo(harness.LogFilePath).Length >= (long)Rows * RowBytes);
    }

    private static EngineHarness Harness(long intervalBytes)
    {
        var harness = new EngineHarness(FsyncPolicy.OsBuffered, tables: [typeof(TerrainChunk)]);
        harness.Options.Snapshots.IntervalTransactions = long.MaxValue;
        harness.Options.Snapshots.IntervalBytes = intervalBytes;
        harness.Options.Resume.RetentionWindowSeconds = 0;
        return harness;
    }

    private static void Write(EngineHarness harness)
    {
        for (var i = 0; i < Rows; i++)
        {
            var id = i;
            harness.Invoke("Bulk", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = id, Data = new byte[RowBytes], Kind = ChunkKind.Rock }));
        }
    }
}
