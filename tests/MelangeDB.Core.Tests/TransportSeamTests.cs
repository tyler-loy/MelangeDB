using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The Core seams phase 03's transport stands on: the durable log epoch, commit observers with
/// their pre-image guarantee, anchored consistent reads, and single-record bulk ingestion.
/// </summary>
public class TransportSeamTests
{
    [Fact]
    public void The_epoch_survives_reopen_and_changes_when_the_log_is_recreated()
    {
        using var harness = new EngineHarness();
        var epoch = harness.Engine.Log.EpochId;
        Assert.NotEqual(Guid.Empty, epoch);

        harness.Restart();
        Assert.Equal(epoch, harness.Engine.Log.EpochId);

        // Deleting the log file is a new incarnation: the epoch must change even though the
        // sidecar was left behind — a stale cursor must never count against the new history.
        harness.Engine.Dispose();
        File.Delete(harness.LogFilePath);
        harness.Restart();
        Assert.NotEqual(epoch, harness.Engine.Log.EpochId);
    }

    [Fact]
    public void A_pre_epoch_log_is_adopted_under_a_minted_epoch_exactly_once()
    {
        using var harness = new EngineHarness();
        harness.Invoke("Seed", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1] }));
        var epochPath = Path.Combine(Path.GetDirectoryName(harness.LogFilePath)!, "melange.epoch");

        // Simulate a log written by a build that predates epochs.
        harness.Engine.Dispose();
        File.Delete(epochPath);
        harness.Restart();
        var minted = harness.Engine.Log.EpochId;
        Assert.NotEqual(Guid.Empty, minted);

        harness.Restart();
        Assert.Equal(minted, harness.Engine.Log.EpochId);
    }

    [Fact]
    public void Commit_observers_see_records_in_order_before_the_store_applies_them()
    {
        using var harness = new EngineHarness();
        var chunkTable = harness.Engine.Schema.Get(typeof(TerrainChunk));
        var observed = new List<(ulong Lsn, byte[]? PreImage)>();
        harness.Engine.AddCommitObserver(new DelegateObserver(record =>
        {
            // At observation time the hot store still holds the row's previous version.
            harness.Engine.HotStore.TryGetRow(chunkTable.Id, record.WriteSet[0].Key, out var before);
            observed.Add((record.Lsn, before.IsEmpty ? null : before.ToArray()));
        }));

        harness.Invoke("Insert", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 9, Data = [1] }));
        harness.Invoke("Update", ctx => ctx.Db.Update(new TerrainChunk { ChunkId = 9, Data = [2] }));
        harness.Invoke("Delete", ctx => ctx.Db.Delete<TerrainChunk>(9L));

        Assert.Equal(3, observed.Count);
        Assert.Equal([1UL, 2UL, 3UL], observed.Select(o => o.Lsn).ToArray());
        Assert.Null(observed[0].PreImage);
        Assert.NotNull(observed[1].PreImage);
        Assert.NotNull(observed[2].PreImage);
    }

    [Fact]
    public void A_throwing_observer_never_poisons_the_committed_transaction()
    {
        using var harness = new EngineHarness();
        harness.Engine.AddCommitObserver(new DelegateObserver(_ => throw new InvalidOperationException("observer bug")));
        harness.Invoke("Insert", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1] }));
        Assert.Equal(1UL, harness.Engine.Log.HeadLsn);
        harness.Invoke("Check", ctx => Assert.NotNull(ctx.Db.Find<TerrainChunk>(1L)));
    }

    [Fact]
    public void ReadConsistent_hands_out_the_head_lsn_its_view_is_anchored_at()
    {
        using var harness = new EngineHarness();
        harness.Invoke("Seed", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1] }));
        var chunkTable = harness.Engine.Schema.Get(typeof(TerrainChunk));
        var (anchor, rows) = harness.Engine.ReadConsistent(head =>
            (head, harness.Engine.HotStore.Scan(chunkTable.Id).Count()));
        Assert.Equal(1UL, anchor);
        Assert.Equal(1, rows);
    }

    [Fact]
    public void Invoke_returns_the_commit_lsn_and_zero_for_read_only_transactions()
    {
        using var harness = new EngineHarness();
        var first = harness.Engine.Invoke("Write", EngineHarness.Caller, ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1] }));
        var readOnly = harness.Engine.Invoke("Read", EngineHarness.Caller, ctx => ctx.Db.Find<TerrainChunk>(1L));
        Assert.Equal(1UL, first);
        Assert.Equal(0UL, readOnly);
    }

    [Fact]
    public void Bulk_insert_appends_one_record_allocates_autoinc_and_upserts()
    {
        using var harness = new EngineHarness();
        var alice = Identity.Hash("alice");
        var record = harness.Engine.BulkInsert(EngineHarness.Caller,
        [
            new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = 1L, ["Kind"] = 2L, ["Data"] = new byte[] { 1 } }),
            new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = 2L, ["Data"] = new byte[] { 2 } }),
            new BulkRow("InventoryItem", new Dictionary<string, object?> { ["Owner"] = alice, ["ItemName"] = "pick", ["Quantity"] = 3L }),
        ]);

        Assert.NotNull(record);
        Assert.Equal(1UL, record!.Lsn);
        Assert.Equal(3, record.WriteSet.Count);
        Assert.Equal(1UL, harness.Engine.Log.HeadLsn);

        harness.Invoke("Verify", ctx =>
        {
            Assert.Equal(ChunkKind.Ore, ctx.Db.Find<TerrainChunk>(1L)!.Value.Kind);
            var item = Assert.Single(ctx.Db.Filter<InventoryItem>("Owner", alice));
            Assert.True(item.Id > 0);
            Assert.Equal(3, item.Quantity);
        });

        // A second load over the same keys upserts rather than failing.
        harness.Engine.BulkInsert(EngineHarness.Caller,
        [
            new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = 1L, ["Data"] = new byte[] { 9 } }),
        ]);
        harness.Invoke("VerifyUpsert", ctx => Assert.Equal(new byte[] { 9 }, ctx.Db.Find<TerrainChunk>(1L)!.Value.Data));

        // And bulk state survives recovery like any other committed transaction.
        harness.Restart();
        harness.Invoke("VerifyRecovered", ctx => Assert.Equal(new byte[] { 9 }, ctx.Db.Find<TerrainChunk>(1L)!.Value.Data));
    }

    [Fact]
    public void Bulk_insert_rejects_unknown_tables_and_uncoercible_values()
    {
        using var harness = new EngineHarness();
        Assert.Throws<ArgumentException>(() => harness.Engine.BulkInsert(EngineHarness.Caller,
            [new BulkRow("Nope", new Dictionary<string, object?> { ["Id"] = 1L })]));
        Assert.Throws<ArgumentException>(() => harness.Engine.BulkInsert(EngineHarness.Caller,
            [new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = "not-a-number" })]));
        Assert.Equal(0UL, harness.Engine.Log.HeadLsn);
    }

    private sealed class DelegateObserver(Action<CommitRecord> onCommit) : ICommitObserver
    {
        public void OnCommit(CommitRecord record) => onCommit(record);
    }
}
