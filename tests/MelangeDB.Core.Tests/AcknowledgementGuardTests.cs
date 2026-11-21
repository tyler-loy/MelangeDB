using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Phase 30 step 2a: commit guards get a second look after durability and before the commit
/// returns. A shard node re-checks its lease there, because one that paused between the pre-append
/// guard and the acknowledgement may already have a successor that cut the log before the record.
/// The record is in the log either way; what is withheld is the acknowledgement.
/// </summary>
public class AcknowledgementGuardTests
{
    private sealed class AckGuard(Func<ulong> durableLsn) : ICommitGuard
    {
        public bool Refuse { get; set; }

        public List<(CommitOrigin Origin, ulong DurableAtAck)> Acks { get; } = [];

        public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin)
        {
        }

        public void ValidateAcknowledgement(string reducerName, CommitOrigin origin)
        {
            Acks.Add((origin, durableLsn()));
            if (Refuse)
                throw new CommitOutcomeUnknownException($"'{reducerName}' lost its lease before the acknowledgement.");
        }
    }

    private static (EngineHarness Harness, AckGuard Guard) Start()
    {
        var harness = new EngineHarness(FsyncPolicy.OnCommit);
        var guard = new AckGuard(() => harness.Engine.Log.DurableLsn);
        harness.Engine.AddCommitGuard(guard);
        return (harness, guard);
    }

    private static void Insert(ReducerContext ctx, string name) =>
        ctx.Db.Insert(new Player { Id = Identity.Hash(name), Name = name, RoomId = 1 });

    [Fact]
    public void Every_commit_path_asks_once_after_the_record_is_durable()
    {
        var (harness, guard) = Start();
        using var _ = harness;
        var schema = harness.Engine.Schema.Get(typeof(TerrainChunk));

        harness.Engine.Invoke("Serialized", EngineHarness.Caller, ctx => Insert(ctx, "a"));
        harness.Engine.Invoke("Snapshot", EngineHarness.Caller, ReadOnlyMemory<byte>.Empty, ctx => Insert(ctx, "b"), isolation: Isolation.Snapshot);
        harness.Engine.ApplyInternal(
            "melange/replica", EngineHarness.Caller,
            [new RowOp(RowOpKind.Insert, schema.Id, SchemaKeyCodec.Encode(schema.PrimaryKey, 9L), RowSerializer.Serialize(schema, new TerrainChunk { ChunkId = 9, Data = [1], Kind = ChunkKind.Ore }))]);
        harness.Engine.BulkInsert(EngineHarness.Caller,
        [
            new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = 1L, ["Data"] = new byte[] { 1 }, ["Kind"] = ChunkKind.Rock }),
        ]);

        Assert.Equal(
            [CommitOrigin.Reducer, CommitOrigin.Reducer, CommitOrigin.Internal, CommitOrigin.Bulk],
            guard.Acks.Select(static a => a.Origin));

        // Each acknowledgement check saw its own record already durable: LSNs 1 to 4, in order.
        Assert.Equal([1UL, 2UL, 3UL, 4UL], guard.Acks.Select(static a => a.DurableAtAck));
    }

    [Fact]
    public void A_refused_acknowledgement_reports_outcome_unknown_and_the_record_stays_in_the_log()
    {
        var (harness, guard) = Start();
        using var _ = harness;
        guard.Refuse = true;
        var schema = harness.Engine.Schema.Get(typeof(TerrainChunk));

        Assert.Throws<CommitOutcomeUnknownException>(() =>
            harness.Engine.Invoke("Serialized", EngineHarness.Caller, ctx => Insert(ctx, "a")));
        Assert.Throws<CommitOutcomeUnknownException>(() =>
            harness.Engine.Invoke("Snapshot", EngineHarness.Caller, ReadOnlyMemory<byte>.Empty, ctx => Insert(ctx, "b"), isolation: Isolation.Snapshot));
        Assert.Throws<CommitOutcomeUnknownException>(() => harness.Engine.ApplyInternal(
            "melange/replica", EngineHarness.Caller,
            [new RowOp(RowOpKind.Insert, schema.Id, SchemaKeyCodec.Encode(schema.PrimaryKey, 9L), RowSerializer.Serialize(schema, new TerrainChunk { ChunkId = 9, Data = [1], Kind = ChunkKind.Ore }))]));
        Assert.Throws<CommitOutcomeUnknownException>(() => harness.Engine.BulkInsert(EngineHarness.Caller,
        [
            new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = 1L, ["Data"] = new byte[] { 1 }, ["Kind"] = ChunkKind.Rock }),
        ]));

        // Not aborts: all four are durable and projected. Whether they are history is for the
        // shard's next owner to decide, by where it cuts the log.
        Assert.Equal(4UL, harness.Engine.Log.DurableLsn);
        Assert.Equal(2, harness.Engine.CommittedView.Count<Player>());
        Assert.Equal(2, harness.Engine.CommittedView.Count<TerrainChunk>());
    }
}
