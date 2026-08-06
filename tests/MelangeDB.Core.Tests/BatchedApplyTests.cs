using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// A record's ops for one table apply as a single version publish. Every intermediate version was
/// structurally shared but never observed — the whole record applies under the engine's write lock
/// — and each one cost a path copy of the row map plus one of every secondary index.
/// <para>
/// Batching rewrote the row and byte accounting to thread through locals, so what these check is
/// that a batched record lands exactly where the same ops land one record at a time: same rows,
/// same index answers, same resident-byte total.
/// </para>
/// </summary>
public class BatchedApplyTests
{
    [Fact]
    public void A_multi_op_record_lands_where_the_same_ops_land_one_at_a_time()
    {
        var ops = new[]
        {
            Insert("a", room: 1),
            Insert("b", room: 2),
            Insert("c", room: 1),
        };

        AssertSameOutcome(ops);
    }

    [Fact]
    public void A_record_that_inserts_then_deletes_the_same_key_accounts_for_both()
    {
        // The accounting hazard: the key's bytes are added on insert and must be removed exactly
        // once on delete, from a row map that only exists as a local inside the batch.
        var ops = new[]
        {
            Insert("a", room: 1),
            Insert("b", room: 2),
            Delete("a"),
        };

        AssertSameOutcome(ops);
    }

    [Fact]
    public void A_record_that_rewrites_a_key_it_just_wrote_keeps_one_index_entry()
    {
        // Two writes to one key in one record: the second must unindex the first's room, or the
        // secondary index keeps a stale entry that no later delete will ever clear.
        var ops = new[]
        {
            Insert("a", room: 1),
            Update("a", room: 7),
            Update("a", room: 9),
        };

        AssertSameOutcome(ops);
        var store = Apply(SingleRecord(ops));
        Assert.Empty(store.ScanIndex(PlayerTable(store), nameof(Player.RoomId), RoomKey(1)));
        Assert.Empty(store.ScanIndex(PlayerTable(store), nameof(Player.RoomId), RoomKey(7)));
        Assert.Single(store.ScanIndex(PlayerTable(store), nameof(Player.RoomId), RoomKey(9)));
    }

    [Fact]
    public void A_record_spanning_two_tables_applies_to_both()
    {
        var ops = new[]
        {
            Insert("a", room: 1),
            Chunk(10),
            Insert("b", room: 2),
            Chunk(11),
        };

        AssertSameOutcome(ops);
    }

    [Fact]
    public void Deleting_a_key_the_store_never_had_changes_nothing()
    {
        AssertSameOutcome([Insert("a", room: 1), Delete("z"), Insert("b", room: 2)]);
    }

    [Fact]
    public void A_table_emptied_by_deletes_accounts_for_no_bytes_at_all()
    {
        // The absolute check the differential ones cannot make: comparing a batched apply against a
        // separate one passes happily when both leak the same bytes, because both run the same
        // accounting. Insert-then-delete-everything has one right answer — zero — and a key or row
        // length dropped from either side of the ledger shows up here.
        var store = Apply(
        [
            Record(1, [Insert("a", room: 1), Insert("b", room: 2), Chunk(10)]),
            Record(2, [Delete("a"), Delete("b")]),
            Record(3, [DeleteChunk(10)]),
        ]);

        foreach (var table in store.Statistics().Tables)
        {
            Assert.Equal(0, table.RowCount);
            Assert.Equal(0, table.ResidentBytes);
        }
    }

    [Fact]
    public void Rewriting_a_row_accounts_for_the_new_size_not_the_sum()
    {
        // An update replaces the row's bytes; the key is already paid for. Both stores must weigh
        // exactly what one long-named player weighs, not that plus the short-named one it replaced.
        var rewritten = Apply([Record(1, [Insert("a", room: 1)]), Record(2, [PlayerOp(RowOpKind.Update, "a", 2, name: "a-much-longer-name")])]);
        var written = Apply([Record(1, [PlayerOp(RowOpKind.Insert, "a", 2, name: "a-much-longer-name")])]);

        Assert.Equal(Footprint(written), Footprint(rewritten));
    }

    /// <summary>
    /// Applies the ops as one record and as one record each, then asserts the two stores are
    /// indistinguishable — rows, index answers, and the resident-byte accounting.
    /// </summary>
    private static void AssertSameOutcome(IReadOnlyList<RowOp> ops)
    {
        var batched = Apply(SingleRecord(ops));
        var separate = Apply([.. ops.Select((op, i) => Record((ulong)i + 1, [op]))]);

        foreach (var table in Registry().Tables)
        {
            Assert.Equal(
                separate.Scan(table.Id).Select(p => (p.Key, p.Value.ToArray())),
                batched.Scan(table.Id).Select(p => (p.Key, p.Value.ToArray())));
            Assert.Equal(separate.Count(table.Id), batched.Count(table.Id));
        }

        Assert.Equal(Footprint(separate), Footprint(batched));
    }

    private static IEnumerable<(string Name, long Rows, long Bytes)> Footprint(InMemoryHotStore store) =>
        store.Statistics().Tables.Select(t => (t.Name, t.RowCount, t.ResidentBytes)).ToList();

    private static InMemoryHotStore Apply(IReadOnlyList<CommitRecord> records)
    {
        var store = new InMemoryHotStore(Registry());
        foreach (var record in records)
            store.Apply(record);
        return store;
    }

    private static IReadOnlyList<CommitRecord> SingleRecord(IReadOnlyList<RowOp> ops) => [Record(1, ops)];

    private static CommitRecord Record(ulong lsn, IReadOnlyList<RowOp> ops) => new()
    {
        Lsn = lsn,
        FormatVersion = 2,
        Timestamp = new Timestamp((long)lsn),
        Caller = EngineHarness.Caller,
        ReducerName = "Batch",
        Arguments = ReadOnlyMemory<byte>.Empty,
        WriteSet = ops,
        SerializedLength = 0,
    };

    private static SchemaRegistry Registry() => SchemaRegistry.FromTypes(typeof(Player), typeof(TerrainChunk));

    private static TableId PlayerTable(InMemoryHotStore store)
    {
        _ = store;
        Assert.True(Registry().TryGetByName(nameof(Player), out var schema));
        return schema.Id;
    }

    private static RowKey RoomKey(int room)
    {
        Assert.True(Registry().TryGetByName(nameof(Player), out var schema));
        return SchemaKeyCodec.Encode(schema.Column(nameof(Player.RoomId)), room);
    }

    private static RowOp Insert(string name, int room) => PlayerOp(RowOpKind.Insert, name, room);

    private static RowOp Update(string name, int room) => PlayerOp(RowOpKind.Update, name, room);

    private static RowOp PlayerOp(RowOpKind kind, string key, int room, string? name = null)
    {
        Assert.True(Registry().TryGetByName(nameof(Player), out var schema));
        var row = new Player { Id = Identity.Hash(key), RoomId = room, X = room, Y = room, Name = name ?? key };
        return new RowOp(kind, schema.Id, SchemaKeyCodec.Encode(schema.PrimaryKey, row.Id), RowSerializer.Serialize(schema, row));
    }

    private static RowOp DeleteChunk(long id)
    {
        Assert.True(Registry().TryGetByName(nameof(TerrainChunk), out var schema));
        return new RowOp(RowOpKind.Delete, schema.Id, SchemaKeyCodec.Encode(schema.PrimaryKey, id));
    }

    private static RowOp Delete(string name)
    {
        Assert.True(Registry().TryGetByName(nameof(Player), out var schema));
        return new RowOp(RowOpKind.Delete, schema.Id, SchemaKeyCodec.Encode(schema.PrimaryKey, Identity.Hash(name)));
    }

    private static RowOp Chunk(long id)
    {
        Assert.True(Registry().TryGetByName(nameof(TerrainChunk), out var schema));
        var row = new TerrainChunk { ChunkId = id, Data = [1, 2, 3], Kind = ChunkKind.Rock };
        return new RowOp(RowOpKind.Insert, schema.Id, SchemaKeyCodec.Encode(schema.PrimaryKey, id), RowSerializer.Serialize(schema, row));
    }
}
