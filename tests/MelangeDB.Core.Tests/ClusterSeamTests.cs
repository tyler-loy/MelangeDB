using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The engine seams clustering hangs off: commit guards at the commit point, the table-access
/// guard inside the transactional views, the internal apply path, per-engine originator ids, and
/// the strategy-facing RowRef. All of them are inert until installed — the single-node behavior
/// tests everywhere else in this suite run with none of them and must never notice.
/// </summary>
public class ClusterSeamTests
{
    private sealed class RecordingGuard : ICommitGuard
    {
        public readonly List<(string Reducer, int Ops, CommitOrigin Origin)> Seen = [];

        public Exception? Throw { get; set; }

        public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin)
        {
            Seen.Add((reducerName, writeSet.Count, origin));
            if (Throw is { } failure)
                throw failure;
        }
    }

    [Fact]
    public void Commit_guard_sees_the_collapsed_write_set_before_the_append()
    {
        using var harness = new EngineHarness();
        var guard = new RecordingGuard();
        harness.Engine.AddCommitGuard(guard);

        harness.Invoke("touch", ctx =>
        {
            var player = ctx.Db.Insert(new Player { Id = Identity.Hash("p1"), Name = "a" });
            ctx.Db.Update(player with { Name = "b" });
        });

        var seen = Assert.Single(guard.Seen);
        Assert.Equal("touch", seen.Reducer);
        Assert.Equal(1, seen.Ops); // Insert + update collapsed to one op.
        Assert.Equal(CommitOrigin.Reducer, seen.Origin);
    }

    [Fact]
    public void Commit_guard_throw_aborts_the_transaction_with_zero_trace()
    {
        using var harness = new EngineHarness();
        var guard = new RecordingGuard { Throw = new InvalidOperationException("guard says no") };
        harness.Engine.AddCommitGuard(guard);

        var failure = Assert.Throws<InvalidOperationException>(() =>
            harness.Invoke("blocked", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p1"), Name = "x" })));

        Assert.Equal("guard says no", failure.Message);
        Assert.Equal(0UL, harness.Engine.Log.HeadLsn);
        Assert.Empty(harness.Dump());
    }

    [Fact]
    public void Commit_guard_sees_bulk_ingestion_with_the_bulk_origin()
    {
        using var harness = new EngineHarness();
        var guard = new RecordingGuard();
        harness.Engine.AddCommitGuard(guard);

        harness.Engine.BulkInsert(EngineHarness.Caller,
        [
            new BulkRow("TerrainChunk", new Dictionary<string, object?> { ["ChunkId"] = 1L, ["Data"] = new byte[] { 1 }, ["Kind"] = ChunkKind.Rock }),
        ]);

        var seen = Assert.Single(guard.Seen);
        Assert.Equal(CommitOrigin.Bulk, seen.Origin);
    }

    [Fact]
    public void Table_access_guard_surfaces_its_message_at_the_point_of_access()
    {
        using var harness = new EngineHarness();
        harness.Engine.SetTableAccessGuard((table, access) =>
        {
            if (table.Name == "Registration")
                throw new InvalidOperationException($"Table 'Registration' is not present on this node ({access}).");
        });

        var read = Assert.Throws<InvalidOperationException>(() =>
            harness.Invoke("read", ctx => ctx.Db.Find<Registration>(1L)));
        Assert.Contains("not present on this node (Read)", read.Message);

        var write = Assert.Throws<InvalidOperationException>(() =>
            harness.Invoke("write", ctx => ctx.Db.Insert(new Registration { Email = "a@b" })));
        Assert.Contains("not present on this node (Write)", write.Message);

        // Guarded tables are unreadable through the committed view too — the policy path.
        Assert.Throws<InvalidOperationException>(() => harness.Engine.CommittedView.Count<Registration>());

        // Unguarded tables are untouched.
        harness.Invoke("ok", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), Name = "fine" }));
        Assert.Equal(1UL, harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void ApplyInternal_appends_one_record_and_projects_it()
    {
        using var harness = new EngineHarness();
        var schema = harness.Engine.Schema.Get(typeof(TerrainChunk));
        var row = RowSerializer.Serialize(schema, new TerrainChunk { ChunkId = 9, Data = [1, 2], Kind = ChunkKind.Ore });
        var key = SchemaKeyCodec.Encode(schema.PrimaryKey, 9L);

        var record = harness.Engine.ApplyInternal(
            "melange/replica", EngineHarness.Caller, [new RowOp(RowOpKind.Insert, schema.Id, key, row)]);

        Assert.NotNull(record);
        Assert.Equal(1UL, record!.Lsn);
        Assert.True(harness.Engine.HotStore.TryGetRow(schema.Id, key, out var stored));
        Assert.Equal(row, stored.ToArray());
    }

    [Fact]
    public void ApplyInternal_reconcile_makes_redelivery_idempotent()
    {
        using var harness = new EngineHarness();
        var schema = harness.Engine.Schema.Get(typeof(TerrainChunk));
        var row = RowSerializer.Serialize(schema, new TerrainChunk { ChunkId = 9, Data = [1], Kind = ChunkKind.Rock });
        var key = SchemaKeyCodec.Encode(schema.PrimaryKey, 9L);
        var missingKey = SchemaKeyCodec.Encode(schema.PrimaryKey, 404L);
        var ops = new[]
        {
            new RowOp(RowOpKind.Insert, schema.Id, key, row),
            new RowOp(RowOpKind.Delete, schema.Id, missingKey),
        };

        var first = harness.Engine.ApplyInternal("melange/replica", EngineHarness.Caller, ops, reconcile: true);
        var second = harness.Engine.ApplyInternal("melange/replica", EngineHarness.Caller, ops, reconcile: true);

        // First delivery: the insert applies, the delete of a missing row is dropped. Second
        // delivery: the same insert arrives as an update of the row it already created.
        Assert.Equal(RowOpKind.Insert, Assert.Single(first!.WriteSet).Kind);
        Assert.Equal(RowOpKind.Update, Assert.Single(second!.WriteSet).Kind);

        // The redelivered log still replays: recovery applies both records cleanly.
        harness.Restart();
        Assert.True(harness.Engine.HotStore.TryGetRow(schema.Id, key, out _));
    }

    [Fact]
    public void ApplyInternal_marker_records_append_with_an_empty_write_set_and_survive_replay()
    {
        using var harness = new EngineHarness();
        var marker = harness.Engine.ApplyInternal(
            "melange/handoff-freeze", EngineHarness.Caller, [], arguments: new byte[] { 1, 2, 3 }, alwaysAppend: true);

        Assert.NotNull(marker);
        Assert.Empty(marker!.WriteSet);

        harness.Restart();
        var replayed = Assert.Single(harness.Engine.Log.ReadFrom(1));
        Assert.Equal("melange/handoff-freeze", replayed.ReducerName);
        Assert.Equal(new byte[] { 1, 2, 3 }, replayed.Arguments.ToArray());
    }

    [Fact]
    public void Engine_originator_prefixes_autoinc_ids_and_survives_restart()
    {
        var root = Directory.CreateTempSubdirectory("melange-originator-").FullName;
        try
        {
            var options = new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(root, "hot") },
                CommitLog = { Path = Path.Combine(root, "log") },
            };
            ulong firstId = 0;
            using (var engine = new MelangeEngine(options, EngineHarness.GeneratedRegistry(typeof(InventoryItem)), originator: 7))
            {
                engine.Invoke("mint", EngineHarness.Caller, ctx =>
                {
                    firstId = ctx.Db.Insert(new InventoryItem { Owner = EngineHarness.Caller, ItemName = "sword", Quantity = 1 }).Id;
                });
            }

            Assert.Equal(7, (ushort)(firstId >> 47));
            Assert.Equal(1UL, firstId & ((1UL << 47) - 1));

            // A restarted engine re-observes only its own originator's ids and continues the sequence.
            using (var engine = new MelangeEngine(options, EngineHarness.GeneratedRegistry(typeof(InventoryItem)), originator: 7))
            {
                ulong secondId = 0;
                engine.Invoke("mint", EngineHarness.Caller, ctx =>
                {
                    secondId = ctx.Db.Insert(new InventoryItem { Owner = EngineHarness.Caller, ItemName = "shield", Quantity = 1 }).Id;
                });
                Assert.Equal(firstId + 1, secondId);
            }

            // A different originator over the same table can never mint the same value.
            var otherRoot = Directory.CreateTempSubdirectory("melange-originator-b-").FullName;
            try
            {
                var otherOptions = new MelangeDbOptions
                {
                    HotStore = { Path = Path.Combine(otherRoot, "hot") },
                    CommitLog = { Path = Path.Combine(otherRoot, "log") },
                };
                using var other = new MelangeEngine(otherOptions, EngineHarness.GeneratedRegistry(typeof(InventoryItem)), originator: 8);
                ulong otherId = 0;
                other.Invoke("mint", EngineHarness.Caller, ctx =>
                {
                    otherId = ctx.Db.Insert(new InventoryItem { Owner = EngineHarness.Caller, ItemName = "axe", Quantity = 1 }).Id;
                });
                Assert.NotEqual(firstId, otherId);
                Assert.Equal(8, (ushort)(otherId >> 47));
            }
            finally
            {
                Directory.Delete(otherRoot, recursive: true);
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void RowRef_reads_columns_from_the_serialized_row()
    {
        using var harness = new EngineHarness();
        var schema = harness.Engine.Schema.Get(typeof(TerrainChunk));
        var bytes = RowSerializer.Serialize(schema, new TerrainChunk { ChunkId = 42, Data = [7], Kind = ChunkKind.Ore });

        var rowRef = schema.ToRowRef(bytes);

        Assert.Equal(42L, rowRef.Column("ChunkId"));
        Assert.Equal(ChunkKind.Ore, rowRef.Column("Kind"));
        Assert.Throws<ArgumentException>(() => rowRef.Column("Nope"));
    }

    [Fact]
    public void A_table_whose_shard_by_names_the_primary_key_is_refused_at_schema_construction()
    {
        // The runtime half of MELANGE0018 — covers the non-codegen registration path.
        var column = new ColumnSchema
        {
            Name = "InstanceId",
            ClrType = typeof(uint),
            Kind = ColumnKind.UInt32,
            IsPrimaryKey = true,
            GetValue = static row => 0u,
            SetValue = static (_, _) => { },
        };
        var failure = Assert.Throws<NotSupportedException>(() => new TableSchema(
            typeof(TerrainChunk), "BadShardBy", [column], placement: Placement.Partitioned, shardBy: "InstanceId"));
        Assert.Contains("its own column", failure.Message);

        var unknown = Assert.Throws<NotSupportedException>(() => new TableSchema(
            typeof(TerrainChunk), "BadShardBy", [column], placement: Placement.Partitioned, shardBy: "Nope"));
        Assert.Contains("no column", unknown.Message);
    }

    [Fact]
    public void Descriptor_auto_site_resolves_to_shard_at_runtime()
    {
        var descriptor = new ReducerDescriptor(
            "x", ReducerKind.Standard, typeof(object),
            (ref ReducerArgsReader _) => { },
            (object _, ReducerContext _, ref ReducerArgsReader _) => { },
            site: ReducerSite.Auto);
        Assert.Equal(ReducerSite.Shard, descriptor.ExecutionSite);
    }
}

/// <summary>The hub-minted internal identity assertion: signature, expiry, and tamper behavior.</summary>
public class InternalIdentityAssertionTests
{
    private static readonly Identity Player = Identity.Hash("player-1");

    [Fact]
    public void A_minted_assertion_validates_and_round_trips_its_claims()
    {
        var expires = DateTimeOffset.UnixEpoch.AddDays(10_000);
        var token = InternalIdentityAssertion.Mint("secret", Player, isGuest: true, isSqlOwner: false, expires, firesLifecycle: true);

        var result = InternalIdentityAssertion.Validate("secret", token, expires.AddMinutes(-1), out var failure);

        Assert.Null(failure);
        Assert.NotNull(result);
        Assert.Equal(Player, result!.Value.Identity);
        Assert.True(result.Value.IsGuest);
        Assert.False(result.Value.IsSqlOwner);
        Assert.True(result.Value.FiresLifecycle);
    }

    [Fact]
    public void An_expired_assertion_is_rejected()
    {
        var expires = DateTimeOffset.UnixEpoch.AddHours(1);
        var token = InternalIdentityAssertion.Mint("secret", Player, false, false, expires);

        Assert.Null(InternalIdentityAssertion.Validate("secret", token, expires.AddSeconds(1), out var failure));
        Assert.Contains("expired", failure);
    }

    [Fact]
    public void A_tampered_assertion_fails_the_signature_check()
    {
        var token = InternalIdentityAssertion.Mint("secret", Player, false, false, DateTimeOffset.UnixEpoch.AddDays(10_000));
        var payload = Convert.FromBase64String(token[InternalIdentityAssertion.Prefix.Length..].Split('.')[0]);
        payload[10] ^= 0xFF;
        var tampered = InternalIdentityAssertion.Prefix
            + Convert.ToBase64String(payload) + "." + token.Split('.')[2];

        Assert.Null(InternalIdentityAssertion.Validate("secret", tampered, DateTimeOffset.UnixEpoch, out var failure));
        Assert.Contains("signature", failure);
    }

    [Fact]
    public void An_assertion_minted_with_a_different_secret_is_rejected()
    {
        var token = InternalIdentityAssertion.Mint("other-secret", Player, false, false, DateTimeOffset.UnixEpoch.AddDays(10_000));
        Assert.Null(InternalIdentityAssertion.Validate("secret", token, DateTimeOffset.UnixEpoch, out var failure));
        Assert.Contains("signature", failure);
    }
}
