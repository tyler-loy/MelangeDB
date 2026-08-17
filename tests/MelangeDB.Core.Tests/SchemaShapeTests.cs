using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Hot-tier schema migration (road-to-0.2 phase 16): the shape sidecar records what row bytes
/// mean, additive changes rebuild the world under the new shape by column name, and destructive
/// changes refuse boot with every reason named. Versioned table structs below share one declared
/// table name — the same table as two deployments would declare it.
/// </summary>
public class SchemaShapeTests : IDisposable
{
    private readonly List<string> _roots = [];

    public void Dispose()
    {
        foreach (var root in _roots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    // -- The versions of table "Hero" ------------------------------------------------------------
    // Deliberately plain structs, registered through the direct TableSchema seam rather than
    // [Table] attributes: six structs declaring the table name "Hero" in one compilation is
    // exactly what the source generator refuses, and refusing it is right — only a test that
    // impersonates successive deployments ever wants it.

    public struct HeroV1
    {
        public ulong Id;
        public int X;
        public string Name;
    }

    /// <summary>V1 plus a column added mid-struct and one appended — byte-wise a reorder plus adds.</summary>
    public struct HeroV2
    {
        public ulong Id;
        public int X;
        public int Level;
        public string Name;
        public bool Alive;
    }

    /// <summary>V1 with the same columns in a different order — additive with zero added columns.</summary>
    public struct HeroReordered
    {
        public ulong Id;
        public string Name;
        public int X;
    }

    /// <summary>V1 without X — a removed column, destructive.</summary>
    public struct HeroDropped
    {
        public ulong Id;
        public string Name;
    }

    /// <summary>V1 with X widened to long — a changed kind, destructive.</summary>
    public struct HeroWidened
    {
        public ulong Id;
        public long X;
        public string Name;
    }

    /// <summary>V1 with the key moved to a new column — destructive however additive the rest looks.</summary>
    public struct HeroRekeyed
    {
        public ulong Id;
        public ulong Guid;
        public int X;
        public string Name;
    }

    public struct CounterV1
    {
        public ulong Id;
        public int Value;
    }

    public struct CounterV2
    {
        public ulong Id;
        public string? Label;
        public int Value;
    }

    /// <summary>Every Hero version indexes X, so migrated rows exercise index re-extraction too.</summary>
    private static TableSchema HeroTable<TRow>(string key = "Id") where TRow : struct =>
        Declare<TRow>("Hero", key, index: typeof(TRow) == typeof(HeroDropped) ? null : "X");

    private static TableSchema CounterTable<TRow>() where TRow : struct => Declare<TRow>("Counter", "Id", autoInc: "Id");

    /// <summary>The direct-construction seam: a TableSchema from a struct's fields, no attributes.</summary>
    internal static TableSchema Declare<TRow>(string name, string key, string? autoInc = null, string? index = null)
        where TRow : struct
    {
        var columns = typeof(TRow).GetFields()
            .OrderBy(f => f.MetadataToken)
            .Select(f => new ColumnSchema
            {
                Name = f.Name,
                ClrType = f.FieldType,
                Kind = KindOf(f.FieldType),
                IsPrimaryKey = f.Name == key,
                IsAutoInc = f.Name == autoInc,
                IsIndexed = f.Name == index,
                GetValue = row => f.GetValue(row),
                SetValue = (row, value) => f.SetValue(row, value),
            })
            .ToList();
        return new TableSchema(typeof(TRow), name, columns);
    }

    private static ColumnKind KindOf(Type type) => type switch
    {
        _ when type == typeof(bool) => ColumnKind.Bool,
        _ when type == typeof(int) => ColumnKind.Int32,
        _ when type == typeof(long) => ColumnKind.Int64,
        _ when type == typeof(ulong) => ColumnKind.UInt64,
        _ when type == typeof(string) => ColumnKind.String,
        _ => throw new NotSupportedException(type.Name),
    };

    // -- Harness ---------------------------------------------------------------------------------

    private string NewRoot()
    {
        var root = Directory.CreateTempSubdirectory("melange-shape-").FullName;
        _roots.Add(root);
        return root;
    }

    private static MelangeDbOptions OptionsFor(string root, bool snapshots = true) => new()
    {
        CommitLog = { Path = Path.Combine(root, "log") },
        HotStore = { Path = Path.Combine(root, "hot") },
        Snapshots = { Enabled = snapshots },
        Resume = { RetentionWindowSeconds = 0 },
    };

    private static MelangeEngine Boot(MelangeDbOptions options, params TableSchema[] tables) =>
        new(options, new SchemaRegistry(tables));

    private static MelangeEngine Boot(MelangeDbOptions options, ILoggerFactory loggers, params TableSchema[] tables) =>
        new(options, new SchemaRegistry(tables), loggers);

    private static ShapeHistory Sidecar(MelangeDbOptions options) =>
        ShapeHistory.Load(Path.Combine(options.CommitLog.Path, ShapeHistory.FileName))!;

    private static List<T> Rows<T>(MelangeEngine engine) where T : new()
    {
        var table = engine.Schema.Get(typeof(T));
        return engine.HotStore.Scan(table.Id)
            .Select(pair => (T)RowSerializer.Deserialize(table, pair.Value))
            .OrderBy(row => table.PrimaryKey.GetValue(row!) switch { ulong id => id, var k => (ulong)k!.GetHashCode() })
            .ToList();
    }

    // -- Additive migrations ---------------------------------------------------------------------

    [Fact]
    public void An_added_mid_class_column_migrates_the_world_and_preserves_every_row()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new HeroV1 { Id = 1, X = 10, Name = "alice" });
                ctx.Db.Insert(new HeroV1 { Id = 2, X = 20, Name = "bob" });
            });
        }

        var headBefore = HeadOf(options, HeroTable<HeroV1>());
        using (var v2 = Boot(options, HeroTable<HeroV2>()))
        {
            var heroes = Rows<HeroV2>(v2);
            Assert.Equal(2, heroes.Count);
            Assert.Equal((10, 0, "alice", false), (heroes[0].X, heroes[0].Level, heroes[0].Name, heroes[0].Alive));
            Assert.Equal((20, 0, "bob", false), (heroes[1].X, heroes[1].Level, heroes[1].Name, heroes[1].Alive));

            // The marker record is the migration's place in the log's own timeline.
            Assert.Equal(headBefore + 1, v2.Log.HeadLsn);

            // Indexes are projections rebuilt from the migrated bytes: the index on X must find
            // rows whose X slice moved when Level landed in front of Name.
            var table = v2.Schema.Get(typeof(HeroV2));
            Assert.Single(v2.HotStore.ScanIndex(table.Id, "X", SchemaKeyCodec.Encode(table.Column("X"), 20)));

            // The new shape's reign starts at the marker, never at an LSN an old row was written under.
            var sidecar = Sidecar(options);
            Assert.Equal(2, sidecar.Entries.Count);
            Assert.Equal(v2.Log.HeadLsn, sidecar.Current.FromLsn);

            // New rows write under the new shape, mixing with migrated ones.
            v2.Invoke("Later", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV2 { Id = 3, X = 30, Level = 5, Name = "carol", Alive = true }));
        }

        // The next boot is a fast path — no new entry — and because the migration's sealing
        // snapshot already truncated every old-shape record, the dead reign compacts away too.
        using (var again = Boot(options, HeroTable<HeroV2>()))
        {
            Assert.Single(Sidecar(options).Entries);
            var heroes = Rows<HeroV2>(again);
            Assert.Equal(3, heroes.Count);
            Assert.Equal((30, 5, "carol", true), (heroes[2].X, heroes[2].Level, heroes[2].Name, heroes[2].Alive));
        }
    }

    [Fact]
    public void A_pure_reorder_is_additive_and_maps_by_name()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 7, X = 42, Name = "dave" }));
        }

        using var reordered = Boot(options, HeroTable<HeroReordered>());
        var hero = Assert.Single(Rows<HeroReordered>(reordered));
        Assert.Equal((7UL, 42, "dave"), (hero.Id, hero.X, hero.Name));
    }

    [Fact]
    public void AutoInc_sequences_continue_across_a_migration()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, CounterTable<CounterV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new CounterV1 { Value = 1 });
                ctx.Db.Insert(new CounterV1 { Value = 2 });
            });
        }

        using var v2 = Boot(options, CounterTable<CounterV2>());
        v2.Invoke("More", EngineHarness.Caller, ctx => ctx.Db.Insert(new CounterV2 { Value = 3, Label = "post" }));
        var ids = Rows<CounterV2>(v2).Select(c => c.Id).ToList();
        Assert.Equal(3, ids.Count);
        Assert.Equal(ids.Distinct().Count(), ids.Count);
        Assert.Equal(ids.Max(), ids[2]); // the post-migration allocation is above everything replayed
    }

    // -- Destructive refusals --------------------------------------------------------------------

    [Fact]
    public void A_removed_column_a_changed_kind_and_a_removed_table_refuse_with_every_reason_named()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>(), CounterTable<CounterV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 1, Name = "a" }));
        }

        // Dropped column + dropped table, in one deploy: both reasons in one refusal.
        var dropped = Assert.Throws<SchemaShapeException>(() => Boot(options, HeroTable<HeroDropped>()));
        Assert.Contains("column 'X'", dropped.Message);
        Assert.Contains("rename", dropped.Message);
        Assert.Contains("table 'Counter' was removed", dropped.Message);
        Assert.Equal(2, dropped.Reasons.Count);

        var widened = Assert.Throws<SchemaShapeException>(() => Boot(options, HeroTable<HeroWidened>(), CounterTable<CounterV1>()));
        Assert.Contains("changed kind Int32 -> Int64", widened.Message);

        // A refused boot changed nothing: the old schema still boots and reads its world.
        using var still = Boot(options, HeroTable<HeroV1>(), CounterTable<CounterV1>());
        Assert.Single(Rows<HeroV1>(still));
    }

    [Fact]
    public void The_primary_key_may_not_move()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (Boot(options, HeroTable<HeroV1>()))
        {
        }

        var refused = Assert.Throws<SchemaShapeException>(() => Boot(options, HeroTable<HeroRekeyed>("Guid")));
        Assert.Contains("[PrimaryKey] moved from 'Id' to 'Guid'", refused.Message);
    }

    // -- Adoption and crash windows --------------------------------------------------------------

    [Fact]
    public void A_directory_without_a_sidecar_adopts_the_booting_schema_exactly_once()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 5, Name = "eve" }));
        }

        // Simulate a pre-phase-16 directory: the sidecar never existed.
        File.Delete(Path.Combine(options.CommitLog.Path, ShapeHistory.FileName));
        using (var adopted = Boot(options, HeroTable<HeroV1>()))
        {
            var sidecar = Sidecar(options);
            Assert.Single(sidecar.Entries);
            Assert.Equal(1UL, sidecar.Entries[0].FromLsn);
            Assert.Single(Rows<HeroV1>(adopted));
        }
    }

    [Fact]
    public void A_migration_interrupted_before_its_snapshot_stays_correct_because_decode_is_by_lsn()
    {
        var root = NewRoot();

        // Snapshots disabled: the migration boot appends the marker and the sidecar entry but
        // cannot seal with a snapshot — exactly the crash-window state. Correctness must not care.
        var options = OptionsFor(root, snapshots: false);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 9, Name = "frank" }));
        }

        using (Boot(options, HeroTable<HeroV2>()))
        {
        }

        // Every later boot re-transforms the old records through the history; no third entry appears.
        using var again = Boot(options, HeroTable<HeroV2>());
        Assert.Equal(2, Sidecar(options).Entries.Count);
        var hero = Assert.Single(Rows<HeroV2>(again));
        Assert.Equal((9, "frank"), (hero.X, hero.Name));
    }

    [Fact]
    public void A_snapshot_taken_at_the_pre_migration_head_is_never_mistaken_for_the_new_shape()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 3, Name = "grace" }));

            // The adversarial setup: an old-shape snapshot at exactly the head the migration
            // boot will see. Without the marker record, the migration's own snapshot would land
            // at this same LSN and the two shapes would be indistinguishable.
            Assert.NotNull(v1.TakeSnapshot());
        }

        using (Boot(options, HeroTable<HeroV2>()))
        {
        }

        using var again = Boot(options, HeroTable<HeroV2>());
        var hero = Assert.Single(Rows<HeroV2>(again));
        Assert.Equal((3, 0, "grace"), (hero.X, hero.Level, hero.Name));
    }

    // -- Compaction ------------------------------------------------------------------------------

    [Fact]
    public void A_reign_no_record_needs_anymore_compacts_away()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 2, Name = "hank" }));
        }

        using (var v2 = Boot(options, HeroTable<HeroV2>()))
        {
            // The migration boot already snapshot-and-truncated at the marker; one more commit,
            // snapshot, and truncate pushes the base past the marker so the old reign is dead.
            v2.Invoke("Later", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV2 { Id = 2, X = 4, Name = "iris" }));
            Assert.NotNull(v2.TakeSnapshot());
            Assert.True(v2.Log.BaseLsn >= Sidecar(options).Current.FromLsn);
        }

        using (Boot(options, HeroTable<HeroV2>()))
        {
            Assert.Single(Sidecar(options).Entries);
        }
    }

    // -- The backup interplay --------------------------------------------------------------------

    [Fact]
    public void An_archive_carries_the_shape_history_so_newer_code_boots_a_restored_directory()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 11, Name = "judy" }));
        }

        var archive = Path.Combine(NewRoot(), "world.mbak");
        MelangeBackup.Create(options.CommitLog.Path, archive);
        MelangeBackup.Verify(archive);

        var restored = Path.Combine(NewRoot(), "restored");
        MelangeBackup.Restore(archive, restored);
        Assert.True(File.Exists(Path.Combine(restored, ShapeHistory.FileName)));

        var restoredOptions = OptionsFor(NewRoot());
        restoredOptions.CommitLog.Path = restored;
        using var v2 = Boot(restoredOptions, HeroTable<HeroV2>());
        var hero = Assert.Single(Rows<HeroV2>(v2));
        Assert.Equal((11, 0, "judy"), (hero.X, hero.Level, hero.Name));
    }

    // -- Adoption over an existing directory (issue #99) ------------------------------------------

    /// <summary>
    /// A directory as it looks coming from a build that predates the shape sidecar: records on
    /// disk, no melange.shape beside them.
    /// </summary>
    private MelangeDbOptions PreSidecarDirectory()
    {
        var options = OptionsFor(NewRoot(), snapshots: false);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 10, Name = "alice" }));
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 2, X = 20, Name = "bob" }));
        }

        File.Delete(Path.Combine(options.CommitLog.Path, ShapeHistory.FileName));
        return options;
    }

    [Fact]
    public void Adopting_a_schema_over_records_a_different_binary_wrote_says_so_at_the_moment_it_happens()
    {
        var options = PreSidecarDirectory();
        var logs = new LogCapture();

        using (var booted = Boot(options, logs, HeroTable<HeroV1>()))
            Assert.Equal(2, Rows<HeroV1>(booted).Count);

        // Everything around this step is loud and this one was silent, so the mis-decode it can
        // cause arrives an arbitrary amount of time after a boot that looked perfectly clean.
        var entry = logs.Single(1008);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal(2d, entry.Number("HeadLsn"));
        Assert.Contains("upgrade rule", entry.Message);

        // The recovery is only actionable with its gates named: /melange/bulk answers 403 unless
        // both are set, and a 3 a.m. instruction that does not work is worse than none.
        Assert.Contains("POST /melange/bulk", entry.Message);
        Assert.Contains("Bulk:Enabled", entry.Message);
        Assert.Contains("Bulk:OwnerRole", entry.Message);
    }

    [Fact]
    public void A_new_world_naming_its_first_shape_says_nothing()
    {
        // The safe case, and the common one: no records exist, so nothing is being reinterpreted.
        var logs = new LogCapture();
        using var fresh = Boot(OptionsFor(NewRoot()), logs, HeroTable<HeroV1>());
        Assert.DoesNotContain(logs.Entries, e => e.EventId == 1008);
    }

    [Fact]
    public void A_boot_that_already_has_a_sidecar_says_nothing_either()
    {
        var options = OptionsFor(NewRoot(), snapshots: false);
        using (var first = Boot(options, HeroTable<HeroV1>()))
            first.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 10, Name = "alice" }));

        var logs = new LogCapture();
        using var second = Boot(options, logs, HeroTable<HeroV1>());
        Assert.DoesNotContain(logs.Entries, e => e.EventId == 1008);
    }

    [Fact]
    public void Adoption_over_an_existing_directory_can_be_refused_outright()
    {
        // The AutoMigrate posture, opt-in: a wrong adoption is not a stall but silently wrong reads
        // of existing data, so a deployment may prefer to stop and be told.
        var options = PreSidecarDirectory();
        options.Schema.AllowAdoption = false;

        var refusal = Assert.Throws<SchemaShapeException>(() => Boot(options, HeroTable<HeroV1>()));
        Assert.Contains("Schema:AllowAdoption", refusal.Message);
        Assert.Contains("silently mis-reads", refusal.Message);

        // Refusing writes nothing: the next boot decides afresh, which is what makes the flag a
        // gate rather than a one-way door.
        Assert.False(File.Exists(Path.Combine(options.CommitLog.Path, ShapeHistory.FileName)));

        options.Schema.AllowAdoption = true;
        using var booted = Boot(options, HeroTable<HeroV1>());
        Assert.Equal(2, Rows<HeroV1>(booted).Count);
    }

    [Fact]
    public void Refusing_adoption_never_blocks_a_new_world()
    {
        // The flag guards a reinterpretation, not a first naming — a deployment that sets
        // AllowAdoption to false for safety must still be able to create a database.
        var options = OptionsFor(NewRoot());
        options.Schema.AllowAdoption = false;
        using var fresh = Boot(options, HeroTable<HeroV1>());
        fresh.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 1, Name = "new" }));
        Assert.Single(Rows<HeroV1>(fresh));
    }

    [Fact]
    public void A_row_that_does_not_decode_names_the_table_and_the_reading_that_explains_it()
    {
        // Issue #99's symptom, and what an operator's log used to carry: one word about a
        // parameter name from deep inside a reader that has no idea which table it was decoding.
        // A short row is what a schema expecting more columns than the bytes hold looks like.
        var table = HeroTable<HeroV2>();
        var truncated = new byte[6];

        var failure = Assert.Throws<InvalidDataException>(() => RowSerializer.Deserialize(table, truncated));

        Assert.Contains("Table 'Hero'", failure.Message);
        Assert.Contains("6 byte(s)", failure.Message);
        Assert.Contains("MIGRATION.md", failure.Message);
        Assert.Contains("EventId 1008", failure.Message);
        Assert.NotNull(failure.InnerException);
    }

    [Fact]
    public void A_generated_codec_names_the_row_type_when_the_bytes_run_out()
    {
        // Production tables carry a generated codec, and index maintenance decodes on the store's
        // apply path — which after a bad adoption is very often the first decode of all, ahead of
        // any read. It has to name what it was decoding too.
        using var harness = new EngineHarness();
        var table = harness.Engine.Schema.Get(typeof(Player));
        var truncated = new CommitRecord
        {
            Lsn = harness.Engine.Log.HeadLsn + 1,
            FormatVersion = 1,
            Timestamp = new Timestamp(1),
            Caller = EngineHarness.Caller,
            ReducerName = "Planted",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [new RowOp(RowOpKind.Insert, table.Id, new RowKey(new byte[16]), new byte[4])],
            Events = [],
            SerializedLength = 0,
        };

        var failure = Assert.Throws<InvalidDataException>(() => harness.Engine.HotStore.Apply(truncated));
        Assert.Contains("Player", failure.Message);
        Assert.Contains("4 byte(s)", failure.Message);
        Assert.Contains("EventId 1008", failure.Message);
    }

    [Fact]
    public void The_migrating_process_can_still_transform_records_written_after_its_own_marker()
    {
        // Issue #92. The migration appends a reign to the live history *after* the transform was
        // resolved, and every decoupled reader in that same process — the Postgres applier, the
        // replica and border pumps, resume replay, a lagging applier's catch-up — transforms
        // unconditionally. Pipeline-driven appliers skip the transform for a record they just
        // watched commit, which is why every other test here missed this.
        var root = NewRoot();
        var options = OptionsFor(root);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 1, X = 10, Name = "alice" }));

        using var v2 = Boot(options, HeroTable<HeroV2>());
        v2.Invoke(
            "Later", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV2 { Id = 2, X = 20, Level = 5, Name = "bob", Alive = true }));

        // A post-marker record is already current-shape, so this is a pass-through — but only if
        // the resolution can index the reign the migration added.
        var record = v2.Log.ReadFrom(v2.Log.HeadLsn).Single();
        var transformed = v2.TransformToCurrentShape(record);
        Assert.Equal(record.WriteSet.Count, transformed.WriteSet.Count);

        var table = v2.Schema.Get(typeof(HeroV2));
        var hero = (HeroV2)RowSerializer.Deserialize(table, transformed.WriteSet[0].Row);
        Assert.Equal((20, 5, "bob", true), (hero.X, hero.Level, hero.Name, hero.Alive));
    }

    [Fact]
    public async Task Concurrent_readers_transform_pre_marker_records_without_racing_the_mapper_cache()
    {
        // The same cache was an unsynchronized Dictionary filled on first use, which the Postgres
        // loop and the replica/border pumps reach concurrently after a migration boot. Undefined
        // behaviour on Dictionary is not something to leave to luck on a hot read path.
        var root = NewRoot();
        var options = OptionsFor(root, snapshots: false);
        using (var v1 = Boot(options, HeroTable<HeroV1>()))
        {
            for (var i = 1; i <= 20; i++)
            {
                var id = (ulong)i;
                v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = id, X = (int)id, Name = $"hero-{id}" }));
            }
        }

        using var v2 = Boot(options, HeroTable<HeroV2>());
        var records = v2.Log.ReadFrom(1).ToList();
        var table = v2.Schema.Get(typeof(HeroV2));
        var ct = TestContext.Current.CancellationToken;

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(
            () =>
            {
                foreach (var record in records)
                {
                    foreach (var op in v2.TransformToCurrentShape(record).WriteSet)
                    {
                        var hero = (HeroV2)RowSerializer.Deserialize(table, op.Row);
                        Assert.Equal($"hero-{hero.Id}", hero.Name);
                    }
                }
            },
            ct))).WaitAsync(TimeSpan.FromSeconds(30), ct);
    }

    private static ulong HeadOf(MelangeDbOptions options, params TableSchema[] tables)
    {
        using var probe = Boot(options, tables);
        return probe.Log.HeadLsn;
    }
}
