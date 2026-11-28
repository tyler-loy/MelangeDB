using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The inspector's reader (road-to-0.3 phase 21, step 1): a read-only view of a data directory or
/// an archive that decodes records, rows, keys and arguments from the shape sidecar alone. Tested
/// against directories the engine itself writes: plain, compacted, restored, migrated, owner-changed,
/// torn, and live.
/// </summary>
public class InspectSourceTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly List<IDisposable> _disposables = [];

    public void Dispose()
    {
        foreach (var disposable in Enumerable.Reverse(_disposables))
            disposable.Dispose();
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

    private string TempDir()
    {
        var dir = Directory.CreateTempSubdirectory("melange-inspect-").FullName;
        _roots.Add(dir);
        return dir;
    }

    private EngineHarness Harness()
    {
        var harness = new EngineHarness(tables: [typeof(Player), typeof(InventoryItem)]);
        _disposables.Add(harness);
        return harness;
    }

    private InspectSource Open(string path, string? engine = null)
    {
        var source = InspectSource.Open(path, engine);
        _disposables.Add(source);
        return source;
    }

    private static void Seed(EngineHarness harness)
    {
        harness.Engine.Invoke(
            "Spawn",
            EngineHarness.Caller,
            ArgsCodec.Encode(["alice", 7, -3L, 2.5, true, null, Identity.Hash("x"), new Timestamp(12), new[] { 1, 2 }, new byte[] { 9 }]),
            ctx =>
            {
                ctx.Db.Insert(new Player { Id = Identity.Hash("alice"), RoomId = 7, X = 1.5f, Y = -2, Name = "alice" });
                ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("alice"), ItemName = "sword", Quantity = 1 });
            },
            ConnectionId.None,
            parentContext: default,
            Isolation.Serialized);
        harness.Invoke("Rename", ctx =>
        {
            ctx.Db.Update(new Player { Id = Identity.Hash("alice"), RoomId = 8, X = 1.5f, Y = -2, Name = "alicia" });
            ctx.Db.Delete<InventoryItem>(1UL);
        });
    }

    [Fact]
    public void A_directory_reads_records_and_decodes_rows_keys_and_arguments_without_the_application()
    {
        var harness = Harness();
        Seed(harness);
        var epoch = harness.Engine.Log.EpochId;
        harness.Engine.Dispose();

        var source = Open(harness.Options.CommitLog.Path);
        var facts = source.Facts;
        Assert.Equal(InspectSourceKind.DataDirectory, facts.Kind);
        Assert.Equal(epoch, facts.Epoch);
        Assert.Empty(facts.Lineage);
        Assert.Null(facts.Provenance);
        Assert.True(facts.ShapeRecorded);
        Assert.Equal(0UL, facts.LogFloor);
        Assert.Equal(1UL, facts.FirstLsn);
        Assert.Equal(2UL, facts.HeadLsn);
        Assert.Equal(2, facts.RecordCount);
        Assert.Equal(0, facts.TornTailBytes);
        Assert.Empty(facts.Gaps);

        var records = source.Records().ToList();
        Assert.Equal(["Spawn", "Rename"], records.Select(static r => r.ReducerName));
        Assert.All(records, static r => Assert.Equal(EngineHarness.Caller, r.Caller));

        var spawn = records[0];
        var arguments = RowDecoder.DecodeArguments(spawn.Arguments.Span);
        Assert.True(arguments.Succeeded, arguments.Error);
        Assert.Equal("alice", arguments.Values[0]);
        Assert.Equal(7L, arguments.Values[1]);
        Assert.Equal(-3L, arguments.Values[2]);
        Assert.Equal(2.5, arguments.Values[3]);
        Assert.Equal(true, arguments.Values[4]);
        Assert.Null(arguments.Values[5]);
        Assert.Equal(Identity.Hash("x"), arguments.Values[6]);
        Assert.Equal(new Timestamp(12), arguments.Values[7]);
        Assert.Equal(new object?[] { 1L, 2L }, (object?[])arguments.Values[8]!);
        Assert.Equal(new byte[] { 9 }, (byte[])arguments.Values[9]!);

        var player = spawn.WriteSet.Single(op => op.Table == TableId.FromName("Player"));
        Assert.Equal("Player", source.TableAt(spawn.Lsn, player.Table)!.Name);
        var key = source.DecodeKey(spawn.Lsn, player.Table, player.Key);
        Assert.Equal(("Id", ColumnKind.Identity, (object?)Identity.Hash("alice")), (key.Column, key.Kind!.Value, key.Value));
        var row = source.DecodeRow(spawn.Lsn, player.Table, player.Row.Span);
        Assert.True(row.Succeeded, row.Error);
        Assert.Equal(
            ["Id", "RoomId", "X", "Y", "Name"],
            row.Columns.Select(static c => c.Name));
        Assert.Equal(7, row.Columns[1].Value);
        Assert.Equal(1.5f, row.Columns[2].Value);
        Assert.Equal("alice", row.Columns[4].Value);

        var rename = records[1];
        Assert.Equal(RowOpKind.Delete, rename.WriteSet.Single(op => op.Table == TableId.FromName("InventoryItem")).Kind);
        var renamed = rename.WriteSet.Single(op => op.Table == TableId.FromName("Player"));
        Assert.Equal("alicia", source.DecodeRow(rename.Lsn, renamed.Table, renamed.Row.Span).Columns[4].Value);

        Assert.Equal("Rename", source.Record(2)!.ReducerName);
        Assert.Null(source.Record(3));
        Assert.Equal(["InventoryItem", "Player"], source.TablesAt(2).Select(static t => t.Name));
        Assert.Equal(InspectChanges.None, source.CheckForChanges());
    }

    [Fact]
    public void Opening_changes_nothing_in_the_directory()
    {
        var harness = Harness();
        Seed(harness);
        harness.Engine.Dispose();
        var directory = harness.Options.CommitLog.Path;
        var before = Listing(directory);

        using (var source = InspectSource.Open(directory))
        {
            _ = source.Records().ToList();
            _ = source.SnapshotRows().ToList();
        }

        Assert.Equal(before, Listing(directory));
    }

    [Fact]
    public void A_compacted_directory_says_which_lsns_were_truncated_away_and_reads_the_snapshot()
    {
        var harness = Harness();
        Seed(harness);
        harness.Options.Resume.RetentionWindowSeconds = 0;
        var snapshotLsn = harness.Engine.TakeSnapshot();
        Assert.NotNull(snapshotLsn);
        harness.Invoke("After", ctx => ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "torch", Quantity = 3 }));
        var baseLsn = harness.Engine.Log.BaseLsn;
        Assert.True(baseLsn > 0);

        var source = Open(harness.Options.CommitLog.Path);
        Assert.Equal(baseLsn, source.Facts.BaseLsn);
        Assert.Equal(baseLsn, source.Facts.LogFloor);
        Assert.Equal(snapshotLsn, source.Facts.SnapshotLsn);
        Assert.NotNull(source.Facts.SnapshotTimestamp);
        Assert.Equal(baseLsn + 1, source.Facts.FirstLsn);
        Assert.All(source.Records(), record => Assert.True(record.Lsn > baseLsn));
        Assert.Null(source.Record(1));

        var refusal = source.ExplainUnreachable(1);
        Assert.NotNull(refusal);
        Assert.Contains($"truncation base", refusal);
        Assert.Contains("OBSERVABILITY.md", refusal);
        Assert.Contains("backup", refusal);
        Assert.Null(source.ExplainUnreachable(source.Facts.HeadLsn));
        Assert.Contains("past the head", source.ExplainUnreachable(source.Facts.HeadLsn + 1));

        // The snapshot's rows decode under the shape that governed the snapshot's LSN.
        var players = source.SnapshotRows().Where(static r => r.Table == TableId.FromName("Player")).ToList();
        var alice = Assert.Single(players);
        Assert.Equal("alicia", source.DecodeRow(source.Facts.SnapshotLsn, alice.Table, alice.Row.Span).Columns[4].Value);
    }

    [Fact]
    public void An_archive_reads_like_its_directory_above_its_snapshot_and_names_its_engine()
    {
        var harness = Harness();
        Seed(harness);
        harness.Options.Resume.RetentionWindowSeconds = 0;
        harness.Engine.TakeSnapshot();
        harness.Invoke("After", ctx => ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "torch", Quantity = 3 }));
        var epoch = harness.Engine.Log.EpochId;
        harness.Engine.Dispose();
        var archive = Path.Combine(TempDir(), "world.mbak");
        var captured = Assert.Single(MelangeBackup.Create(harness.Options.CommitLog.Path, archive).Engines);

        var source = Open(archive);
        var facts = source.Facts;
        Assert.Equal(InspectSourceKind.Archive, facts.Kind);
        Assert.Equal("node", facts.EngineKey);
        Assert.Equal(["node"], facts.Engines);
        Assert.Equal(epoch, facts.Epoch);
        Assert.NotNull(facts.CapturedAt);
        Assert.Equal(captured.SnapshotLsn, facts.SnapshotLsn);
        Assert.Equal(captured.SnapshotLsn, facts.LogFloor);
        Assert.Equal(captured.HeadLsn, facts.HeadLsn);
        Assert.Equal(captured.TailRecords, facts.RecordCount);
        Assert.True(facts.ShapeRecorded);

        var after = Assert.Single(source.Records());
        Assert.Equal("After", after.ReducerName);
        var torch = after.WriteSet.Single();
        Assert.Equal("torch", source.DecodeRow(after.Lsn, torch.Table, torch.Row.Span).Columns[2].Value);
        Assert.Equal(captured.SnapshotRows, source.SnapshotRows().Count());
        Assert.Contains("archive's snapshot", source.ExplainUnreachable(1));
        Assert.Equal(InspectChanges.None, source.CheckForChanges());

        Assert.Contains("no engine 'shard-1'", Assert.Throws<InvalidOperationException>(() => InspectSource.Open(archive, "shard-1")).Message);
    }

    [Fact]
    public void A_restored_directory_carries_its_lineage_and_a_clone_its_provenance()
    {
        var harness = Harness();
        Seed(harness);
        var sourceEpoch = harness.Engine.Log.EpochId;
        harness.Engine.Dispose();
        var archive = Path.Combine(TempDir(), "world.mbak");
        MelangeBackup.Create(harness.Options.CommitLog.Path, archive);

        var restoredDir = Path.Combine(TempDir(), "restored");
        var restored = Assert.Single(MelangeBackup.Restore(archive, restoredDir, new RestoreOptions { AtLsn = 1 }).Engines);
        var source = Open(restoredDir);
        Assert.Equal(restored.NewEpoch, source.Facts.Epoch);
        var ancestor = Assert.Single(source.Facts.Lineage);
        Assert.Equal((sourceEpoch, 1UL), (ancestor.Epoch, ancestor.UpToLsn));
        Assert.Equal(1UL, source.Facts.HeadLsn);
        Assert.Equal("Spawn", Assert.Single(source.Records()).ReducerName);
        Assert.Null(source.Facts.Provenance);

        var cloneDir = Path.Combine(TempDir(), "clone");
        MelangeBackup.Clone(archive, cloneDir);
        var clone = Open(cloneDir);
        Assert.NotNull(clone.Facts.Provenance);
        Assert.Equal(sourceEpoch, clone.Facts.Provenance!.SourceEpoch);
    }

    [Fact]
    public void Each_record_decodes_under_the_shape_that_governed_its_lsn_across_a_migration()
    {
        var root = TempDir();
        var options = new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
        };
        using (var v1 = new MelangeEngine(options, new SchemaRegistry([SchemaShapeTests.Declare<SchemaShapeTests.HeroV1>("Hero", "Id")])))
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new SchemaShapeTests.HeroV1 { Id = 1, X = 10, Name = "alice" }));
        using (var v2 = new MelangeEngine(options, new SchemaRegistry([SchemaShapeTests.Declare<SchemaShapeTests.HeroV2>("Hero", "Id")])))
            v2.Invoke("Later", EngineHarness.Caller, ctx => ctx.Db.Insert(new SchemaShapeTests.HeroV2 { Id = 2, X = 20, Level = 5, Name = "bob", Alive = true }));

        var source = Open(options.CommitLog.Path);
        Assert.Equal(2, source.Shapes!.Entries.Count);
        var seed = source.Records().First(static r => r.ReducerName == "Seed");
        var later = source.Records().First(static r => r.ReducerName == "Later");
        var old = source.DecodeRow(seed.Lsn, seed.WriteSet[0].Table, seed.WriteSet[0].Row.Span);
        var current = source.DecodeRow(later.Lsn, later.WriteSet[0].Table, later.WriteSet[0].Row.Span);
        Assert.True(old.Succeeded && current.Succeeded, old.Error ?? current.Error);
        Assert.Equal(["Id", "X", "Name"], old.Columns.Select(static c => c.Name));
        Assert.Equal("alice", old.Columns[2].Value);
        Assert.Equal(["Id", "X", "Level", "Name", "Alive"], current.Columns.Select(static c => c.Name));
        Assert.Equal(5, current.Columns[2].Value);

        // The wrong shape is reported, never guessed through.
        var misread = RowDecoder.DecodeRow(source.Shapes.Current.Tables["Hero"], seed.WriteSet[0].Row.Span);
        Assert.False(misread.Succeeded);
    }

    [Fact]
    public void A_log_that_changed_owner_reads_across_its_segments_and_never_past_a_cut()
    {
        var directory = TempDir();
        FileCommitLog Log(long term) => new(new CommitLogOptions { Path = directory, Term = term });
        static CommitRequest Request(string reducer) =>
            new(new Timestamp(1), EngineHarness.Caller, reducer, ReadOnlyMemory<byte>.Empty,
                [new RowOp(RowOpKind.Insert, TableId.FromName("Whatever"), new RowKey([1]), new byte[] { 2 })]);

        using (var first = Log(1))
        {
            first.WaitDurable(first.Append(Request("one")).Lsn);
            first.WaitDurable(first.Append(Request("two")).Lsn);
        }

        using (var second = Log(2))
            second.WaitDurable(second.Append(Request("three")).Lsn);

        // The first owner, paused rather than dead, resumes writing into its sealed segment: a
        // whole, valid record at LSN 3, past the cut, which is never history.
        var scratch = TempDir();
        using (var ghost = new FileCommitLog(new CommitLogOptions { Path = scratch }))
        {
            for (var i = 0; i < 3; i++)
                ghost.WaitDurable(ghost.Append(Request("ghost")).Lsn);
        }

        var ghostLog = File.ReadAllBytes(Path.Combine(scratch, "melange.log"));
        using (var stale = new FileStream(Path.Combine(directory, "melange.log.1"), FileMode.Append))
            stale.Write(ghostLog.AsSpan(FileCommitLog.HeaderSize + ((ghostLog.Length - FileCommitLog.HeaderSize) / 3 * 2)));

        var source = Open(directory);
        Assert.Equal(2, source.Facts.Term);
        Assert.Equal(1, source.Facts.SealedSegments);
        Assert.Equal(3UL, source.Facts.HeadLsn);
        Assert.Equal(["one", "two", "three"], source.Records().Select(static r => r.ReducerName));
        Assert.Equal(["two", "three"], source.Records(2).Select(static r => r.ReducerName));
        Assert.Equal(0, source.Facts.TornTailBytes);
        Assert.False(source.Facts.ShapeRecorded);
        Assert.Contains("melange.shape", source.DecodeRow(1, TableId.FromName("Whatever"), [2]).Error);

        // A new owner sealing the live segment after the read opened is reported.
        using (var third = Log(3))
            third.WaitDurable(third.Append(Request("four")).Lsn);
        Assert.True(source.CheckForChanges().OwnerChanged);
        Assert.Equal(3, source.Records().Count());
    }

    [Fact]
    public void A_torn_tail_ends_the_read_and_is_counted_not_refused()
    {
        var harness = Harness();
        Seed(harness);
        harness.Engine.Dispose();
        using (var log = new FileStream(harness.LogFilePath, FileMode.Append))
            log.Write(new byte[] { 0x40, 0, 0, 0, 0xde, 0xad });

        var source = Open(harness.Options.CommitLog.Path);
        Assert.Equal(2UL, source.Facts.HeadLsn);
        Assert.Equal(6, source.Facts.TornTailBytes);
        Assert.Equal(2, source.Records().Count());
    }

    [Fact]
    public void A_record_at_or_below_the_base_is_not_shown_while_a_crash_left_it_in_the_file()
    {
        var harness = Harness();
        Seed(harness);
        harness.Engine.Dispose();

        // Truncation writes the base before it swaps the compacted file in; a crash between the two
        // leaves records at or below the base in the file, which recovery skips, and so does this.
        var baseBytes = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(baseBytes, 1);
        File.WriteAllBytes(Path.Combine(harness.Options.CommitLog.Path, "melange.base"), baseBytes);

        var source = Open(harness.Options.CommitLog.Path);
        Assert.Equal((1UL, 2UL, 2UL, 1L), (source.Facts.LogFloor, source.Facts.FirstLsn, source.Facts.HeadLsn, source.Facts.RecordCount));
        Assert.Equal(["Rename"], source.Records().Select(static r => r.ReducerName));
        Assert.Empty(source.Facts.Gaps);
    }

    [Fact]
    public void A_live_engines_directory_is_read_to_the_head_it_reached_and_the_append_after_is_reported()
    {
        var harness = Harness();
        Seed(harness);

        // The engine holds melange.lock; the reader takes no lock, so it is never in the way.
        var source = Open(harness.Options.CommitLog.Path);
        Assert.Equal(2UL, source.Facts.HeadLsn);
        Assert.False(source.CheckForChanges().Any);

        harness.Invoke("Later", ctx => ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "torch", Quantity = 3 }));
        Assert.True(source.CheckForChanges().Appended);
        Assert.Equal(2, source.Records().Count());

        // And the engine keeps working with the reader's handles open, snapshot and truncation included.
        harness.Options.Resume.RetentionWindowSeconds = 0;
        Assert.NotNull(harness.Engine.TakeSnapshot());
        harness.Invoke("Again", ctx => ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "rope", Quantity = 1 }));
        Assert.True(source.CheckForChanges().Truncated);
        Assert.Equal(["Spawn", "Rename"], source.Records().Select(static r => r.ReducerName));
        using var fresh = InspectSource.Open(harness.Options.CommitLog.Path);
        Assert.Equal(harness.Engine.Log.HeadLsn, fresh.Facts.HeadLsn);
    }

    [Fact]
    public void A_walk_seeks_by_lsn_through_a_log_longer_than_one_index_stride()
    {
        var directory = TempDir();
        using (var log = new FileCommitLog(new CommitLogOptions { Path = directory, FsyncPolicy = FsyncPolicy.Interval }))
        {
            for (var i = 1; i <= 2500; i++)
                log.Append(new CommitRequest(new Timestamp(i), EngineHarness.Caller, $"r{i}", ReadOnlyMemory<byte>.Empty, []));
        }

        var source = Open(directory);
        Assert.Equal(2500UL, source.Facts.HeadLsn);
        Assert.Equal("r1500", source.Record(1500)!.ReducerName);
        Assert.Equal(["r2047", "r2048", "r2049"], source.Records(2047, 2049).Select(static r => r.ReducerName));
        Assert.Equal(2500, source.Records().Count());

        var archive = Path.Combine(TempDir(), "long.mbak");
        MelangeBackup.Create(directory, archive);
        var archived = Open(archive);
        Assert.Equal("r1025", archived.Record(1025)!.ReducerName);
        Assert.Equal(["r2499", "r2500"], archived.Records(2499).Select(static r => r.ReducerName));
    }

    [Fact]
    public void A_path_that_is_not_a_source_says_what_it_is()
    {
        var root = TempDir();
        Directory.CreateDirectory(Path.Combine(root, "shard-1"));
        File.WriteAllBytes(Path.Combine(root, "shard-1", "melange.log"), []);
        var refusal = Assert.Throws<InvalidOperationException>(() => InspectSource.Open(root)).Message;
        Assert.Contains("shard-1", refusal);
        var empty = Directory.CreateDirectory(Path.Combine(root, "empty")).FullName;
        Assert.Contains("not a MelangeDB data directory", Assert.Throws<InvalidOperationException>(() => InspectSource.Open(empty)).Message);

        var notAnArchive = Path.Combine(root, "notes.txt");
        File.WriteAllText(notAnArchive, "hello");
        Assert.Throws<InvalidDataException>(() => InspectSource.Open(notAnArchive));
    }

    private static List<string> Listing(string directory) =>
        [.. Directory.EnumerateFiles(directory)
            .Select(path => $"{Path.GetFileName(path)}|{new FileInfo(path).Length}|{File.GetLastWriteTimeUtc(path):O}")
            .Order(StringComparer.Ordinal)];
}
