using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Xunit;
using static MelangeDB.Core.Tests.SchemaShapeTests;

namespace MelangeDB.Core.Tests;

/// <summary>
/// A <c>[Unique]</c> declared over rows that already exist is checked against them, not assumed
/// (road-to-0.4 phase 27, issue #192). Before this, the boot succeeded, the unique index held
/// several rows for one value, and only the next insert was refused.
/// </summary>
public class UniqueConstraintMigrationTests : IDisposable
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

    // X indexed, or X unique. Uniqueness is the only difference: the bytes are the same shape.
    private static TableSchema Indexed() => Declare<HeroV1>("Hero", "Id", index: "X");

    private static TableSchema UniqueX() => Declare<HeroV1>("Hero", "Id", unique: "X");

    private MelangeDbOptions NewOptions()
    {
        var root = Directory.CreateTempSubdirectory("melange-unique-").FullName;
        _roots.Add(root);
        return new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
            Resume = { RetentionWindowSeconds = 0 },
        };
    }

    private static MelangeEngine Boot(MelangeDbOptions options, params TableSchema[] tables) =>
        new(options, new SchemaRegistry(tables));

    private static void Seed(MelangeDbOptions options, TableSchema table, params (ulong Id, int X)[] heroes)
    {
        using var engine = Boot(options, table);
        engine.Invoke("Seed", EngineHarness.Caller, ctx =>
        {
            foreach (var (id, x) in heroes)
                ctx.Db.Insert(new HeroV1 { Id = id, X = x, Name = $"hero-{id}" });
        });
    }

    private static string SidecarPath(MelangeDbOptions options) =>
        Path.Combine(options.CommitLog.Path, ShapeHistory.FileName);

    private static ShapeHistory Sidecar(MelangeDbOptions options) => ShapeHistory.Load(SidecarPath(options))!;

    private static ulong HeadOf(MelangeDbOptions options, TableSchema table)
    {
        using var probe = Boot(options, table);
        return probe.Log.HeadLsn;
    }

    /// <summary>Rewrites the sidecar as a build before phase 27 wrote it: no Unique field anywhere.</summary>
    private static void StripUniqueFromSidecar(MelangeDbOptions options)
    {
        var path = SidecarPath(options);
        var json = JsonNode.Parse(File.ReadAllText(path))!;
        foreach (var entry in json["Entries"]!.AsArray())
        {
            foreach (var (_, table) in entry!["Tables"]!.AsObject())
                table!.AsObject().Remove("Unique");
        }

        File.WriteAllText(path, json.ToJsonString());
        Assert.All(Sidecar(options).Current.Tables.Values, t => Assert.Null(t.Unique));
    }

    [Fact]
    public void Declaring_unique_over_duplicate_rows_refuses_naming_each_value_and_the_keys_holding_it()
    {
        var options = NewOptions();
        Seed(options, Indexed(), (1, 10), (2, 10), (3, 20), (4, 20), (5, 20), (6, 30));
        var sidecarBefore = File.ReadAllBytes(SidecarPath(options));
        var headBefore = HeadOf(options, Indexed());

        var refusal = Assert.Throws<SchemaShapeException>(() => Boot(options, UniqueX()));

        var reason = Assert.Single(refusal.Reasons);
        Assert.Contains("table 'Hero': column 'X' is declared [Unique] but 2 value(s)", reason);
        Assert.Contains("10 (held by Id 1, 2)", reason);
        Assert.Contains("20 (held by Id 3, 4, 5)", reason);
        Assert.DoesNotContain("30", reason);
        Assert.Contains("Dedupe under the previous declaration", refusal.Message);

        // The refusal records nothing: no marker, no new reign, and the previous declaration boots
        // exactly as before.
        Assert.Equal(sidecarBefore, File.ReadAllBytes(SidecarPath(options)));
        using var previous = Boot(options, Indexed());
        Assert.Equal(headBefore, previous.Log.HeadLsn);
        Assert.Equal(6, previous.HotStore.Scan(previous.Schema.Get(typeof(HeroV1)).Id).Count());
    }

    [Fact]
    public void Declaring_unique_over_distinct_rows_migrates_and_is_recorded()
    {
        var options = NewOptions();
        Seed(options, Indexed(), (1, 10), (2, 20));
        var headBefore = HeadOf(options, Indexed());

        using (var unique = Boot(options, UniqueX()))
        {
            // A recorded reign, like any additive change: the constraint has a place in the log.
            Assert.Equal(headBefore + 1, unique.Log.HeadLsn);
            Assert.Equal(["X"], Sidecar(options).Current.Tables["Hero"].Unique);

            var refused = Assert.Throws<InvalidOperationException>(() =>
                unique.Invoke("Dup", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 3, X = 10, Name = "c" })));
            Assert.Contains("unique constraint on 'X' violated", refused.Message);
        }

        // And it is checked once: the next boot is the fast path — no marker, nothing to check.
        var headAfter = HeadOf(options, UniqueX());
        using var again = Boot(options, UniqueX());
        Assert.Equal(headAfter, again.Log.HeadLsn);
        Assert.Equal(["X"], Sidecar(options).Current.Tables["Hero"].Unique);
    }

    [Fact]
    public void Dropping_unique_is_additive_and_recorded()
    {
        var options = NewOptions();
        Seed(options, UniqueX(), (1, 10), (2, 20));
        var headBefore = HeadOf(options, UniqueX());

        using var relaxed = Boot(options, Indexed());

        Assert.Equal(headBefore + 1, relaxed.Log.HeadLsn);
        Assert.Empty(Sidecar(options).Current.Tables["Hero"].Unique!);
        relaxed.Invoke("Dup", EngineHarness.Caller, ctx => ctx.Db.Insert(new HeroV1 { Id = 3, X = 10, Name = "c" }));
    }

    [Fact]
    public void A_column_added_as_unique_names_its_default_and_says_why()
    {
        var options = NewOptions();
        Seed(options, Indexed(), (1, 10), (2, 20));

        // V2 adds Level and Alive; Level arrives [Unique], so both existing rows hold 0.
        var refusal = Assert.Throws<SchemaShapeException>(
            () => Boot(options, Declare<HeroV2>("Hero", "Id", index: "X", unique: "Level")));

        var reason = Assert.Single(refusal.Reasons);
        Assert.Contains("column 'Level'", reason);
        Assert.Contains("0 (held by Id 1, 2)", reason);
        Assert.Contains("new in this deploy", reason);
    }

    [Fact]
    public void An_empty_table_takes_unique_without_a_check_to_fail()
    {
        var options = NewOptions();
        using (var first = Boot(options, Indexed()))
            first.Invoke("Touch", EngineHarness.Caller, _ => { });

        using var unique = Boot(options, UniqueX());
        Assert.Equal(["X"], Sidecar(options).Current.Tables["Hero"].Unique);
    }

    [Fact]
    public void A_sidecar_that_predates_uniqueness_recording_has_every_unique_column_checked_once()
    {
        var options = NewOptions();
        Seed(options, UniqueX(), (1, 10), (2, 20));
        var headBefore = HeadOf(options, UniqueX());
        StripUniqueFromSidecar(options);
        var logs = new LogCapture();

        using (var upgraded = new MelangeEngine(options, new SchemaRegistry([UniqueX()]), logs))
        {
            // Filled in place, not as a reign: nothing about the schema changed.
            Assert.Equal(headBefore, upgraded.Log.HeadLsn);
            Assert.Equal(["X"], Sidecar(options).Current.Tables["Hero"].Unique);
        }

        var entry = logs.Single(1010);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal(1d, entry.Number("Count"));
        Assert.Equal(1d, entry.Number("Columns"));
        Assert.Equal(options.CommitLog.Path, entry.Fields["Directory"]);
        Assert.Contains("checking 1 unique column(s)", entry.Message);
    }

    [Fact]
    public void A_sidecar_that_predates_uniqueness_recording_refuses_duplicates_an_earlier_build_let_in()
    {
        // Issue #192 as it would have happened: [Unique] added over duplicates under a build that
        // never checked and never recorded uniqueness. The upgrade boot is the one that can catch it.
        var options = NewOptions();
        Seed(options, Indexed(), (1, 10), (2, 10));
        StripUniqueFromSidecar(options);

        var refusal = Assert.Throws<SchemaShapeException>(() => Boot(options, UniqueX()));

        Assert.Contains("10 (held by Id 1, 2)", Assert.Single(refusal.Reasons));
        Assert.All(Sidecar(options).Current.Tables.Values, t => Assert.Null(t.Unique));
    }

    [Fact]
    public void Adopting_over_existing_records_checks_unique_columns_rather_than_assuming_them()
    {
        var options = NewOptions();
        Seed(options, Indexed(), (1, 10), (2, 10));
        File.Delete(SidecarPath(options));
        options.Schema.AllowAdoption = true;

        var refusal = Assert.Throws<SchemaShapeException>(() => Boot(options, UniqueX()));

        Assert.Contains("10 (held by Id 1, 2)", Assert.Single(refusal.Reasons));
    }

    [Fact]
    public void Null_is_never_a_duplicate()
    {
        var options = NewOptions();
        var nullable = Declare<CounterV2>("Counter", "Id", index: "Label");
        using (var engine = Boot(options, nullable))
        {
            engine.Invoke("Seed", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new CounterV2 { Id = 1, Label = null, Value = 1 });
                ctx.Db.Insert(new CounterV2 { Id = 2, Label = null, Value = 2 });
                ctx.Db.Insert(new CounterV2 { Id = 3, Label = "a", Value = 3 });
            });
        }

        using var unique = Boot(options, Declare<CounterV2>("Counter", "Id", unique: "Label"));
        Assert.Equal(["Label"], Sidecar(options).Current.Tables["Counter"].Unique);
    }

    [Fact]
    public void A_dry_run_reports_the_change_and_the_columns_to_check()
    {
        var options = NewOptions();
        Seed(options, Indexed(), (1, 10));

        var diff = ShapeCompatibility.Compare(Sidecar(options).Current, new SchemaRegistry([UniqueX()]));

        Assert.True(diff.IsAdditive);
        Assert.Contains(diff.AdditiveChanges, c => c.Contains("column 'X' is now [Unique]", StringComparison.Ordinal));
        Assert.Equal([new ShapeUniqueColumn("Hero", "X", ColumnIsNew: false)], diff.UniqueToCheck);
    }

    [Fact]
    public void Refusals_name_at_most_a_bounded_number_of_values_and_keys()
    {
        var options = NewOptions();
        // Twelve values, six rows each: past both bounds (ten values named, five keys per value).
        var heroes = Enumerable.Range(0, 72).Select(i => ((ulong)(i + 1), i % 12)).ToArray();
        Seed(options, Indexed(), heroes);

        var reason = Assert.Single(Assert.Throws<SchemaShapeException>(() => Boot(options, UniqueX())).Reasons);

        Assert.Contains("12 value(s)", reason);
        Assert.Contains("; and 2 more duplicated value(s)", reason);
        Assert.Contains("0 (held by Id 1, 13, 25, 37, 49, and 1 more)", reason);
    }
}
