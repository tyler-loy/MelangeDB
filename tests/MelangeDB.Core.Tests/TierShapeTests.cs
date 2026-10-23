using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// A table's tier in the shape sidecar (road-to-0.3 phase 25): recorded by a stable name, compared
/// at boot, and filled in once for a sidecar written before tiers were recorded. Between
/// <c>Hot</c> and <c>HotWithRelationalBackplane</c> the rows stay in the hot store, so the change
/// is additive and becomes a new reign; any tier this build does not know is refused.
/// </summary>
public class TierShapeTests : IDisposable
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

    public struct Ledger
    {
        public ulong Id;
        public long Delta;
    }

    private static TableSchema LedgerTable(StorageTier tier)
    {
        var declared = SchemaShapeTests.Declare<Ledger>("Ledger", "Id");
        return new TableSchema(declared.RowType, declared.Name, declared.Columns, tier: tier);
    }

    private MelangeDbOptions NewOptions()
    {
        var root = Directory.CreateTempSubdirectory("melange-tier-").FullName;
        _roots.Add(root);
        return new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
            Resume = { RetentionWindowSeconds = 0 },
        };
    }

    private static MelangeEngine Boot(MelangeDbOptions options, StorageTier tier, ILoggerFactory? loggers = null) =>
        new(options, new SchemaRegistry([LedgerTable(tier)]), loggers);

    private static string SidecarPath(MelangeDbOptions options) =>
        Path.Combine(options.CommitLog.Path, ShapeHistory.FileName);

    private static ShapeHistory Sidecar(MelangeDbOptions options) => ShapeHistory.Load(SidecarPath(options))!;

    private static void Seed(MelangeEngine engine, ulong id) =>
        engine.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new Ledger { Id = id, Delta = (long)id * 10 }));

    private static int LedgerRows(MelangeEngine engine) =>
        engine.HotStore.Scan(engine.Schema.Get(typeof(Ledger)).Id).Count();

    /// <summary>Rewrites every table's recorded tier in the sidecar; null removes it, as a 0.2 sidecar has none.</summary>
    private static void RewriteTiers(MelangeDbOptions options, string? tier)
    {
        var root = JsonNode.Parse(File.ReadAllText(SidecarPath(options)))!;
        foreach (var entry in root["Entries"]!.AsArray())
        {
            foreach (var (_, shape) in entry!["Tables"]!.AsObject())
            {
                var table = shape!.AsObject();
                table.Remove("Tier");
                if (tier is not null)
                    table["Tier"] = tier;
            }
        }

        File.WriteAllText(SidecarPath(options), root.ToJsonString());
    }

    [Fact]
    public void A_new_sidecar_records_each_tier_by_its_stable_name()
    {
        var options = NewOptions();
        using (Boot(options, StorageTier.HotWithRelationalBackplane))
        {
        }

        Assert.Equal(ShapeTiers.HotWithRelationalBackplane, Sidecar(options).Current.Tables["Ledger"].Tier);

        // The aliased 0.2 name must never reach disk: which of two names sharing a value the enum
        // formats to is unspecified, and a persisted name has to read the same on every runtime.
        var json = File.ReadAllText(SidecarPath(options));
        Assert.Contains("\"HotWithRelationalBackplane\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"Relational\"", json, StringComparison.Ordinal);
    }

    [Fact]
    public void Every_tier_has_a_sidecar_name()
    {
        foreach (var tier in Enum.GetValues<StorageTier>())
            Assert.False(string.IsNullOrEmpty(ShapeTiers.NameOf(tier)));
    }

    [Theory]
    [InlineData(StorageTier.Hot, StorageTier.HotWithRelationalBackplane)]
    [InlineData(StorageTier.HotWithRelationalBackplane, StorageTier.Hot)]
    public void A_change_between_hot_resident_tiers_is_an_additive_migration_that_keeps_every_row(StorageTier from, StorageTier to)
    {
        var options = NewOptions();
        using (var before = Boot(options, from))
        {
            Seed(before, 1);
            Seed(before, 2);
        }

        var logs = new LogCapture();
        using (var after = Boot(options, to, logs))
        {
            Assert.Equal(2, LedgerRows(after));

            var sidecar = Sidecar(options);
            Assert.Equal(2, sidecar.Entries.Count);
            Assert.Equal(ShapeTiers.NameOf(to), sidecar.Current.Tables["Ledger"].Tier);
            Assert.Equal(after.Log.HeadLsn, sidecar.Current.FromLsn);

            var migrated = logs.Single(1006);
            Assert.Contains($"tier {ShapeTiers.NameOf(from)} -> {ShapeTiers.NameOf(to)}", migrated.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void An_unchanged_tier_boots_on_the_fast_path()
    {
        var options = NewOptions();
        using (var first = Boot(options, StorageTier.HotWithRelationalBackplane))
            Seed(first, 1);

        var logs = new LogCapture();
        using (Boot(options, StorageTier.HotWithRelationalBackplane, logs))
        {
        }

        Assert.Single(Sidecar(options).Entries);
        Assert.DoesNotContain(logs.Entries, e => e.EventId is 1006 or 1009);
    }

    [Fact]
    public void A_sidecar_from_before_tiers_were_recorded_has_them_filled_in_place_once()
    {
        var options = NewOptions();
        using (var first = Boot(options, StorageTier.HotWithRelationalBackplane))
            Seed(first, 1);
        RewriteTiers(options, tier: null);
        var entriesBefore = Sidecar(options).Entries.Count;

        var logs = new LogCapture();
        using (var upgraded = Boot(options, StorageTier.HotWithRelationalBackplane, logs))
        {
            Assert.Equal(1, LedgerRows(upgraded));

            // In place, not a new reign: the tier never changes what a record's bytes mean.
            var sidecar = Sidecar(options);
            Assert.Equal(entriesBefore, sidecar.Entries.Count);
            Assert.Equal(ShapeTiers.HotWithRelationalBackplane, sidecar.Current.Tables["Ledger"].Tier);

            var recorded = logs.Single(1009);
            Assert.Equal(LogLevel.Information, recorded.Level);
            Assert.Equal(1d, recorded.Number("Count"));
            Assert.DoesNotContain(logs.Entries, e => e.EventId == 1006);
        }

        var again = new LogCapture();
        using (Boot(options, StorageTier.HotWithRelationalBackplane, again))
            Assert.DoesNotContain(again.Entries, e => e.EventId == 1009);
    }

    [Fact]
    public void A_recorded_tier_this_build_does_not_know_refuses_boot()
    {
        var options = NewOptions();
        using (var first = Boot(options, StorageTier.Hot))
            Seed(first, 1);

        // What a newer build's tier looks like to this one: a name it cannot place, so it cannot
        // know whether the rows are in its hot store. Booting it as Hot could present an empty
        // table as the whole truth.
        RewriteTiers(options, tier: "Columnar");

        var refused = Assert.Throws<SchemaShapeException>(() => Boot(options, StorageTier.Hot));
        var reason = Assert.Single(refused.Reasons);
        Assert.Contains("tier Columnar -> Hot is not a supported change", reason, StringComparison.Ordinal);
    }
}
