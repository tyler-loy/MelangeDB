using System.Diagnostics;
using MelangeDB.Core;
using MelangeDB.Sample;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The sample worker, end to end: the exact composition the executable runs, from
/// Host.CreateApplicationBuilder through generated registration to committed rows — including the
/// feature-flag scenario, where one config change alters the running reducer with no restart.
/// </summary>
public class SampleEndToEndTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-sample-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public async Task Sample_worker_commits_greetings_and_honours_a_live_feature_flag()
    {
        int visitors;
        using (var host = BuildSample())
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            var engine = host.Services.GetRequiredService<MelangeEngine>();

            // The worker greets on its own schedule; wait for the first committed transaction.
            var head = await WaitForCommitAsync(engine, 0);
            Assert.All(Visitors(engine), visitor => Assert.False(visitor.GreetedExcitedly));

            // Flip the feature flag with no restart; the next greeting behaves differently.
            var root = (IConfigurationRoot)host.Services.GetRequiredService<IConfiguration>();
            root["Sample:Greeting:Excited"] = "true";
            root.Reload();

            // Wait two commits: one may already have been in flight when the flag flipped; the one
            // after it necessarily started — and read the flag — after the reload.
            await WaitForCommitAsync(engine, head + 1);
            Assert.True(Visitors(engine).Last().GreetedExcitedly);

            // Both tables committed atomically: totals match visitors.
            visitors = Visitors(engine).Count;
            var total = Totals(engine);
            Assert.Equal(visitors, (int)total);

            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        // And the whole thing survives a restart from the log alone.
        using (var second = BuildSample())
        {
            await second.StartAsync(TestContext.Current.CancellationToken);
            var recovered = second.Services.GetRequiredService<MelangeEngine>();
            Assert.True(Visitors(recovered).Count >= visitors);
            await second.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public void Every_table_declared_in_the_sample_assembly_is_registered_without_manual_registration()
    {
        using var host = BuildSample();
        var registry = host.Services.GetRequiredService<SchemaRegistry>();
        var declared = typeof(Visitor).Assembly.GetTypes()
            .Where(t => t.GetCustomAttributes(typeof(TableAttribute), inherit: false).Length > 0)
            .OrderBy(t => t.Name, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(declared);
        foreach (var table in declared)
            Assert.Contains(registry.Tables, schema => schema.RowType == table);
        Assert.Equal(declared.Count, registry.Tables.Count);
    }

    private IHost BuildSample() => SampleHost.Build([], builder =>
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(_root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(_root, "hot"),
            ["Sample:Greeting:Excited"] = "false",
            ["Logging:LogLevel:Default"] = "Warning",
        }));

    private static async Task<ulong> WaitForCommitAsync(MelangeEngine engine, ulong pastLsn)
    {
        var stopwatch = Stopwatch.StartNew();
        while (engine.Log.HeadLsn <= pastLsn)
        {
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(30), "Timed out waiting for the sample worker to commit.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        return engine.Log.HeadLsn;
    }

    private static List<Visitor> Visitors(MelangeEngine engine)
    {
        var visitors = new List<Visitor>();
        engine.Invoke("VerifyVisitors", Identity.Hash("test-observer"), ctx => visitors.AddRange(
            ctx.Db.Scan<Visitor>().OrderBy(v => v.Id)));
        return visitors;
    }

    private static long Totals(MelangeEngine engine)
    {
        long count = 0;
        engine.Invoke("VerifyTotals", Identity.Hash("test-observer"), ctx =>
            count = ctx.Db.Find<GreetingTotal>((byte)0)?.Count ?? 0);
        return count;
    }
}
