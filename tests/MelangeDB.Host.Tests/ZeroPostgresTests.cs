using MelangeDB.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The zero-Postgres done-criterion: a deployment that configures no Postgres starts and runs —
/// including one that declares relational-tier tables, whose rows simply live in the hot store
/// with the missing projection announced loudly (EventId 1607), not discovered by surprise.
/// </summary>
public class ZeroPostgresTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-zero-pg-").FullName;

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
    public async Task Relational_tables_without_postgres_run_from_the_hot_store_and_warn()
    {
        var logs = new LogCollector();
        using var host = TestApp.Build(_root, configure: builder => builder.Logging.AddProvider(logs));
        await host.StartAsync(TestContext.Current.CancellationToken);

        host.Reducers().Call("Archive", TestApp.Caller, "no postgres anywhere");
        var table = host.Engine().Schema.Get(typeof(AuditArchive));
        Assert.Equal(1, host.Engine().HotStore.Count(table.Id));

        Assert.Contains(logs.Entries, e =>
            e.EventId == 1607 && e.Level == LogLevel.Warning && e.Message.Contains("AuditArchive"));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
