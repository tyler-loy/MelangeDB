using MelangeDB.Client;
using MelangeDB.Sample;
using MelangeDB.Types;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The in-repo proof that one schema serves a server tree and a client tree: the sample worker
/// exports its manifest, the committed JSON is held byte-identical to the build, and the sample
/// client's generated bindings — the actual code in samples/MelangeDB.Sample.Client — greet and
/// observe a typed visitor against the real worker.
/// </summary>
public class SampleManifestTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-sample-manifest-").FullName;

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
    public void The_committed_sample_manifest_matches_the_worker_build()
    {
        // The manifest the sample client generates from is committed next to the worker; if the
        // worker's tables or reducers change, this fails until it is re-exported — schema drift
        // becomes a build break, never a runtime null. Re-export with:
        //   dotnet run --project src/MelangeDB.Cli -- schema samples/MelangeDB.Sample.Worker/bin/Debug/net10.0/MelangeDB.Sample.Worker.dll -o samples/MelangeDB.Sample.Worker/melange-schema.json
        var committed = File.ReadAllText(Path.Combine(RepoRoot(), "samples", "MelangeDB.Sample.Worker", "melange-schema.json"));
        var embedded = (string)typeof(MelangeDB.Sample.Visitor).Assembly
            .GetType("MelangeDB.Generated.MelangeSchemaManifest")!
            .GetField("Json", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetRawConstantValue()!;
        Assert.Equal(embedded, committed);
    }

    [Fact]
    public async Task The_sample_clients_generated_bindings_greet_the_real_worker()
    {
        using var host = SampleHost.Build([], builder =>
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["MelangeDb:CommitLog:Path"] = Path.Combine(_root, "log"),
                ["MelangeDb:HotStore:Path"] = Path.Combine(_root, "hot"),
                ["Logging:LogLevel:Default"] = "Warning",
                ["Urls"] = "http://127.0.0.1:0",
            }));
        await host.StartAsync(TestContext.Current.CancellationToken);
        var address = host.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        var uri = new Uri(new Uri(address.Replace("http://", "ws://")), "/melange");

        await using var client = new MelangeClient(new MelangeClientOptions
        {
            Uri = uri,
            Token = DevIdentity.MintToken("typed-sample-test"),
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeConnection(client);

        var arrived = new TaskCompletionSource<MelangeDB.Types.Visitor>(TaskCreationOptions.RunContinuationsAsynchronously);
        conn.Db.Visitor.OnInsert += visitor =>
        {
            if (visitor.Name == "TypedCaller")
                arrived.TrySetResult(visitor);
        };
        await conn.Db.Visitor.SubscribeAllAsync(TestContext.Current.CancellationToken);

        var lsn = await conn.Reducers.GreetAsync("TypedCaller", TestContext.Current.CancellationToken);
        Assert.True(lsn > 0);
        var typed = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

        // Typed through and through: AutoInc assigned the id, the reducer stamped ctx.Timestamp,
        // and the local cache answers the primary-key lookup.
        Assert.True(typed.Id > 0);
        Assert.True(typed.VisitedAt.UnixTimeMicroseconds > 0);
        Assert.Equal("TypedCaller", conn.Db.Visitor.Id.Find(typed.Id)!.Value.Name);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "MelangeDB.slnx")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        return directory.FullName;
    }
}
