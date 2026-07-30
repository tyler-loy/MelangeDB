using MelangeDB.Client;
using MelangeDB.Sample;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The phase-03 demonstrable: the sample worker serves a websocket, and a MelangeDB.Client
/// connects to it, calls a reducer, and sees the resulting row arrive as a live delta — the exact
/// flow the console client sample performs.
/// </summary>
public class SampleSocketTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-sample-socket-").FullName;

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
    public async Task A_client_connects_to_the_sample_worker_calls_Greet_and_sees_the_visitor_delta()
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

        await using var client = new MelangeClient(new MelangeClientOptions { Uri = uri });
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var visitors = await client.SubscribeAsync("SELECT * FROM Visitor", cancellationToken: TestContext.Current.CancellationToken);
        var arrived = new TaskCompletionSource<MelangeRow>(TaskCreationOptions.RunContinuationsAsynchronously);
        visitors.OnInsert += row =>
        {
            if ((string?)row.Columns["Name"] == "SocketCaller")
                arrived.TrySetResult(row);
        };

        var lsn = await client.CallReducerAsync("Greet", ["SocketCaller"], TestContext.Current.CancellationToken);
        Assert.True(lsn > 0);
        var delta = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        Assert.Equal("SocketCaller", delta.Columns["Name"]);
        Assert.True(visitors.Count >= 1);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
