using MelangeDB.Core;
using Microsoft.Extensions.Hosting;

namespace MelangeDB.Sample;

/// <summary>Drives the sample: greets a rotating cast of visitors until the host shuts down.</summary>
public sealed class GreeterWorker(MelangeReducerHost reducers) : BackgroundService
{
    private static readonly string[] Names = ["Ada", "Grace", "Barbara", "Linus"];

    private static readonly Identity Caller = Identity.Hash("sample-worker");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var visit = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            reducers.Call("Greet", Caller, Names[visit++ % Names.Length]);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
