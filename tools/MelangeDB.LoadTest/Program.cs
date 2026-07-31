using MelangeDB.LoadTest;

// Load-tests a MelangeDB spatial cluster with real clients over real sockets. Methodology, flags,
// and measured numbers: docs/LOAD-TESTING.md.
using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};
return await LoadTestTool.RunAsync(args, Console.Out, cancellation.Token);
