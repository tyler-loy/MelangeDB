using System.Diagnostics;
using System.Globalization;

namespace MelangeDB.LoadTest;

/// <summary>
/// The tool's entry point behind <c>Program</c>, callable in-process (the smoke test drives it
/// exactly as the console does). Three subcommands: <c>serve</c> hosts the cluster, <c>drive</c>
/// generates load against an address, and <c>all</c> runs both for a one-command local run —
/// with the serve side in a separate process by default, so server nodes and the load driver
/// never fight for one thread pool.
/// </summary>
public static class LoadTestTool
{
    public static async Task<int> RunAsync(string[] args, TextWriter output, CancellationToken ct = default)
    {
        if (args.Length == 0 || args[0] is not ("serve" or "drive" or "all"))
        {
            output.WriteLine("Usage: MelangeDB.LoadTest <serve|drive|all> [flags] — see docs/LOAD-TESTING.md.");
            output.WriteLine("  serve  host the hub + shard nodes (prints GATEWAY, STATS, READY)");
            output.WriteLine("  drive  connect players and generate load: --address ws://host:port/gateway");
            output.WriteLine("  all    serve in a child process, then drive against it");
            output.WriteLine("  presets: --smoke (small, <30 s)  --soak (5 min, leak watching)");
            return 2;
        }

        if (LoadTestOptions.Parse(args, 1, output) is not { } options)
            return 2;

        try
        {
            return args[0] switch
            {
                "serve" => await ServeAsync(options, output, ct).ConfigureAwait(false),
                "drive" => (await LoadTestDriver.RunAsync(options, output, ct).ConfigureAwait(false)).Pass ? 0 : 1,
                _ => await AllAsync(options, output, ct).ConfigureAwait(false),
            };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            output.WriteLine("Cancelled.");
            return 1;
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException or TimeoutException)
        {
            output.WriteLine($"Error: {exception.Message}");
            return 1;
        }
    }

    private static async Task<int> ServeAsync(LoadTestOptions options, TextWriter output, CancellationToken ct)
    {
        await using var server = await LoadTestServer.StartAsync(options, output, ct).ConfigureAwait(false);
        var clock = Stopwatch.StartNew();
        try
        {
            // Idle until told to stop, printing the same counters the stats endpoint serves —
            // a remote driver cannot see this console, but an operator watching serve can.
            while (options.ServeSeconds == 0 || clock.Elapsed < TimeSpan.FromSeconds(options.ServeSeconds))
            {
                await Task.Delay(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                var stats = server.Stats();
                output.WriteLine(
                    $"[serve {clock.Elapsed.TotalSeconds,5:F0}s] handoffs {stats.HandoffsCompleted} completed " +
                    $"{stats.HandoffsAborted} aborted {stats.HandoffsInFlight} in flight; working set " +
                    $"{stats.WorkingSetBytes / (1024 * 1024)} MiB, GC heap {stats.GcHeapBytes / (1024 * 1024)} MiB, " +
                    $"gen2 {stats.Gen2Collections}");
            }
        }
        catch (OperationCanceledException)
        {
            output.WriteLine("Serve: shutting down.");
        }

        return 0;
    }

    private static async Task<int> AllAsync(LoadTestOptions options, TextWriter output, CancellationToken ct)
    {
        if (options.InProcessServer)
        {
            output.WriteLine(
                "all --in-process-server: serve and drive share this process and its thread pool — " +
                "fine for a smoke run, contaminated as a measurement; use the default child-process mode for numbers.");
            await using var server = await LoadTestServer.StartAsync(options, output, ct).ConfigureAwait(false);
            options.Address = server.GatewayUri;
            var result = await LoadTestDriver.RunAsync(options, output, ct).ConfigureAwait(false);
            return result.Pass ? 0 : 1;
        }

        var exe = Environment.ProcessPath
            ?? throw new InvalidOperationException("Cannot locate this executable to spawn the serve process; run serve and drive separately.");
        var start = new ProcessStartInfo(exe)
        {
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        foreach (var argument in ServeArguments(options))
            start.ArgumentList.Add(argument);
        using var child = Process.Start(start)
            ?? throw new InvalidOperationException("Failed to start the serve process.");
        try
        {
            var ready = new TaskCompletionSource<Uri>(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = Task.Run(async () =>
            {
                Uri? gateway = null;
                while (await child.StandardOutput.ReadLineAsync(CancellationToken.None).ConfigureAwait(false) is { } line)
                {
                    output.WriteLine($"[serve] {line}");
                    if (line.StartsWith("GATEWAY ", StringComparison.Ordinal))
                        gateway = new Uri(line["GATEWAY ".Length..]);
                    if (line == "READY" && gateway is not null)
                        ready.TrySetResult(gateway);
                }

                ready.TrySetException(new InvalidOperationException("The serve process exited before READY."));
            }, CancellationToken.None);

            options.Address = await ready.Task.WaitAsync(TimeSpan.FromSeconds(120), ct).ConfigureAwait(false);
            var result = await LoadTestDriver.RunAsync(options, output, ct).ConfigureAwait(false);
            return result.Pass ? 0 : 1;
        }
        finally
        {
            try
            {
                child.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                // Already exited.
            }
        }
    }

    /// <summary>The serve flags implied by an <c>all</c> invocation's options.</summary>
    private static List<string> ServeArguments(LoadTestOptions options) =>
    [
        "serve",
        "--nodes", options.Nodes.ToString(CultureInfo.InvariantCulture),
        "--world-blocks", $"{options.WorldBlocksX}x{options.WorldBlocksY}",
        "--block-chunks", $"{options.BlockChunksX}x{options.BlockChunksY}",
        "--band", options.BandChunks.ToString(CultureInfo.InvariantCulture),
        "--margin", options.MarginChunks.ToString(CultureInfo.InvariantCulture),
        "--handoff-min-ms", options.HandoffMinIntervalMs.ToString(CultureInfo.InvariantCulture),
        "--fsync", options.Fsync,
        "--fsync-interval-ms", options.FsyncIntervalMs.ToString(CultureInfo.InvariantCulture),
        "--port", options.Port.ToString(CultureInfo.InvariantCulture),
        "--listen", options.Listen,
    ];
}
