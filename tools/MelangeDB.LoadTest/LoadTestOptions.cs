using System.Globalization;

namespace MelangeDB.LoadTest;

/// <summary>
/// Every knob the tool takes, with the defaults a first local run wants. One options type serves
/// all three subcommands; <c>serve</c> reads the world/cluster half, <c>drive</c> reads the
/// workload half, and <c>all</c> passes both through. The tool's flags are documented in
/// docs/LOAD-TESTING.md (they are the tool's, not MelangeDB option keys).
/// </summary>
public sealed class LoadTestOptions
{
    // ---- serve: cluster and world shape ----

    /// <summary>Shard node count. Shards are assigned least-loaded-first across them.</summary>
    public int Nodes { get; set; } = 4;

    /// <summary>World width, in blocks (one block = one shard).</summary>
    public int WorldBlocksX { get; set; } = 2;

    /// <summary>World height, in blocks.</summary>
    public int WorldBlocksY { get; set; } = 2;

    /// <summary>Block width, in chunks.</summary>
    public int BlockChunksX { get; set; } = 8;

    /// <summary>Block height, in chunks.</summary>
    public int BlockChunksY { get; set; } = 8;

    /// <summary>
    /// Border band depth in chunks (Cluster:BorderBandChunks). 3 rather than the library default
    /// of 2, applying the documented derivation to this workload's walk speed: walkers step one
    /// chunk per step, so the band must cover margin + the chunks stepped during one handoff
    /// window with slack for a rate-limited retrigger.
    /// </summary>
    public int BandChunks { get; set; } = 3;

    /// <summary>Hysteresis margin in chunks (Cluster:HandoffMarginChunks).</summary>
    public int MarginChunks { get; set; } = 1;

    /// <summary>Per-entity transfer rate limit (Cluster:HandoffMinIntervalMs).</summary>
    public int HandoffMinIntervalMs { get; set; } = 2000;

    /// <summary>CommitLog:FsyncPolicy for every node: "interval" or "commit". Interval is the
    /// capacity-run default; per-commit measures the disk's fsync ceiling instead.</summary>
    public string Fsync { get; set; } = "interval";

    /// <summary>CommitLog:FsyncIntervalMs when <see cref="Fsync"/> is "interval".</summary>
    public int FsyncIntervalMs { get; set; } = 50;

    /// <summary>The hub's HTTP port (gateway + stats). 0 binds an ephemeral port and prints it.</summary>
    public int Port { get; set; }

    /// <summary>The address the hub binds. Use 0.0.0.0 to accept a remote driver.</summary>
    public string Listen { get; set; } = "127.0.0.1";

    /// <summary>Data root for logs and hot stores. Defaults to a fresh temp directory, deleted on exit.</summary>
    public string? DataPath { get; set; }

    /// <summary>serve only: exit after this many seconds. 0 means run until Ctrl+C.</summary>
    public int ServeSeconds { get; set; }

    // ---- drive: workload ----

    /// <summary>The gateway address to drive, e.g. ws://127.0.0.1:5000/gateway.</summary>
    public Uri? Address { get; set; }

    /// <summary>Concurrent simulated players, each a real websocket client.</summary>
    public int Players { get; set; } = 200;

    /// <summary>Movement reducer calls per player per second.</summary>
    public double TickHz { get; set; } = 15;

    /// <summary>A walker steps one chunk every this many ticks; other ticks re-commit in place.
    /// 30 at 15 Hz is a chunk step every 2 s — roughly the reference workload's sprint.</summary>
    public int ChunkEveryTicks { get; set; } = 30;

    /// <summary>Fraction of players that are seam walkers, oscillating across a shard boundary so
    /// handoff and border traffic is continuously exercised.</summary>
    public double SeamFraction { get; set; } = 0.25;

    /// <summary>The measured window, in seconds (excludes warm-up).</summary>
    public int DurationSeconds { get; set; } = 120;

    /// <summary>Warm-up seconds excluded from every reported statistic.</summary>
    public int WarmupSeconds { get; set; } = 10;

    /// <summary>Optional time-series output file; .json writes JSON, anything else CSV.</summary>
    public string? OutPath { get; set; }

    /// <summary>Poll the serve side's /loadtest/stats endpoint (derived from the address).</summary>
    public bool PollServerStats { get; set; } = true;

    /// <summary>Seconds between periodic progress/stats samples.</summary>
    public int SampleSeconds { get; set; } = 10;

    // ---- all ----

    /// <summary>all only: host the serve side on a background task in this process instead of a
    /// separate one. For the smoke test and debugging; contaminates measurement, and says so.</summary>
    public bool InProcessServer { get; set; }

    public string World => $"{WorldBlocksX}x{WorldBlocksY} blocks of {BlockChunksX}x{BlockChunksY} chunks";

    public int WorldChunksX => WorldBlocksX * BlockChunksX;

    public int WorldChunksY => WorldBlocksY * BlockChunksY;

    /// <summary>The small-and-fast preset: a two-node world, a handful of players, under 30 s.</summary>
    public void ApplySmoke()
    {
        Nodes = 2;
        WorldBlocksX = 2;
        WorldBlocksY = 1;
        BlockChunksX = 4;
        BlockChunksY = 4;
        Players = 8;
        TickHz = 10;
        ChunkEveryTicks = 4;
        SeamFraction = 1.0;
        DurationSeconds = 12;
        WarmupSeconds = 3;
        HandoffMinIntervalMs = 500;
        SampleSeconds = 5;
    }

    /// <summary>The leak-watching preset: long window, so the memory series shows a trend.</summary>
    public void ApplySoak()
    {
        DurationSeconds = 300;
        WarmupSeconds = 15;
    }

    /// <summary>Parses argv after the subcommand. Returns null (having printed why) on bad input.</summary>
    public static LoadTestOptions? Parse(string[] args, int from, TextWriter error)
    {
        var options = new LoadTestOptions();
        for (var i = from; i < args.Length; i++)
        {
            var flag = args[i];
            switch (flag)
            {
                case "--smoke":
                    options.ApplySmoke();
                    continue;
                case "--soak":
                    options.ApplySoak();
                    continue;
                case "--no-stats":
                    options.PollServerStats = false;
                    continue;
                case "--in-process-server":
                    options.InProcessServer = true;
                    continue;
            }

            if (i + 1 >= args.Length)
            {
                error.WriteLine($"Flag {flag} needs a value.");
                return null;
            }

            var value = args[++i];
            try
            {
                switch (flag)
                {
                    case "--nodes": options.Nodes = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--world-blocks": (options.WorldBlocksX, options.WorldBlocksY) = ParsePair(value); break;
                    case "--block-chunks": (options.BlockChunksX, options.BlockChunksY) = ParsePair(value); break;
                    case "--band": options.BandChunks = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--margin": options.MarginChunks = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--handoff-min-ms": options.HandoffMinIntervalMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--fsync":
                        if (value is not ("interval" or "commit"))
                        {
                            error.WriteLine($"--fsync takes 'interval' or 'commit', not '{value}'.");
                            return null;
                        }

                        options.Fsync = value;
                        break;
                    case "--fsync-interval-ms": options.FsyncIntervalMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--port": options.Port = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--listen": options.Listen = value; break;
                    case "--data": options.DataPath = value; break;
                    case "--serve-seconds": options.ServeSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--address": options.Address = new Uri(value); break;
                    case "--players": options.Players = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--tick-hz": options.TickHz = double.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--chunk-every": options.ChunkEveryTicks = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--seam-fraction": options.SeamFraction = double.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--duration-seconds": options.DurationSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--warmup-seconds": options.WarmupSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--sample-seconds": options.SampleSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "--out": options.OutPath = value; break;
                    default:
                        error.WriteLine($"Unknown flag {flag}. See docs/LOAD-TESTING.md.");
                        return null;
                }
            }
            catch (FormatException)
            {
                error.WriteLine($"Flag {flag} could not parse '{value}'.");
                return null;
            }
        }

        return options;

        static (int, int) ParsePair(string value)
        {
            var parts = value.Split('x', 2);
            return (int.Parse(parts[0], CultureInfo.InvariantCulture), int.Parse(parts[1], CultureInfo.InvariantCulture));
        }
    }
}
