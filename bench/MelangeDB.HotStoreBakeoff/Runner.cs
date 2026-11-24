using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Storage.Faster;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.HotStoreBakeoff;

/// <summary>One measured number.</summary>
internal sealed record Measure(string Name, double Value, string Unit);

/// <summary>One engine's run: its numbers, and whether every row read back as written.</summary>
internal sealed record EngineResult(string Engine, List<Measure> Measures, string? CorrectnessFailure);

internal sealed record Settings(int Rows, long BudgetBytes, int HotSet, int Ops, int ColdReads, int BlobRows, int BlobBytes, string Root, bool Compaction, bool IndexGrowth, bool NativeDirectory);

/// <summary>
/// Drives each engine through the same <see cref="IHotStore"/> workloads, each engine in its own
/// child process so memory and caches do not bleed between them. Run with no <c>--engine</c> to
/// orchestrate every engine and print a comparison; <c>--engine name</c> runs one and prints its
/// result as a JSON line.
/// </summary>
internal static class Runner
{
    private static readonly Identity Caller = Identity.Hash("bakeoff");

    public static async Task<int> RunAsync(string[] args)
    {
        var flags = Parse(args);
        var settings = new Settings(
            Rows: Int(flags, "rows", 1_000_000),
            BudgetBytes: Int(flags, "budget-mb", 64) * (1L << 20),
            HotSet: Int(flags, "hot-set", 10_000),
            Ops: Int(flags, "ops", 200_000),
            ColdReads: Int(flags, "cold-reads", 50_000),
            BlobRows: Int(flags, "blob-rows", 50_000),
            BlobBytes: Int(flags, "blob-bytes", 4096),
            Root: flags.GetValueOrDefault("root") ?? Path.Combine(Path.GetTempPath(), "melange-bakeoff"),
            Compaction: flags.GetValueOrDefault("compaction") != "off",
            IndexGrowth: flags.GetValueOrDefault("index-growth") == "on",
            NativeDirectory: flags.GetValueOrDefault("native-directory") == "on");

        if (flags.TryGetValue("engine", out var single))
        {
            var result = RunEngine(single, settings);
            Console.WriteLine("RESULT " + JsonSerializer.Serialize(result));
            return result.CorrectnessFailure is null ? 0 : 1;
        }

        Console.WriteLine(
            $"Hot-store bake-off: {settings.Rows:N0} entities, {settings.BlobRows:N0} × {settings.BlobBytes:N0} B terrain blobs, " +
            $"{settings.BudgetBytes >> 20} MiB budget, hot set {settings.HotSet:N0}, {settings.Ops:N0} ops per phase. " +
            $"{Environment.ProcessorCount} cores, {Environment.OSVersion}.");
        var engines = flags.TryGetValue("engines", out var list) ? list.Split(',') : [.. Engines.Names];
        var results = new List<EngineResult>();
        foreach (var engine in engines)
        {
            Console.WriteLine($"-- {engine}");
            var start = new ProcessStartInfo(Environment.ProcessPath!)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            foreach (var arg in args)
                start.ArgumentList.Add(arg);
            start.ArgumentList.Add("--engine");
            start.ArgumentList.Add(engine);
            using var child = Process.Start(start)!;
            var errors = child.StandardError.ReadToEndAsync();
            EngineResult? result = null;
            while (await child.StandardOutput.ReadLineAsync() is { } line)
            {
                if (line.StartsWith("RESULT ", StringComparison.Ordinal))
                    result = JsonSerializer.Deserialize<EngineResult>(line["RESULT ".Length..]);
                else
                    Console.WriteLine($"   {line}");
            }

            await child.WaitForExitAsync();
            if (result is null)
            {
                Console.WriteLine($"   FAILED (exit {child.ExitCode}): {(await errors).Trim()}");
                results.Add(new EngineResult(engine, [], $"crashed: exit {child.ExitCode}"));
                continue;
            }

            results.Add(result);
        }

        PrintTable(results);
        return results.All(static r => r.CorrectnessFailure is null) ? 0 : 1;
    }

    private static EngineResult RunEngine(string engine, Settings s)
    {
        var schema = BakeoffSchema.Load();
        var entity = schema.Get(typeof(Entity));
        var terrain = schema.Get(typeof(Terrain));
        var root = Path.Combine(s.Root, $"{engine}-{Environment.ProcessId}");
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(root, "hot"), MemoryBudgetBytes = s.BudgetBytes, Compaction = { Enabled = s.Compaction }, NativeDirectory = s.NativeDirectory },
            Residency = { Default = Residency.Paged },
        };
        var measures = new List<Measure>();
        if (s.IndexGrowth)
            IndexGrowth(engine, root, s, measures);
        var random = new Random(42);
        var keys = new RowKey[s.Rows];
        for (var i = 0; i < s.Rows; i++)
            keys[i] = SchemaKeyCodec.Encode(entity.PrimaryKey, (ulong)i);
        var versions = new int[s.Rows];
        ulong lsn = 1;

        var store = Engines.Provider(engine).Create(new HotStoreContext
        {
            Schema = schema,
            Options = options,
            Residency = ResidencyResolver.Resolve(schema, options.Residency),
            LoggerFactory = NullLoggerFactory.Instance,
        });
        try
        {
            // Rebuild: what every start does, a snapshot streamed in through recovery's bulk mode.
            var clock = Stopwatch.StartNew();
            (store as IBulkRecovery)?.BeginRecovery();
            store.LoadSnapshot(lsn++, Enumerable.Range(0, s.Rows).Select(i => new SnapshotRow(entity.Id, keys[i], EntityRow(entity, i, 0))));
            (store as IBulkRecovery)?.CompleteRecovery();
            measures.Add(new("rebuild", clock.Elapsed.TotalSeconds, "s"));
            Footprint(measures, store, root, "after rebuild");

            void Update(int i)
            {
                versions[i]++;
                Apply(store, lsn++, new RowOp(RowOpKind.Update, entity.Id, keys[i], EntityRow(entity, i, versions[i])));
            }

            Phase(measures, "hot updates", s.Ops, _ => Update(random.Next(s.HotSet)));
            Phase(measures, "uniform updates", s.Ops, _ => Update(random.Next(s.Rows)));
            Phase(measures, "hot reads", s.Ops, n => store.TryGetRow(entity.Id, keys[random.Next(s.HotSet)], out _));
            Phase(measures, "cold reads", s.ColdReads, n => store.TryGetRow(entity.Id, keys[random.Next(s.Rows)], out _));
            Phase(measures, "mixed 90/10 update/read", s.Ops, n =>
            {
                if (random.Next(10) == 0)
                    store.TryGetRow(entity.Id, keys[random.Next(s.Rows)], out _);
                else
                    Update(random.Next(s.HotSet));
            });

            clock.Restart();
            var scanned = store.Scan(entity.Id).LongCount();
            measures.Add(new("full scan", scanned / clock.Elapsed.TotalSeconds, "rows/s"));

            var chunks = s.Rows / 64;
            var windowRows = 0L;
            clock.Restart();
            for (var w = 0; w < 2_000; w++)
            {
                var low = random.Next(Math.Max(1, chunks - 4));
                windowRows += store.ScanIndexRange(
                    entity.Id,
                    nameof(Entity.ChunkId),
                    SchemaKeyCodec.Encode(entity.Columns.First(c => c.Name == nameof(Entity.ChunkId)), low),
                    SchemaKeyCodec.Encode(entity.Columns.First(c => c.Name == nameof(Entity.ChunkId)), low + 3)).LongCount();
            }

            measures.Add(new("index window scans (~256 rows)", 2_000 / clock.Elapsed.TotalSeconds, "scans/s"));
            measures.Add(new("index window rows", windowRows / clock.Elapsed.TotalSeconds, "rows/s"));

            var deletes = s.Rows / 10;
            Phase(measures, "deletes", deletes, d =>
            {
                var i = s.Rows - 1 - d * 10;
                versions[i] = -1;
                Apply(store, lsn++, new RowOp(RowOpKind.Delete, entity.Id, keys[i], default));
            });

            // Terrain: a 4 KB bytes column per row, stored out of line in the blob space.
            var blobKeys = new RowKey[s.BlobRows];
            var blobVersions = new int[s.BlobRows];
            for (var i = 0; i < s.BlobRows; i++)
                blobKeys[i] = SchemaKeyCodec.Encode(terrain.PrimaryKey, (ulong)i);
            Phase(measures, "blob inserts", s.BlobRows, i =>
                Apply(store, lsn++, new RowOp(RowOpKind.Insert, terrain.Id, blobKeys[i], TerrainRow(terrain, i, 0, s.BlobBytes))));
            Phase(measures, "blob reads", Math.Min(s.Ops, 20_000), n => store.TryGetRow(terrain.Id, blobKeys[random.Next(s.BlobRows)], out _));
            Phase(measures, "blob rewrites (1-8 KB)", Math.Min(s.Ops, 20_000), _ =>
            {
                var i = random.Next(s.BlobRows);
                blobVersions[i]++;
                Apply(store, lsn++, new RowOp(RowOpKind.Update, terrain.Id, blobKeys[i], TerrainRow(terrain, i, blobVersions[i], s.BlobBytes)));
            });
            clock.Restart();
            var blobScanned = store.Scan(terrain.Id).LongCount();
            measures.Add(new("blob full scan", blobScanned / clock.Elapsed.TotalSeconds, "rows/s"));
            Footprint(measures, store, root, "at end");

            // Churn: the entity table rewritten three times over, uniformly, so nearly every update
            // supersedes a record that has left memory. An append-only log grows by a row per update
            // until something reclaims it (#287); log against live bytes after this is that number.
            Phase(measures, "churn updates (3x table)", s.Rows * 3, _ => Update(random.Next(s.Rows)));
            Footprint(measures, store, root, "after churn");

            var failure = Verify(store, entity, terrain, keys, versions, blobKeys, blobVersions, s, random);

            return new EngineResult(engine, measures, failure);
        }
        catch (Exception exception)
        {
            return new EngineResult(engine, measures, $"threw: {exception.GetType().Name}: {exception.Message}");
        }
        finally
        {
            (store as IDisposable)?.Dispose();
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    /// <summary>
    /// <c>--index-growth on</c>. The engine's own key-value space, before any store exists: four
    /// million small keys into a space sized from the budget, as the store sizes it, so its hash index
    /// grows several times. The max is what one growth costs a single write; for an index that
    /// rehashes at once, every key it holds. Off by default: it leaves the container's page cache and
    /// memory in a different state for the phases after it (cold reads measured 100k/s lower).
    /// </summary>
    private static void IndexGrowth(string engine, string root, Settings s, List<Measure> measures)
    {
        var directory = Path.Combine(root, "growth");
        var keys = 4_000_000;
        using (var space = Engines.Factory(engine).Open(directory, PagedKvSpace.Main, s.BudgetBytes, Math.Max(s.BudgetBytes / 1024, 1L << 16)))
        {
            var key = new byte[8];
            var value = new byte[16];
            Phase(measures, "index growth inserts (4M keys)", keys, n =>
            {
                System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(key, n);
                space.Upsert(key, value);
            });
        }

        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>Every count, plus a sample of rows read back byte for byte against what was written last.</summary>
    private static string? Verify(
        IHotStore store, TableSchema entity, TableSchema terrain, RowKey[] keys, int[] versions,
        RowKey[] blobKeys, int[] blobVersions, Settings s, Random random)
    {
        var live = versions.Count(static v => v >= 0);
        if (store.Count(entity.Id) != live)
            return $"entity count {store.Count(entity.Id)}, expected {live}";
        if (store.Scan(entity.Id).LongCount() != live)
            return "entity scan count disagrees with Count";
        for (var n = 0; n < 20_000; n++)
        {
            var i = random.Next(s.Rows);
            var found = store.TryGetRow(entity.Id, keys[i], out var row);
            if (versions[i] < 0)
            {
                if (found)
                    return $"entity {i} was deleted but reads back";
                continue;
            }

            if (!found || !row.Span.SequenceEqual(EntityRow(entity, i, versions[i])))
                return $"entity {i} version {versions[i]} reads back {(found ? "different bytes" : "missing")}";
        }

        for (var n = 0; n < 5_000; n++)
        {
            var i = random.Next(s.BlobRows);
            if (!store.TryGetRow(terrain.Id, blobKeys[i], out var row)
                || !row.Span.SequenceEqual(TerrainRow(terrain, i, blobVersions[i], s.BlobBytes)))
            {
                return $"terrain {i} version {blobVersions[i]} reads back wrong";
            }
        }

        return null;
    }

    private static byte[] EntityRow(TableSchema table, int id, int version) =>
        RowSerializer.Serialize(table, new Entity
        {
            Id = (ulong)id,
            ChunkId = id / 64,
            X = id + version * 0.25f,
            Y = version,
            Z = -id,
            Health = 100 - (version % 100),
            Name = $"entity-{id:D8}",
        });

    /// <summary>Version 0 is the configured size; rewrites vary between 1 and 8 KB, growing and shrinking.</summary>
    private static byte[] TerrainRow(TableSchema table, int id, int version, int baseBytes)
    {
        var size = version == 0 ? baseBytes : 1024 + (int)((uint)(id * 31 + version * 977) % 7168);
        var data = new byte[size];
        Array.Fill(data, (byte)(id + version));
        return RowSerializer.Serialize(table, new Terrain { Id = (ulong)id, Data = data });
    }

    private static void Apply(IHotStore store, ulong lsn, RowOp op) =>
        store.Apply(new CommitRecord
        {
            Lsn = lsn,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Caller,
            ReducerName = "bakeoff",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [op],
            SerializedLength = 0,
        });

    /// <summary>Runs <paramref name="count"/> operations, timing each: throughput, and p50/p99/max latency.</summary>
    private static void Phase(List<Measure> measures, string name, int count, Action<int> op)
    {
        var ticks = new long[count];
        var total = Stopwatch.StartNew();
        for (var i = 0; i < count; i++)
        {
            var start = Stopwatch.GetTimestamp();
            op(i);
            ticks[i] = Stopwatch.GetTimestamp() - start;
        }

        total.Stop();
        Array.Sort(ticks);
        double Micros(long t) => t * 1_000_000.0 / Stopwatch.Frequency;
        measures.Add(new(name, count / total.Elapsed.TotalSeconds, "ops/s"));
        measures.Add(new($"{name} p50", Micros(ticks[count / 2]), "us"));
        measures.Add(new($"{name} p99", Micros(ticks[(int)(count * 0.99)]), "us"));
        measures.Add(new($"{name} max", Micros(ticks[^1]), "us"));
    }

    private static void Footprint(List<Measure> measures, IHotStore store, string root, string when)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        using var process = Process.GetCurrentProcess();
        measures.Add(new($"managed heap {when}", GC.GetTotalMemory(false) / 1048576.0, "MiB"));
        measures.Add(new($"working set {when}", process.WorkingSet64 / 1048576.0, "MiB"));
        if (store.Statistics().NativeDirectoryBytes is > 0 and var native)
            measures.Add(new($"native directory {when}", native / 1048576.0, "MiB"));
        var disk = Directory.Exists(root)
            ? new DirectoryInfo(root).EnumerateFiles("*", SearchOption.AllDirectories).Sum(static f => f.Length)
            : 0;
        measures.Add(new($"disk {when}", disk / 1048576.0, "MiB"));
        foreach (var log in store.Statistics().Logs)
        {
            if (log.LogBytes >= 0)
                measures.Add(new($"{log.Space} log {when}", log.LogBytes / 1048576.0, "MiB"));
            if (log.LiveBytes >= 0)
                measures.Add(new($"{log.Space} live {when}", log.LiveBytes / 1048576.0, "MiB"));
            if (log.CompactionSteps > 0)
            {
                measures.Add(new($"{log.Space} compaction steps {when}", log.CompactionSteps, "steps"));
                measures.Add(new($"{log.Space} compaction lock per step {when}", log.CompactionLockTime.TotalMicroseconds / log.CompactionSteps, "us"));
            }

            if (log.InPlaceWrites >= 0 && log.InPlaceWrites + log.AppendedWrites > 0)
                measures.Add(new($"{log.Space} in-place writes {when}", 100.0 * log.InPlaceWrites / (log.InPlaceWrites + log.AppendedWrites), "%"));
        }
    }

    private static void PrintTable(List<EngineResult> results)
    {
        Console.WriteLine();
        Console.WriteLine("| Measure | " + string.Join(" | ", results.Select(static r => r.Engine)) + " |");
        Console.WriteLine("| --- |" + string.Concat(Enumerable.Repeat(" ---: |", results.Count)));
        var names = results.SelectMany(static r => r.Measures).Select(static m => (m.Name, m.Unit)).Distinct();
        foreach (var (name, unit) in names)
        {
            var cells = results.Select(r => r.Measures.FirstOrDefault(m => m.Name == name) is { } m ? Format(m.Value) : "—");
            Console.WriteLine($"| {name} ({unit}) | {string.Join(" | ", cells)} |");
        }

        Console.WriteLine("| correct | " + string.Join(" | ", results.Select(static r => r.CorrectnessFailure is null ? "yes" : "NO: " + r.CorrectnessFailure)) + " |");
    }

    private static string Format(double value) => value switch
    {
        >= 100 => value.ToString("N0", CultureInfo.InvariantCulture),
        >= 10 => value.ToString("N1", CultureInfo.InvariantCulture),
        _ => value.ToString("N2", CultureInfo.InvariantCulture),
    };

    private static Dictionary<string, string> Parse(string[] args)
    {
        var flags = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Expected --flag value pairs, got '{args[i]}'.");
            flags[args[i][2..]] = args[i + 1];
        }

        return flags;
    }

    private static int Int(Dictionary<string, string> flags, string name, int fallback) =>
        flags.TryGetValue(name, out var value) ? int.Parse(value, CultureInfo.InvariantCulture) : fallback;
}
