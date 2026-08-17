using MelangeDB.Cli;
using MelangeDB.Core;

// The `melange` umbrella command. Five verbs:
//
//   melange schema path/to/Module.dll [-o melange-schema.json]
//   melange schema http://localhost:5310 [-o melange-schema.json]
//   melange backup ./data/log [-o world.mbak]
//   melange backup https://game.example.com --token <jwt> [-o world.mbak]
//   melange backup verify world.mbak
//   melange restore world.mbak -o ./data/log [--at-lsn 41200]
//   melange clone world.mbak -o ./staging/log
//
// `schema` writes a module's client-visible schema manifest for the client binding generator;
// its URL form fetches from a running server's schema endpoint. `backup` captures a stopped
// server's data directory into a .mbak archive — or, in its URL form, streams one from a running
// server's backup endpoint (Backup:Enabled, and the token must carry the server's
// Backup:OwnerRole claim); `verify` CRC-walks and dry-replays one; `restore`
// materializes a data directory a server boots from — with a fresh epoch, always, because a
// restore is a rewind and stale resume cursors must full-resync rather than resume into history
// that no longer happened. `--at-lsn` cuts the tail at a named LSN: the moment just before the
// mistake, within the window one archive holds. `clone` materializes an explicitly *different*
// world from a production archive — staging seeded from production, with subscriber checkpoints
// dropped and a provenance sidecar recording what it is a clone of. An unverified backup is a
// hope, not a backup.
if (args.Length == 0)
{
    PrintUsage();
    return 2;
}

return args[0] switch
{
    "schema" => await RunSchemaAsync(args.AsSpan(1).ToArray()),
    "backup" when args.Length >= 2 && args[1] == "verify" => RunVerify(args.AsSpan(2).ToArray()),
    "backup" => await RunBackupAsync(args.AsSpan(1).ToArray()),
    "restore" => RunRestore(args.AsSpan(1).ToArray()),
    "clone" => RunClone(args.AsSpan(1).ToArray()),
    _ => Usage(),
};

static async Task<int> RunSchemaAsync(string[] rest)
{
    if (!ParseSourceAndOutput(rest, "melange-schema.json", out var source, out var output, out _))
        return Usage();
    try
    {
        var json = source.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || source.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
            ? await SchemaExporter.FetchAsync(new Uri(source))
            : SchemaExporter.ReadFromAssembly(source);

        // UTF-8, no BOM, bytes exactly as generated — the file is a wire artifact, not a document.
        await File.WriteAllTextAsync(output, json, new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        Console.WriteLine($"Wrote {output} ({json.Length} chars) from {source}.");
        return 0;
    }
    catch (Exception exception) when (exception is InvalidOperationException or FileNotFoundException or HttpRequestException or UriFormatException or BadImageFormatException)
    {
        Console.Error.WriteLine(exception.Message);
        return 1;
    }
}

static async Task<int> RunBackupAsync(string[] rest)
{
    if (!ParseSourceAndOutput(rest, "world.mbak", out var source, out var output, out var token))
        return Usage();
    try
    {
        if (source.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || source.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            var bytes = await BackupFetcher.FetchAsync(new Uri(source), output, token);
            Console.WriteLine($"Wrote {output} ({bytes} bytes) from {source}. Verify it: melange backup verify {output}");
            return 0;
        }

        var summary = MelangeBackup.Create(source, output);
        foreach (var engine in summary.Engines)
            Console.WriteLine($"Captured {engine.Key}: LSNs {engine.SnapshotLsn}..{engine.HeadLsn}, {engine.SnapshotRows} snapshot rows + {engine.TailRecords} log records.");
        Console.WriteLine($"Wrote {output} ({summary.TotalBytes} bytes). Verify it: melange backup verify {output}");
        return 0;
    }
    catch (Exception exception) when (IsHandled(exception) || exception is HttpRequestException or UriFormatException)
    {
        Console.Error.WriteLine(exception.Message);
        return 1;
    }
}

static int RunVerify(string[] rest)
{
    if (rest.Length != 1 || rest[0] is "-h" or "--help")
        return Usage();
    try
    {
        var report = MelangeBackup.Verify(rest[0]);
        foreach (var engine in report.Engines)
        {
            var identity = engine.Identity;
            Console.WriteLine($"{identity.Key}: LSNs {identity.SnapshotLsn}..{identity.HeadLsn}, {identity.SnapshotRows} snapshot rows + {identity.TailRecords} log records.");
            foreach (var (table, rows) in engine.RowsByTable)
                Console.WriteLine($"  table {table}: {rows} rows");
        }

        Console.WriteLine($"{rest[0]} verified: every frame intact, dry replay complete.");
        return 0;
    }
    catch (Exception exception) when (IsHandled(exception))
    {
        Console.Error.WriteLine(exception.Message);
        return 1;
    }
}

static int RunRestore(string[] rest)
{
    if (!ParseArguments(rest, null, out var archive, out var target, out _, out var atLsn))
        return Usage();
    try
    {
        var summary = MelangeBackup.Restore(archive, target, new RestoreOptions { AtLsn = atLsn });
        foreach (var engine in summary.Engines)
        {
            Console.WriteLine($"Restored {engine.Key} into {engine.Directory}: head LSN {engine.HeadLsn}, new epoch {engine.NewEpoch:D}.");
            if (engine.HeadLsn < engine.CapturedHeadLsn)
            {
                Console.WriteLine(
                    $"  Cut at LSN {engine.HeadLsn}: {engine.CapturedHeadLsn - engine.HeadLsn} record(s) up to the captured head " +
                    $"{engine.CapturedHeadLsn} are not in this world. The rewind is total — AutoInc ids allocated in that " +
                    "range are free again, so reconcile anything outside this database that recorded them.");
            }
        }

        Console.WriteLine("The fresh epoch means clients with pre-restore resume cursors will full-resync — that is the point of it.");
        return 0;
    }
    catch (Exception exception) when (IsHandled(exception))
    {
        Console.Error.WriteLine(exception.Message);
        return 1;
    }
}

static int RunClone(string[] rest)
{
    if (!ParseSourceAndOutput(rest, null, out var archive, out var target, out _))
        return Usage();
    try
    {
        var summary = MelangeBackup.Clone(archive, target);
        foreach (var engine in summary.Engines)
            Console.WriteLine($"Cloned {engine.Key} into {engine.Directory}: head LSN {engine.HeadLsn}, new epoch {engine.NewEpoch:D}.");
        Console.WriteLine(
            "This is a different world, not a copy of the same one: subscriber checkpoints were dropped, and " +
            $"{CloneProvenance.FileName} records what it is a clone of (the server says so at every boot).");
        Console.WriteLine(
            "What the archive cannot carry is yours: give it its own Postgres schema (an empty one bootstraps " +
            "itself), its own data directory, and its own fleet. Never point it at production's.");
        return 0;
    }
    catch (Exception exception) when (IsHandled(exception))
    {
        Console.Error.WriteLine(exception.Message);
        return 1;
    }
}

// The shared argument shape: one positional source, one -o/--output, and --token for the
// privileged URL form. A null default output makes -o required (restore refuses to guess where a
// world should land). --token falls back to the MELANGE_TOKEN environment variable, which keeps
// the JWT out of shell history and process listings in scripts.
static bool ParseSourceAndOutput(string[] rest, string? defaultOutput, out string source, out string output, out string? token)
    => ParseArguments(rest, defaultOutput, out source, out output, out token, out _);

static bool ParseArguments(
    string[] rest, string? defaultOutput, out string source, out string output, out string? token, out ulong? atLsn)
{
    source = "";
    output = defaultOutput ?? "";
    token = Environment.GetEnvironmentVariable("MELANGE_TOKEN");
    atLsn = null;
    var sawSource = false;
    var sawOutput = false;
    for (var i = 0; i < rest.Length; i++)
    {
        switch (rest[i])
        {
            case "-o" or "--output" when i + 1 < rest.Length:
                output = rest[++i];
                sawOutput = true;
                break;
            case "--token" when i + 1 < rest.Length:
                token = rest[++i];
                break;
            case "--at-lsn" when i + 1 < rest.Length && ulong.TryParse(rest[i + 1], out var lsn):
                atLsn = lsn;
                i++;
                break;
            case "--at-lsn":
                Console.Error.WriteLine("--at-lsn takes an LSN: a whole number naming the moment to restore to.");
                return false;
            case "-h" or "--help":
                return false;
            default:
                source = rest[i];
                sawSource = true;
                break;
        }
    }

    return sawSource && (sawOutput || defaultOutput is not null);
}

static bool IsHandled(Exception exception)
    => exception is InvalidOperationException or InvalidDataException or FileNotFoundException
        or DirectoryNotFoundException or NotSupportedException or UnauthorizedAccessException;

static int Usage()
{
    PrintUsage();
    return 2;
}

static void PrintUsage()
{
    Console.Error.WriteLine("usage: melange schema <module.dll | http(s)://host[:port][/path]> [-o melange-schema.json]");
    Console.Error.WriteLine("       melange backup <data-dir | http(s)://host[:port][/path]> [-o world.mbak] [--token <jwt>]");
    Console.Error.WriteLine("       melange backup verify <archive>");
    Console.Error.WriteLine("       melange restore <archive> -o <data-dir> [--at-lsn <n>]");
    Console.Error.WriteLine("       melange clone <archive> -o <data-dir>");
}
