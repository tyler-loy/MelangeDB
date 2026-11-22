using System.Text.Json.Nodes;
using MelangeDB.Conformance;
using MelangeDB.Conformance.Harness;

// MelangeDB.Conformance record [-o <path>]   writes the decode corpus (default: conformance/corpus.json)
// MelangeDB.Conformance schema [-o <path>]   writes the conformance module's manifest
// MelangeDB.Conformance run [--corpus <path>] [--only <scenario,...>] -- <adapter command> [args...]
//                                             runs the live suite against a client's adapter
switch (args.FirstOrDefault())
{
    case "record":
        await File.WriteAllTextAsync(Output() ?? Path.Combine("conformance", "corpus.json"), CorpusRecorder.Render(await CorpusRecorder.RecordAsync()));
        return 0;
    case "schema":
        await File.WriteAllTextAsync(Output() ?? Path.Combine("conformance", "melange-schema.json"), MelangeDB.Generated.MelangeSchemaManifest.Json);
        return 0;
    case "run":
        return await RunAsync();
    default:
        Console.Error.WriteLine("usage: MelangeDB.Conformance record|schema [-o <path>]");
        Console.Error.WriteLine("       MelangeDB.Conformance run [--corpus <path>] [--only <scenario,...>] -- <adapter command> [args...]");
        return 2;
}

string? Output() => args.Length >= 3 && args[1] == "-o" ? args[2] : null;

async Task<int> RunAsync()
{
    var separator = Array.IndexOf(args, "--");
    if (separator < 0 || separator == args.Length - 1)
    {
        Console.Error.WriteLine("run: name the adapter command after `--`.");
        return 2;
    }

    var options = args[1..separator];
    string Option(string name, string fallback) =>
        Array.IndexOf(options, name) is var at and >= 0 && at + 1 < options.Length ? options[at + 1] : fallback;
    var only = Option("--only", string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal);
    var corpus = JsonNode.Parse(await File.ReadAllTextAsync(Option("--corpus", Path.Combine("conformance", "corpus.json"))))!.AsObject();

    await using var adapter = AdapterProcess.Start(args[separator + 1], args[(separator + 2)..]);
    var results = await ConformanceSuite.RunAsync(adapter, corpus, only.Count == 0 ? null : only.Contains);
    foreach (var result in results)
    {
        Console.WriteLine($"{(result.Passed ? "PASS" : "FAIL")}  {result.Name,-22} {result.Obligations,-28} {result.Elapsed.TotalMilliseconds,7:F0} ms");
        if (result.Failure is { } failure)
            Console.WriteLine("      " + failure.ReplaceLineEndings("\n      "));
    }

    var failed = results.Count(static r => !r.Passed);
    Console.WriteLine(failed == 0 ? $"All {results.Count} scenarios passed." : $"{failed} of {results.Count} scenarios failed.");
    return failed == 0 ? 0 : 1;
}
