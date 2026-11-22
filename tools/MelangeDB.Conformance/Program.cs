using MelangeDB.Conformance;

// melange-conformance record [-o <path>]   writes the decode corpus (default: conformance/corpus.json)
// melange-conformance schema [-o <path>]   writes the conformance module's manifest
if (args.Length == 0 || args[0] is not ("record" or "schema"))
{
    Console.Error.WriteLine("usage: MelangeDB.Conformance record|schema [-o <path>]");
    return 2;
}

var output = args.Length >= 3 && args[1] == "-o" ? args[2] : null;
if (args[0] == "record")
{
    var corpus = CorpusRecorder.Render(await CorpusRecorder.RecordAsync());
    await File.WriteAllTextAsync(output ?? Path.Combine("conformance", "corpus.json"), corpus);
}
else
{
    await File.WriteAllTextAsync(output ?? Path.Combine("conformance", "melange-schema.json"), MelangeDB.Generated.MelangeSchemaManifest.Json);
}

return 0;
