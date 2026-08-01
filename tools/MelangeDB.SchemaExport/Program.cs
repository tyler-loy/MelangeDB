using MelangeDB.SchemaExport;

// Writes a module's client-visible schema manifest to a file the client binding generator
// consumes as an AdditionalFile. Two sources, one writer:
//
//   MelangeDB.SchemaExport path/to/Module.dll [-o melange-schema.json]
//   MelangeDB.SchemaExport http://localhost:5310 [-o melange-schema.json]
//
// The URL form fetches from a running server's schema endpoint (on in Development); a bare base
// URL gets /melange/schema appended. Both forms write the generator's verbatim JSON, so the two
// paths produce byte-identical files.
var source = null as string;
var output = "melange-schema.json";
for (var i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "-o" or "--output" when i + 1 < args.Length:
            output = args[++i];
            break;
        case "-h" or "--help":
            source = null;
            i = args.Length;
            break;
        default:
            source = args[i];
            break;
    }
}

if (source is null)
{
    Console.Error.WriteLine("usage: MelangeDB.SchemaExport <module.dll | http(s)://host[:port][/path]> [-o melange-schema.json]");
    return 2;
}

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
