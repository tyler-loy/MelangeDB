namespace MelangeDB.CodeGen;

/// <summary>One generated source file: its name and its exact text.</summary>
internal readonly record struct GeneratedFile(string Name, string Content);

/// <summary>
/// The client bindings a manifest produces, as files. The one list both front doors use: the
/// Roslyn generator adds each file as a source, and <c>melange generate --lang cs</c> writes each
/// to disk. A byte-identity test holds the two together; sharing this list is what makes that test
/// a guard against wiring drift rather than against two emitters.
/// </summary>
internal static class ClientBindings
{
    /// <summary>The C# bindings: one file per table, then the model (schema, connection, reducers, producers).</summary>
    public static IReadOnlyList<GeneratedFile> CSharp(ClientSchemaModel schema)
    {
        var files = new List<GeneratedFile>(schema.Tables.Items.Length + 1);
        foreach (var table in schema.Tables.Items)
            files.Add(new GeneratedFile($"{table.TypeName}.Client.g.cs", ClientEmitter.EmitTable(table)));
        files.Add(new GeneratedFile("MelangeClientModel.g.cs", ClientEmitter.EmitModel(schema)));
        return files;
    }

    /// <summary>The TypeScript bindings for <c>@melangedb/client</c>: one module.</summary>
    public static IReadOnlyList<GeneratedFile> TypeScript(ClientSchemaModel schema) =>
        [new GeneratedFile(TypeScriptEmitter.FileName, TypeScriptEmitter.Emit(schema))];
}
