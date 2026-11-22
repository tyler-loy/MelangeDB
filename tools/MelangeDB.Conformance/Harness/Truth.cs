using System.Text.Json.Nodes;

namespace MelangeDB.Conformance.Harness;

/// <summary>What a client must hold, in the corpus's canonical form, computed from what was written.</summary>
public static class Truth
{
    /// <summary>The <see cref="SampleRows"/> row <paramref name="variant"/> as a client must render it.</summary>
    public static JsonObject Sample(long id, int variant, int group) => Sample(SampleRows.Build(id, variant, group));

    public static JsonObject Sample(Sample row) => new()
    {
        ["Id"] = CorpusJson.I64(row.Id),
        ["Group"] = row.Group,
        ["Flag"] = row.Flag,
        ["I8"] = row.I8,
        ["U8"] = row.U8,
        ["I16"] = row.I16,
        ["U16"] = row.U16,
        ["I32"] = row.I32,
        ["U32"] = row.U32,
        ["I64"] = CorpusJson.I64(row.I64),
        ["U64"] = CorpusJson.U64(row.U64),
        ["F32"] = CorpusJson.F32(row.F32),
        ["F64"] = CorpusJson.F64(row.F64),
        ["Text"] = row.Text,
        ["Blob"] = row.Blob is null ? null : CorpusJson.Hex(row.Blob),
        ["Who"] = CorpusJson.Hex(row.Who.ToByteArray()),
        ["At"] = CorpusJson.I64(row.At.UnixTimeMicroseconds),
        ["Mood"] = (byte)row.Mood,
    };
}

/// <summary>Builders for tagged values, the protocol's form for arguments and parameters.</summary>
public static class Tagged
{
    public static JsonObject Null() => new() { ["tag"] = "Null" };

    public static JsonObject Int64(long value) => new() { ["tag"] = "Int64", ["value"] = CorpusJson.I64(value) };

    public static JsonObject String(string value) => new() { ["tag"] = "String", ["value"] = value };

    public static JsonObject Array(params JsonObject[] items) => new() { ["tag"] = "Array", ["items"] = new JsonArray([.. items]) };
}
