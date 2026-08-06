using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;

namespace MelangeDB.Benchmarks;

/// <summary>How wide the row a wire benchmark case carries is.</summary>
public enum RowShape
{
    /// <summary>A position update: the shape a game tick actually sends, over and over.</summary>
    Narrow,

    /// <summary>A full entity record — the shape an initial subscription set carries.</summary>
    Wide,
}

/// <summary>
/// Measurement gap 5, and the gate finding #15 passed: what does sending a row as a named column
/// map cost, against sending the schema-ordered v1 bytes the store already holds?
/// <para>
/// #15 was a protocol break, worth doing only if the answer here was large, because the cost was
/// not the encoder: it was the client, the bindings generator, and the cache all moving together.
/// So this suite measures the three things that decision needed. <b>Bytes</b>, printed at setup,
/// because bandwidth at 15 Hz × hundreds of players was the headline claim. <b>Encode</b>, which is
/// server CPU on the fan-out path. <b>Decode</b>, which is client CPU on every frame. The bytes
/// answer was 1.18–1.40x and the CPU answer was 4.6–12.4x, so the headline was wrong and the
/// decision was right; protocol v2 sends the bytes.
/// </para>
/// <para>
/// The map path stays here now that nothing in the server runs it, because a benchmark that
/// deletes its baseline can no longer say what was gained. Both halves are hand-rolled against
/// <c>MsgPackWriter</c> for that reason.
/// </para>
/// <para>
/// The map path is measured exactly as the server runs it — <c>RowWire.ToColumns</c> then a
/// MessagePack map — rather than against a hand-built dictionary, because the decode from row bytes
/// is part of what the compact form deletes and leaving it out would flatter the result.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class WireFormatBenchmarks
{
    private SchemaRegistry _schema = null!;
    private TableSchema _table = null!;
    private byte[] _row = [];
    private byte[] _mapFrame = [];
    private byte[] _bytesFrame = [];
    private string[] _columnNames = [];

    [Params(RowShape.Narrow, RowShape.Wide)]
    public RowShape Shape { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _schema = Shape == RowShape.Wide
            ? BenchSchema.For(nameof(WideRow))
            : BenchSchema.For(nameof(NarrowRow));
        _table = _schema.Tables[0];
        _columnNames = [.. _table.Columns.Select(c => c.Name)];

        _row = Shape == RowShape.Wide
            ? RowSerializer.Serialize(_table, new WideRow
            {
                Id = 42,
                Name = "player-with-a-realistic-name",
                X = 1.5f,
                Y = 2.5f,
                Z = 3.5f,
                Yaw = 0.75f,
                Health = 100,
                Mana = 55,
                Level = 12,
                Experience = 123_456,
                GuildId = 7,
                LastSeen = new Timestamp(1_700_000_000_000_000),
            })
            : RowSerializer.Serialize(_table, new NarrowRow { Id = 42, X = 1.5f, Y = 2.5f, Z = 3.5f });

        _mapFrame = EncodeAsMap();
        _bytesFrame = EncodeAsBytes();

        // The headline number this suite exists for. BenchmarkDotNet has no size column, and a
        // ratio of two byte counts is the one figure from here that travels between machines.
        Console.WriteLine(
            $"[wire size] {Shape}: map {_mapFrame.Length} B, bytes {_bytesFrame.Length} B, " +
            $"ratio {(double)_mapFrame.Length / _bytesFrame.Length:F2}x");
    }

    /// <summary>Today's server path: walk the row into a name→value map, then write the map.</summary>
    [Benchmark(Description = "encode: column map", Baseline = true), BenchmarkCategory("encode")]
    public int EncodeMap() => EncodeAsMap().Length;

    /// <summary>The compact path: the row bytes go on the wire as they already sit in the store.</summary>
    [Benchmark(Description = "encode: v1 bytes"), BenchmarkCategory("encode")]
    public int EncodeBytes() => EncodeAsBytes().Length;

    /// <summary>Today's client path: read the map, then coerce each value by name.</summary>
    [Benchmark(Description = "decode: column map"), BenchmarkCategory("decode")]
    public int DecodeMap()
    {
        var reader = new MsgPackReader(_mapFrame);
        var count = reader.ReadMapHeader();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            _ = reader.ReadString();
            total += SkipValue(ref reader);
        }

        return total;
    }

    /// <summary>The compact path: one binary read, then a positional walk into the row struct.</summary>
    [Benchmark(Description = "decode: v1 bytes"), BenchmarkCategory("decode")]
    public int DecodeBytes()
    {
        var reader = new MsgPackReader(_bytesFrame);
        var payload = reader.ReadBinary();
        var row = new RowReader(payload);
        var total = 0;
        foreach (var column in _table.Columns)
            total += ReadColumn(ref row, column.Kind);
        return total;
    }

    private byte[] EncodeAsMap()
    {
        var columns = RowWire.ToColumns(_table, _row, projection: null);
        var writer = new MsgPackWriter(256);
        writer.WriteMapHeader(columns.Count);
        foreach (var name in _columnNames)
        {
            writer.WriteString(name);
            WriteValue(ref writer, columns[name]);
        }

        return writer.ToArray();
    }

    private byte[] EncodeAsBytes()
    {
        var writer = new MsgPackWriter(256);
        writer.WriteBinary(_row);
        return writer.ToArray();
    }

    private static void WriteValue(ref MsgPackWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNil(); break;
            case bool b: writer.WriteBool(b); break;
            case float f: writer.WriteFloat32(f); break;
            case double d: writer.WriteFloat64(d); break;
            case string s: writer.WriteString(s); break;
            case byte[] bytes: writer.WriteBinary(bytes); break;
            case Timestamp t: writer.WriteInt64(t.UnixTimeMicroseconds); break;
            case ulong u: writer.WriteUInt64(u); break;
            default: writer.WriteInt64(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture)); break;
        }
    }

    /// <summary>Reads past one value, returning a byte the caller sums so nothing is optimized away.</summary>
    private static int SkipValue(ref MsgPackReader reader)
    {
        if (reader.TryReadNil())
            return 0;
        var code = reader.PeekCode();
        if (code is 0xc2 or 0xc3)
            return reader.ReadBool() ? 1 : 0;
        if (code is 0xc4 or 0xc5 or 0xc6)
            return reader.ReadBinary().Length;
        if (code is 0xd9 or 0xda or 0xdb || (code & 0xe0) == 0xa0)
            return reader.ReadString()?.Length ?? 0;
        if (code is 0xca or 0xcb)
            return (int)reader.ReadFloat64();
        return (int)reader.ReadInt64();
    }

    private static int ReadColumn(ref RowReader reader, ColumnKind kind) => kind switch
    {
        ColumnKind.Bool => reader.ReadBool() ? 1 : 0,
        ColumnKind.Int32 => reader.ReadInt32(),
        ColumnKind.UInt32 => (int)reader.ReadUInt32(),
        ColumnKind.Int64 => (int)reader.ReadInt64(),
        ColumnKind.UInt64 => (int)reader.ReadUInt64(),
        ColumnKind.Float32 => (int)reader.ReadFloat32(),
        ColumnKind.Float64 => (int)reader.ReadFloat64(),
        ColumnKind.String => reader.ReadString()?.Length ?? 0,
        ColumnKind.Timestamp => (int)reader.ReadTimestamp().UnixTimeMicroseconds,
        _ => throw new NotSupportedException($"The wire benchmark does not cover {kind}."),
    };
}
