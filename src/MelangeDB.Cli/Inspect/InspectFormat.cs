using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using MelangeDB.Core;

namespace MelangeDB.Cli;

/// <summary>
/// How <c>melange inspect</c> writes values, for people and for scripts. Text output is ASCII on
/// purpose: a string value is printed as a JSON literal, escaped, so a control character or an
/// unprintable byte in a row can never rewrite the operator's terminal.
/// </summary>
internal static class InspectFormat
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    /// <summary>The reducer name the engine writes a schema migration's marker record under.</summary>
    public const string ShapeMarkerReducer = "__melange/shape";

    public static string Value(object? value) => value switch
    {
        null => "null",
        string text => Quote(text),
        bool flag => flag ? "true" : "false",
        byte[] bytes => Hex(bytes),
        float real => Real(real),
        double real => Real(real),
        Identity identity => identity.ToString(),
        Timestamp timestamp => Time(timestamp),
        ScheduleAt schedule => schedule.ToString(),
        object?[] array => "[" + string.Join(", ", array.Select(Value)) + "]",
        IFormattable formattable => formattable.ToString(null, Invariant),
        _ => value.ToString() ?? "",
    };

    /// <summary>A string as a quoted literal, ASCII only: controls and everything past <c>~</c> as <c>\uXXXX</c>.</summary>
    private static string Real(float value) => value.ToString("R", Invariant);

    private static string Real(double value) => value.ToString("R", Invariant);

    public static string Quote(string text)
    {
        var builder = new StringBuilder(text.Length + 2).Append('"');
        foreach (var c in text)
        {
            switch (c)
            {
                case '"':
                    builder.Append("\\\"");
                    break;
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                case < ' ' or > '~':
                    builder.Append("\\u").Append(((int)c).ToString("x4", Invariant));
                    break;
                default:
                    builder.Append(c);
                    break;
            }
        }

        return builder.Append('"').ToString();
    }

    /// <summary>A name the log carries (a reducer, an event type) made safe to print: quoted only when it needs escaping.</summary>
    public static string Name(string name) =>
        name.All(static c => c is > ' ' and <= '~') ? name : Quote(name);

    /// <summary>Bytes as <c>0x</c> hex, cut after <paramref name="max"/> bytes with the full length said.</summary>
    public static string Hex(ReadOnlySpan<byte> bytes, int max = 64) =>
        bytes.Length <= max
            ? "0x" + Convert.ToHexStringLower(bytes)
            : $"0x{Convert.ToHexStringLower(bytes[..max])}... ({bytes.Length} bytes)";

    public static string Time(Timestamp timestamp)
    {
        try
        {
            return timestamp.ToDateTimeOffset().UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff'Z'", Invariant);
        }
        catch (ArgumentOutOfRangeException)
        {
            return $"{timestamp.UnixTimeMicroseconds} us";
        }
    }

    public static string Reducer(string name) =>
        name == ShapeMarkerReducer ? $"{name} (schema migration marker)" : Name(name);

    public static string Count(long count, string noun) =>
        count == 1 ? $"1 {noun}" : $"{count.ToString(Invariant)} {noun}s";

    public static string OpName(RowOpKind kind) => kind switch
    {
        RowOpKind.Insert => "insert",
        RowOpKind.Update => "update",
        RowOpKind.Delete => "delete",
        _ => $"op{(byte)kind}",
    };

    /// <summary>The table's name under the shape governing <paramref name="lsn"/>, or its id when no shape names it.</summary>
    public static string TableName(InspectSource source, ulong lsn, TableId table) =>
        source.TableAt(lsn, table)?.Name ?? table.ToString();

    /// <summary>A key as <c>column = value</c>, or as hex when it cannot be decoded.</summary>
    public static string Key(InspectSource source, ulong lsn, TableId table, in RowKey key)
    {
        var decoded = source.DecodeKey(lsn, table, key);
        return decoded.Succeeded ? $"{decoded.Column} = {Value(decoded.Value)}" : $"key {Hex(key.Span)}";
    }

    /// <summary>
    /// The source's facts, printed before any answer: investigations fail at these edges, and an
    /// empty answer is only readable next to the range it was looked for in.
    /// </summary>
    public static void WriteFacts(TextWriter output, InspectFacts facts)
    {
        var kind = facts.Kind == InspectSourceKind.Archive
            ? $"archive, engine {facts.EngineKey}" + (facts.Engines.Count > 1 ? $" of {string.Join(", ", facts.Engines)}" : "")
            : "data directory";
        output.WriteLine($"source    {facts.Path} ({kind})");
        if (facts.CapturedAt is { } captured)
            output.WriteLine($"captured  {captured.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss'Z'", Invariant)}");
        output.WriteLine($"epoch     {(facts.Epoch == Guid.Empty ? "none recorded" : facts.Epoch.ToString("D"))}");
        foreach (var ancestor in facts.Lineage)
            output.WriteLine($"lineage   descends from epoch {ancestor.Epoch:D} up to LSN {ancestor.UpToLsn}: history at or below it is inherited");
        if (facts.Provenance is { } provenance)
        {
            output.WriteLine(
                $"clone     of epoch {provenance.SourceEpoch:D} at LSN {provenance.SourceHeadLsn}, from {provenance.Archive} " +
                $"(captured {DateTimeOffset.FromUnixTimeMilliseconds(provenance.ArchiveCapturedAtUnixMs).UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss'Z'", Invariant)})");
        }

        output.WriteLine(facts.RecordCount == 0
            ? $"records   none (the log holds nothing past LSN {facts.LogFloor})"
            : $"records   LSN {facts.FirstLsn}..{facts.HeadLsn} ({Count(facts.RecordCount, "record")})");
        output.WriteLine(facts.Kind == InspectSourceKind.Archive
            ? $"floor     LSN {facts.LogFloor}: an archive holds only the log above its snapshot (the source's truncation base was {facts.BaseLsn})"
            : $"floor     LSN {facts.LogFloor} (truncation base)");
        output.WriteLine(facts.SnapshotLsn == 0
            ? $"snapshot  none{(facts.SnapshotIgnored ? " usable (melange.snapshot belongs to another epoch, so it is ignored)" : "")}"
            : $"snapshot  LSN {facts.SnapshotLsn}" + (facts.SnapshotTimestamp is { } at ? $" ({Time(at)})" : ""));
        if (facts.Kind == InspectSourceKind.DataDirectory && (facts.Term > 0 || facts.SealedSegments > 0))
            output.WriteLine($"owner     term {facts.Term}, after {Count(facts.SealedSegments, "sealed segment")} from earlier owners");
        foreach (var note in Notes(facts))
            output.WriteLine($"note      {note}");
        output.WriteLine();
    }

    public static IEnumerable<string> Notes(InspectFacts facts)
    {
        if (facts.Kind == InspectSourceKind.DataDirectory)
            yield return $"read without a lock, to LSN {facts.HeadLsn}; an engine running against this directory may be ahead of it";
        if (facts.TornTailBytes > 0)
        {
            yield return $"{Count(facts.TornTailBytes, "byte")} after the head are not a whole record: an append still being written, " +
                "or a crash's torn tail. Neither is history.";
        }

        foreach (var (after, next) in facts.Gaps)
            yield return $"the LSNs jump from {after} to {next}: records between them are not in this source";
        if (!facts.ShapeRecorded)
            yield return "no melange.shape: rows and keys show only as bytes";
    }

    /// <summary>What moved under a directory while it was read, said after the answer it qualifies.</summary>
    public static IEnumerable<string> Changes(InspectChanges changes)
    {
        if (changes.OwnerChanged)
            yield return "the shard changed owner while this read ran: a new owner sealed the live segment. This answer is of the log before it.";
        if (changes.Appended)
            yield return "the engine appended past the head while this read ran; open the directory again to see the newer records.";
        if (changes.Truncated)
            yield return "the log was truncated while this read ran; the records shown were read before it, and the oldest may be gone now.";
    }
}

/// <summary>
/// A JSON document written to a <see cref="TextWriter"/> as it grows, so a window of a million
/// records never sits in memory whole. 64-bit integers are written as strings, because a JSON
/// number past 2^53 loses digits in most parsers and AutoInc ids live up there.
/// </summary>
internal sealed class InspectJson : IDisposable
{
    private readonly TextWriter _output;
    private readonly ArrayBufferWriter<byte> _buffer = new(64 * 1024);

    public InspectJson(TextWriter output)
    {
        _output = output;
        Writer = new Utf8JsonWriter(_buffer, new JsonWriterOptions { Indented = true });
    }

    public Utf8JsonWriter Writer { get; }

    /// <summary>Moves what is written so far to the output once enough has gathered.</summary>
    public void FlushIfLarge()
    {
        if (Writer.BytesPending + _buffer.WrittenCount >= 64 * 1024)
            Flush();
    }

    public void Dispose()
    {
        Flush();
        _output.WriteLine();
        Writer.Dispose();
    }

    private void Flush()
    {
        Writer.Flush();
        _output.Write(Encoding.UTF8.GetString(_buffer.WrittenSpan));
        _buffer.ResetWrittenCount();
    }

    public void Facts(InspectFacts facts)
    {
        var w = Writer;
        w.WriteStartObject("source");
        w.WriteString("kind", facts.Kind == InspectSourceKind.Archive ? "archive" : "directory");
        w.WriteString("path", facts.Path);
        if (facts.EngineKey is not null)
        {
            w.WriteString("engine", facts.EngineKey);
            w.WriteStartArray("engines");
            foreach (var engine in facts.Engines)
                w.WriteStringValue(engine);
            w.WriteEndArray();
        }

        if (facts.CapturedAt is { } captured)
            w.WriteString("capturedAt", captured);
        w.WriteString("epoch", facts.Epoch == Guid.Empty ? null : facts.Epoch.ToString("D"));
        w.WriteStartArray("lineage");
        foreach (var ancestor in facts.Lineage)
        {
            w.WriteStartObject();
            w.WriteString("epoch", ancestor.Epoch.ToString("D"));
            w.WriteNumber("upToLsn", ancestor.UpToLsn);
            w.WriteEndObject();
        }

        w.WriteEndArray();
        if (facts.Provenance is { } provenance)
        {
            w.WriteStartObject("clone");
            w.WriteString("sourceEpoch", provenance.SourceEpoch.ToString("D"));
            w.WriteNumber("sourceHeadLsn", provenance.SourceHeadLsn);
            w.WriteString("archive", provenance.Archive);
            w.WriteString("archiveCapturedAt", DateTimeOffset.FromUnixTimeMilliseconds(provenance.ArchiveCapturedAtUnixMs));
            w.WriteString("clonedAt", DateTimeOffset.FromUnixTimeMilliseconds(provenance.ClonedAtUnixMs));
            w.WriteEndObject();
        }

        w.WriteNumber("baseLsn", facts.BaseLsn);
        w.WriteNumber("logFloor", facts.LogFloor);
        w.WriteNumber("firstLsn", facts.FirstLsn);
        w.WriteNumber("headLsn", facts.HeadLsn);
        w.WriteNumber("records", facts.RecordCount);
        w.WriteNumber("snapshotLsn", facts.SnapshotLsn);
        if (facts.SnapshotTimestamp is { } at)
            w.WriteString("snapshotTimestamp", Time(at));
        w.WriteBoolean("snapshotIgnored", facts.SnapshotIgnored);
        if (facts.Kind == InspectSourceKind.DataDirectory)
        {
            w.WriteNumber("term", facts.Term);
            w.WriteNumber("sealedSegments", facts.SealedSegments);
        }

        w.WriteNumber("tornTailBytes", facts.TornTailBytes);
        w.WriteStartArray("gaps");
        foreach (var (after, next) in facts.Gaps)
        {
            w.WriteStartObject();
            w.WriteNumber("after", after);
            w.WriteNumber("next", next);
            w.WriteEndObject();
        }

        w.WriteEndArray();
        w.WriteBoolean("shapeRecorded", facts.ShapeRecorded);
        w.WriteEndObject();
    }

    public void Changes(InspectChanges changes)
    {
        var w = Writer;
        w.WriteStartObject("changedWhileRead");
        w.WriteBoolean("ownerChanged", changes.OwnerChanged);
        w.WriteBoolean("appended", changes.Appended);
        w.WriteBoolean("truncated", changes.Truncated);
        w.WriteEndObject();
    }

    public void Notes(IEnumerable<string> notes)
    {
        Writer.WriteStartArray("notes");
        foreach (var note in notes)
            Writer.WriteStringValue(note);
        Writer.WriteEndArray();
    }

    /// <summary>A column value by its kind: integers up to 32 bits and floats as numbers, the rest as strings.</summary>
    public void ColumnValue(object? value)
    {
        var w = Writer;
        switch (value)
        {
            case null:
                w.WriteNullValue();
                break;
            case bool flag:
                w.WriteBooleanValue(flag);
                break;
            case sbyte or byte or short or ushort or int or uint:
                w.WriteNumberValue(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                break;
            case long or ulong:
                w.WriteStringValue(((IFormattable)value).ToString(null, CultureInfo.InvariantCulture));
                break;
            case float real when float.IsFinite(real):
                w.WriteNumberValue(real);
                break;
            case double real when double.IsFinite(real):
                w.WriteNumberValue(real);
                break;
            case float or double:
                w.WriteStringValue(InspectFormat.Value(value));
                break;
            case string text:
                w.WriteStringValue(text);
                break;
            case byte[] bytes:
                w.WriteStringValue(Convert.ToHexStringLower(bytes));
                break;
            case Timestamp timestamp:
                w.WriteStringValue(Time(timestamp));
                break;
            case object?[] array:
                w.WriteStartArray();
                foreach (var element in array)
                    ColumnValue(element);
                w.WriteEndArray();
                break;
            default:
                w.WriteStringValue(InspectFormat.Value(value));
                break;
        }
    }

    /// <summary>An argument as <c>{ "type", "value" }</c>: the tag is all an argument carries about its type.</summary>
    public void Argument(string? name, object? value)
    {
        var w = Writer;
        w.WriteStartObject();
        if (name is not null)
            w.WriteString("name", name);
        w.WriteString("type", value switch
        {
            null => "null",
            bool => "bool",
            long => "int64",
            ulong => "uint64",
            double => "float64",
            string => "string",
            byte[] => "bytes",
            Identity => "identity",
            Timestamp => "timestamp",
            object?[] => "array",
            _ => "unknown",
        });
        w.WritePropertyName("value");
        ColumnValue(value);
        w.WriteEndObject();
    }

    private static string Time(Timestamp timestamp)
    {
        try
        {
            return timestamp.ToDateTimeOffset().ToString("O", CultureInfo.InvariantCulture);
        }
        catch (ArgumentOutOfRangeException)
        {
            return timestamp.UnixTimeMicroseconds.ToString(CultureInfo.InvariantCulture);
        }
    }
}
