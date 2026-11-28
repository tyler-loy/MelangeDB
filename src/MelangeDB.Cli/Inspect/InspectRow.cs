using System.Globalization;
using MelangeDB.Core;

namespace MelangeDB.Cli;

/// <summary>
/// Turns <c>--key</c> text into the encoded primary key a write set carries, by the key column's
/// kind in the recorded shape: the inverse of how a key is printed, so a key copied from one verb's
/// output is accepted by the next.
/// </summary>
internal static class InspectKeys
{
    /// <summary>Resolves <c>--table</c> and <c>--key</c>/<c>--key-hex</c>; null with the reason on failure.</summary>
    public static (TableId Table, RowKey Key, InspectTable? Known)? Resolve(InspectSource source, InspectOptions options, TextWriter error)
    {
        var name = options.Table!;
        var known = FindTable(source, name);
        if (options.KeyHex is { } hex)
        {
            var digits = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? hex[2..] : hex;
            try
            {
                return (TableId.FromName(name), new RowKey(Convert.FromHexString(digits)), known);
            }
            catch (FormatException)
            {
                error.WriteLine("melange inspect: --key-hex takes the encoded key as hex digits.");
                return null;
            }
        }

        if (known is null)
        {
            error.WriteLine(source.Facts.ShapeRecorded
                ? $"melange inspect: no shape in this source names a table '{name}'. Names are case-sensitive; inspect log lists the tables a commit wrote."
                : $"melange inspect: this source records no shapes (no melange.shape), so --key cannot be encoded; pass the encoded key with --key-hex.");
            return null;
        }

        if (known.KeyColumn is not { } column)
        {
            error.WriteLine($"melange inspect: the recorded shape of '{name}' names key column '{known.Shape.Key}' but does not list it; pass --key-hex.");
            return null;
        }

        if (!TryParse(column.Kind, options.Key!, out var value, out var problem))
        {
            error.WriteLine($"melange inspect: '{options.Key}' is not a {column.Kind} key for {name}.{column.Name}: {problem}");
            return null;
        }

        return (known.Id, SchemaKeyCodec.Encode(column.Kind, value), known);
    }

    /// <summary>The newest shape that names the table: its key column cannot move or change kind, so any reign that has it agrees.</summary>
    public static InspectTable? FindTable(InspectSource source, string name)
    {
        if (source.Shapes is not { } shapes)
            return null;
        for (var i = shapes.Entries.Count - 1; i >= 0; i--)
        {
            if (source.FindTable(name, Math.Max(shapes.Entries[i].FromLsn, 1)) is { } table)
                return table;
        }

        return null;
    }

    public static bool TryParse(ColumnKind kind, string text, out object value, out string problem)
    {
        value = null!;
        problem = "";
        var integer = NumberStyles.AllowLeadingSign;
        var invariant = CultureInfo.InvariantCulture;
        switch (kind)
        {
            case ColumnKind.Bool when bool.TryParse(text, out var flag):
                value = flag;
                return true;
            case ColumnKind.Int8 when sbyte.TryParse(text, integer, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.UInt8 when byte.TryParse(text, NumberStyles.None, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.Int16 when short.TryParse(text, integer, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.UInt16 when ushort.TryParse(text, NumberStyles.None, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.Int32 when int.TryParse(text, integer, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.UInt32 when uint.TryParse(text, NumberStyles.None, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.Int64 when long.TryParse(text, integer, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.UInt64 when ulong.TryParse(text, NumberStyles.None, invariant, out var v):
                value = v;
                return true;
            case ColumnKind.String:
                value = text;
                return true;
            case ColumnKind.Bytes when TryHex(text, out var bytes):
                value = bytes;
                return true;
            case ColumnKind.Identity when TryHex(text, out var bytes) && bytes.Length == Identity.Size:
                value = new Identity(bytes);
                return true;
            case ColumnKind.Timestamp when long.TryParse(text, integer, invariant, out var micros):
                value = new Timestamp(micros);
                return true;
            case ColumnKind.Timestamp when DateTimeOffset.TryParse(text, invariant, DateTimeStyles.AssumeUniversal, out var at):
                value = Timestamp.FromDateTimeOffset(at);
                return true;
        }

        problem = kind switch
        {
            ColumnKind.Bool => "expected true or false.",
            ColumnKind.Bytes => "expected hex digits.",
            ColumnKind.Identity => $"expected {Identity.Size * 2} hex digits.",
            ColumnKind.Timestamp => "expected microseconds since the Unix epoch, or an ISO 8601 time.",
            ColumnKind.Float32 or ColumnKind.Float64 or ColumnKind.ScheduleAt => "that kind cannot be a key.",
            _ => "expected a whole number in the column's range.",
        };
        return false;
    }

    private static bool TryHex(string text, out byte[] bytes)
    {
        var digits = text.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? text[2..] : text;
        try
        {
            bytes = Convert.FromHexString(digits);
            return true;
        }
        catch (FormatException)
        {
            bytes = [];
            return false;
        }
    }
}

/// <summary>
/// <c>melange inspect row</c>: one key's mutation history, "who mutated this row, and when". A scan
/// of write sets rather than an index: every commit in the window whose write set touched the key,
/// with the caller and reducer that did it and what each version changed from the one before.
/// </summary>
internal static class InspectRow
{
    private sealed record Touch(CommitRecord Record, RowOp Op, DecodedRow? Row);

    public static int Run(InspectSource source, InspectOptions options, TextWriter output, TextWriter error)
    {
        var resolved = InspectKeys.Resolve(source, options, error);
        if (resolved is null)
            return 1;
        var (table, key, known) = resolved.Value;

        var from = options.From ?? 0;
        var to = options.To ?? ulong.MaxValue;
        var notes = new List<string>();
        if (source.Facts.LogFloor > 0 && from <= source.Facts.LogFloor)
        {
            notes.Add(
                $"the history begins after LSN {source.Facts.LogFloor}; " +
                (source.Facts.Kind == InspectSourceKind.Archive
                    ? "an archive holds only the log above its snapshot, whose row is shown as the starting state."
                    : "mutations before it were truncated away, and an older backup holds them."));
        }

        if (known is { IsArchiveTier: true })
            notes.Add($"'{known.Name}' is archive-tier: its rows live in Postgres, so this is what the log still holds of the row's history, not all of it.");

        // The starting state, where the snapshot sits exactly at the floor: the row as the
        // earliest record in the source found it.
        DecodedRow? start = null;
        var startFound = false;
        if (source.Facts.SnapshotLsn > 0 && source.Facts.SnapshotLsn == source.Facts.LogFloor && from <= source.Facts.SnapshotLsn)
        {
            foreach (var row in source.SnapshotRows())
            {
                if (row.Table == table && row.Key == key)
                {
                    start = source.DecodeRow(source.Facts.SnapshotLsn, table, row.Row.Span);
                    startFound = true;
                    break;
                }
            }
        }

        var touches = new List<Touch>();
        foreach (var record in source.Records(from, to))
        {
            foreach (var op in record.WriteSet)
            {
                if (op.Table == table && op.Key == key)
                    touches.Add(new Touch(record, op, op.Kind == RowOpKind.Delete ? null : source.DecodeRow(record.Lsn, table, op.Row.Span)));
            }
        }

        var tableName = known?.Name ?? options.Table!;
        if (options.Json)
        {
            WriteJson(source, table, key, tableName, start, startFound, touches, notes, output);
            return 0;
        }

        InspectFormat.WriteFacts(output, source.Facts);
        output.WriteLine($"row       {tableName}  {InspectFormat.Key(source, source.Facts.HeadLsn, table, key)}  (encoded {InspectFormat.Hex(key.Span)})");
        output.WriteLine($"history   {InspectFormat.Count(touches.Count, "commit")} touched it" + (options.From is not null || options.To is not null ? " in the window" : ""));
        foreach (var note in notes)
            output.WriteLine($"note      {note}");
        output.WriteLine();

        DecodedRow? previous = null;
        if (startFound)
        {
            output.WriteLine($"as of the snapshot at LSN {source.Facts.SnapshotLsn}:");
            WriteColumns(output, start!, before: null);
            previous = start;
        }
        else if (source.Facts.SnapshotLsn > 0 && source.Facts.SnapshotLsn == source.Facts.LogFloor && from <= source.Facts.SnapshotLsn)
        {
            output.WriteLine($"as of the snapshot at LSN {source.Facts.SnapshotLsn}: no row has this key.");
        }

        foreach (var touch in touches)
        {
            var record = touch.Record;
            output.WriteLine(
                $"LSN {record.Lsn}  {InspectFormat.Time(record.Timestamp)}  {InspectFormat.OpName(touch.Op.Kind)}  by {InspectFormat.Reducer(record.ReducerName)}  caller {record.Caller}");
            if (touch.Row is null)
                output.WriteLine("      (deleted)");
            else
                WriteColumns(output, touch.Row, previous);
            previous = touch.Row;
        }

        output.WriteLine();
        foreach (var change in InspectFormat.Changes(source.CheckForChanges()))
            output.WriteLine($"note      {change}");
        return 0;
    }

    /// <summary>A version's columns: all of them the first time, then only what changed from the version before.</summary>
    private static void WriteColumns(TextWriter output, DecodedRow row, DecodedRow? before)
    {
        var width = row.Columns.Count == 0 ? 0 : row.Columns.Max(static c => c.Name.Length);
        var shown = 0;
        foreach (var column in row.Columns)
        {
            if (before is { Succeeded: true })
            {
                var prior = before.Columns.FirstOrDefault(c => c.Name == column.Name);
                if (prior is not null && InspectCommand.SameValue(prior.Value, column.Value))
                    continue;
                var was = prior is null ? "   (new column)" : $"   (was {InspectFormat.Value(prior.Value)})";
                output.WriteLine($"      {column.Name.PadRight(width)}  {InspectFormat.Value(column.Value)}{was}");
            }
            else
            {
                output.WriteLine($"      {column.Name.PadRight(width)}  {InspectFormat.Value(column.Value)}");
            }

            shown++;
        }

        if (shown == 0 && row.Error is null)
            output.WriteLine("      (rewritten with no column changed)");
        if (row.Error is not null)
            output.WriteLine($"      (cannot decode: {row.Error})");
    }

    private static void WriteJson(
        InspectSource source,
        TableId table,
        RowKey key,
        string tableName,
        DecodedRow? start,
        bool startFound,
        List<Touch> touches,
        List<string> notes,
        TextWriter output)
    {
        using var json = new InspectJson(output);
        var w = json.Writer;
        w.WriteStartObject();
        json.Facts(source.Facts);
        w.WriteStartObject("row");
        w.WriteString("table", tableName);
        InspectCommand.WriteKeyJson(json, source, source.Facts.HeadLsn, table, key);
        w.WriteEndObject();
        if (startFound)
        {
            w.WriteStartObject("snapshot");
            w.WriteNumber("lsn", source.Facts.SnapshotLsn);
            w.WritePropertyName("row");
            InspectCommand.WriteRowJson(json, start!, []);
            w.WriteEndObject();
        }

        w.WriteStartArray("history");
        foreach (var touch in touches)
        {
            var record = touch.Record;
            w.WriteStartObject();
            w.WriteNumber("lsn", record.Lsn);
            w.WriteString("timestamp", record.Timestamp.ToDateTimeOffset());
            w.WriteString("caller", record.Caller.ToString());
            w.WriteString("reducer", record.ReducerName);
            w.WriteString("op", InspectFormat.OpName(touch.Op.Kind));
            w.WritePropertyName("row");
            if (touch.Row is null)
                w.WriteNullValue();
            else
                InspectCommand.WriteRowJson(json, touch.Row, touch.Op.Row.Span);
            w.WriteEndObject();
            json.FlushIfLarge();
        }

        w.WriteEndArray();
        json.Notes(notes.Concat(InspectFormat.Notes(source.Facts)));
        json.Changes(source.CheckForChanges());
        w.WriteEndObject();
    }
}
