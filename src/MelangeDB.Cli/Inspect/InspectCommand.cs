using System.Globalization;
using MelangeDB.Core;

namespace MelangeDB.Cli;

/// <summary>The parsed flags of one <c>melange inspect</c> call.</summary>
internal sealed class InspectOptions
{
    public required string Verb { get; init; }

    public string Source { get; set; } = "";

    public string? Engine { get; set; }

    public bool Json { get; set; }

    public ulong? From { get; set; }

    public ulong? To { get; set; }

    public ulong? AtLsn { get; set; }

    public string? Reducer { get; set; }

    public string? Caller { get; set; }

    public string? Table { get; set; }

    public string? Key { get; set; }

    public string? KeyHex { get; set; }

    public string? Manifest { get; set; }

    public bool All { get; set; }

    public bool Diff { get; set; }
}

/// <summary>
/// <c>melange inspect</c>: the commit log as a readable record (road-to-0.3 phase 21). Every verb
/// reads a data directory or a <c>.mbak</c> through <see cref="InspectSource"/>, read-only and
/// lock-free, prints the source's facts before its answer, and takes <c>--json</c>.
/// </summary>
internal static class InspectCommand
{
    private static readonly Dictionary<string, string[]> FlagsByVerb = new(StringComparer.Ordinal)
    {
        ["log"] = ["--engine", "--json", "--from", "--to", "--reducer", "--caller", "--table", "--diff", "--manifest"],
        ["record"] = ["--engine", "--json", "--at-lsn", "--manifest", "--diff"],
        ["row"] = ["--engine", "--json", "--table", "--key", "--key-hex", "--from", "--to"],
        ["at"] = ["--engine", "--json", "--at-lsn", "--table", "--key", "--key-hex", "--all"],
    };

    private static readonly string[] ValueFlags =
        ["--engine", "--from", "--to", "--at-lsn", "--reducer", "--caller", "--table", "--key", "--key-hex", "--manifest"];

    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        if (args.Length == 0 || args[0] is "-h" or "--help" || !FlagsByVerb.ContainsKey(args[0]))
        {
            if (args.Length > 0 && args[0] is not ("-h" or "--help"))
                error.WriteLine($"melange inspect: unknown verb '{args[0]}'.");
            PrintUsage(error);
            return 2;
        }

        if (!TryParse(args, error, out var options))
        {
            PrintUsage(error);
            return 2;
        }

        try
        {
            using var source = InspectSource.Open(options.Source, options.Engine);
            return options.Verb switch
            {
                "log" => RunLog(source, options, output, error),
                "record" => RunRecord(source, options, output, error),
                "row" => InspectRow.Run(source, options, output, error),
                "at" => InspectAt.Run(source, options, output, error),
                _ => 2,
            };
        }
        catch (Exception exception) when (exception is InvalidOperationException or InvalidDataException or IOException
            or UnauthorizedAccessException or NotSupportedException or MelangeDB.CodeGen.ManifestException)
        {
            error.WriteLine($"melange inspect: {exception.Message}");
            return 1;
        }
    }

    public static void PrintUsage(TextWriter error)
    {
        error.WriteLine("usage: melange inspect log <source> [--from <lsn>] [--to <lsn>] [--reducer <name>] [--caller <hex>] [--table <name>] [--diff] [--json]");
        error.WriteLine("       melange inspect record <source> --at-lsn <n> [--diff] [--manifest melange-schema.json] [--json]");
        error.WriteLine("       melange inspect row <source> --table <name> (--key <value> | --key-hex <hex>) [--from <lsn>] [--to <lsn>] [--json]");
        error.WriteLine("       melange inspect at <source> --at-lsn <n> [--table <name> [--key <value> | --key-hex <hex>] [--all]] [--json]");
        error.WriteLine("  <source> is a data directory (CommitLog:Path, or a shard's) or a .mbak archive; --engine <key> picks an archive's engine.");
        error.WriteLine("  Read-only and lock-free: safe against a live engine's directory. See docs/INSPECT.md.");
    }

    private static bool TryParse(string[] args, TextWriter error, out InspectOptions options)
    {
        options = new InspectOptions { Verb = args[0] };
        var allowed = FlagsByVerb[options.Verb];
        var sawSource = false;
        for (var i = 1; i < args.Length; i++)
        {
            var arg = args[i];
            if (!arg.StartsWith('-'))
            {
                if (sawSource)
                {
                    error.WriteLine($"melange inspect {options.Verb}: one source at a time ('{options.Source}', then '{arg}').");
                    return false;
                }

                options.Source = arg;
                sawSource = true;
                continue;
            }

            if (arg is "-h" or "--help")
                return false;
            if (!allowed.Contains(arg))
            {
                error.WriteLine($"melange inspect {options.Verb} does not take {arg}.");
                return false;
            }

            string? value = null;
            if (ValueFlags.Contains(arg))
            {
                if (i + 1 >= args.Length)
                {
                    error.WriteLine($"melange inspect {options.Verb}: {arg} takes a value.");
                    return false;
                }

                value = args[++i];
            }

            switch (arg)
            {
                case "--json":
                    options.Json = true;
                    break;
                case "--all":
                    options.All = true;
                    break;
                case "--diff":
                    options.Diff = true;
                    break;
                case "--engine":
                    options.Engine = value;
                    break;
                case "--reducer":
                    options.Reducer = value;
                    break;
                case "--table":
                    options.Table = value;
                    break;
                case "--key":
                    options.Key = value;
                    break;
                case "--key-hex":
                    options.KeyHex = value;
                    break;
                case "--manifest":
                    options.Manifest = value;
                    break;
                case "--caller":
                    if (value!.Length == 0 || !value.All(Uri.IsHexDigit))
                    {
                        error.WriteLine("--caller takes an identity in hex, or the start of one.");
                        return false;
                    }

                    options.Caller = value.ToLowerInvariant();
                    break;
                case "--from" or "--to" or "--at-lsn":
                    if (!ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var lsn))
                    {
                        error.WriteLine($"{arg} takes an LSN: a whole number.");
                        return false;
                    }

                    if (arg == "--from")
                        options.From = lsn;
                    else if (arg == "--to")
                        options.To = lsn;
                    else
                        options.AtLsn = lsn;
                    break;
            }
        }

        if (!sawSource)
        {
            error.WriteLine($"melange inspect {options.Verb}: name a data directory or a .mbak archive.");
            return false;
        }

        if (options.From > options.To)
        {
            error.WriteLine($"--from {options.From} is past --to {options.To}.");
            return false;
        }

        if (options.Verb is "record" or "at" && options.AtLsn is null)
        {
            error.WriteLine(options.Verb == "record"
                ? "melange inspect record takes --at-lsn <n>: the commit to show."
                : "melange inspect at takes --at-lsn <n>: the commit to show the world as of.");
            return false;
        }

        if (options.Verb == "at" && (options.Key is not null || options.KeyHex is not null || options.All) && options.Table is null)
        {
            error.WriteLine("melange inspect at: --key, --key-hex and --all go with --table <name>.");
            return false;
        }

        if (options.Verb == "at" && options.Key is not null && options.KeyHex is not null)
        {
            error.WriteLine("melange inspect at: one of --key or --key-hex, not both.");
            return false;
        }

        if (options.Verb == "log" && options.Manifest is not null && !options.Diff)
        {
            error.WriteLine("melange inspect log: --manifest names arguments, which only --diff prints.");
            return false;
        }

        if (options.Verb == "row" && (options.Table is null || (options.Key is null) == (options.KeyHex is null)))
        {
            error.WriteLine("melange inspect row takes --table <name> and one of --key <value> or --key-hex <hex>.");
            return false;
        }

        return true;
    }

    // --- log --------------------------------------------------------------------------------

    private static int RunLog(InspectSource source, InspectOptions options, TextWriter output, TextWriter error)
    {
        var from = options.From ?? 0;
        var to = options.To ?? ulong.MaxValue;
        var notes = new List<string>();
        if (options.From is { } requested && source.ExplainUnreachable(requested) is { } unreachable)
            notes.Add(unreachable);
        else if (options.To is { } until && source.ExplainUnreachable(until) is { } belowFloor && until <= source.Facts.LogFloor)
            notes.Add(belowFloor);
        TableId? table = null;
        if (options.Table is { } tableName)
        {
            table = TableId.FromName(tableName);
            if (source.Shapes is { } shapes && !shapes.Entries.Any(e => e.Tables.ContainsKey(tableName)))
                notes.Add($"no shape in this source names a table '{tableName}'; matching by its table id ({table}) anyway");
        }

        bool Matches(CommitRecord record) =>
            (options.Reducer is null || record.ReducerName == options.Reducer)
            && (options.Caller is null || record.Caller.ToString().StartsWith(options.Caller, StringComparison.Ordinal))
            && (table is not { } wanted || record.WriteSet.Any(op => op.Table == wanted));

        if (options.Diff)
            return RunLogDiff(source, options, from, to, Matches, notes, output, error);

        long shown = 0;
        if (options.Json)
        {
            using var json = new InspectJson(output);
            var w = json.Writer;
            w.WriteStartObject();
            json.Facts(source.Facts);
            w.WriteStartArray("records");
            foreach (var record in source.Records(from, to))
            {
                if (!Matches(record))
                    continue;
                w.WriteStartObject();
                w.WriteNumber("lsn", record.Lsn);
                w.WriteString("timestamp", record.Timestamp.ToDateTimeOffset());
                w.WriteString("caller", record.Caller.ToString());
                w.WriteString("reducer", record.ReducerName);
                w.WriteNumber("writes", record.WriteSet.Count);
                w.WriteNumber("events", record.Events.Count);
                w.WriteNumber("bytes", record.SerializedLength);
                w.WriteEndObject();
                shown++;
                json.FlushIfLarge();
            }

            w.WriteEndArray();
            w.WriteNumber("shown", shown);
            json.Notes(notes.Concat(InspectFormat.Notes(source.Facts)));
            json.Changes(source.CheckForChanges());
            w.WriteEndObject();
            return 0;
        }

        InspectFormat.WriteFacts(output, source.Facts);
        foreach (var note in notes)
            output.WriteLine($"note      {note}");
        if (notes.Count > 0)
            output.WriteLine();
        var width = Math.Max(3, source.Facts.HeadLsn.ToString(CultureInfo.InvariantCulture).Length);
        output.WriteLine($"{"LSN".PadLeft(width)}  {"TIME (UTC)",-27}  {"CALLER",-16}  {"WRITES",6}  {"EVENTS",6}  REDUCER");
        foreach (var record in source.Records(from, to))
        {
            if (!Matches(record))
                continue;
            output.WriteLine(
                $"{record.Lsn.ToString(CultureInfo.InvariantCulture).PadLeft(width)}  {InspectFormat.Time(record.Timestamp),-27}  " +
                $"{record.Caller.ToString()[..16]}  {record.WriteSet.Count,6}  {record.Events.Count,6}  {InspectFormat.Reducer(record.ReducerName)}");
            shown++;
        }

        output.WriteLine();
        output.WriteLine($"{InspectFormat.Count(shown, "record")} shown.");
        foreach (var change in InspectFormat.Changes(source.CheckForChanges()))
            output.WriteLine($"note      {change}");
        return 0;
    }

    /// <summary>
    /// <c>log --diff</c>: every matching record in full, each write beside what it replaced. One
    /// replay carried across the window, standing just before its first record, never a replay
    /// per record; refused where <c>inspect at</c> would be, for the same reason.
    /// </summary>
    private static int RunLogDiff(
        InspectSource source,
        InspectOptions options,
        ulong from,
        ulong to,
        Func<CommitRecord, bool> matches,
        List<string> notes,
        TextWriter output,
        TextWriter error)
    {
        var first = Math.Max(from, source.Facts.FirstLsn);
        if (source.Facts.RecordCount == 0 || first > Math.Min(to, source.Facts.HeadLsn))
        {
            error.WriteLine("melange inspect: the window holds no records to diff.");
            return 1;
        }

        if (InspectProjection.ExplainUnbuildable(source, first - 1) is { } refusal)
        {
            error.WriteLine($"melange inspect: --diff needs the world as of LSN {first - 1}, just before the window. {refusal}");
            return 1;
        }

        var parameters = options.Manifest is null ? null : ParameterNames.Load(options.Manifest);
        var projection = InspectProjection.Build(source, first - 1);
        long shown = 0;
        if (options.Json)
        {
            using var json = new InspectJson(output);
            var w = json.Writer;
            w.WriteStartObject();
            json.Facts(source.Facts);
            w.WriteStartArray("records");
            foreach (var record in source.Records(first, to))
            {
                if (matches(record))
                {
                    WriteRecordJson(json, source, record, RowDecoder.DecodeArguments(record.Arguments.Span), parameters?.For(record.ReducerName), InspectAt.Before(source, projection, record));
                    shown++;
                    json.FlushIfLarge();
                }

                projection.Apply(record);
            }

            w.WriteEndArray();
            w.WriteNumber("shown", shown);
            json.Notes(notes.Concat(InspectFormat.Notes(source.Facts)));
            json.Changes(source.CheckForChanges());
            w.WriteEndObject();
            return 0;
        }

        InspectFormat.WriteFacts(output, source.Facts);
        foreach (var note in notes)
            output.WriteLine($"note      {note}");
        output.WriteLine(projection.FromSnapshot
            ? $"diff      against the world as of LSN {first - 1}, built from the snapshot at LSN {projection.StartLsn}"
            : $"diff      against the world as of LSN {first - 1}, built from empty");
        output.WriteLine();
        foreach (var record in source.Records(first, to))
        {
            if (matches(record))
            {
                WriteRecordText(output, source, record, RowDecoder.DecodeArguments(record.Arguments.Span), parameters?.For(record.ReducerName), parameters is not null, InspectAt.Before(source, projection, record));
                shown++;
            }

            projection.Apply(record);
        }

        output.WriteLine($"{InspectFormat.Count(shown, "record")} shown.");
        foreach (var change in InspectFormat.Changes(source.CheckForChanges()))
            output.WriteLine($"note      {change}");
        return 0;
    }

    // --- record -----------------------------------------------------------------------------

    private static int RunRecord(InspectSource source, InspectOptions options, TextWriter output, TextWriter error)
    {
        var lsn = options.AtLsn!.Value;
        if (source.ExplainUnreachable(lsn) is { } unreachable)
        {
            error.WriteLine($"melange inspect: {unreachable}");
            return 1;
        }

        var record = source.Record(lsn);
        if (record is null)
        {
            error.WriteLine($"melange inspect: LSN {lsn} is not in this source: the LSNs jump past it.");
            return 1;
        }

        Dictionary<(TableId, RowKey), DecodedRow?>? before = null;
        if (options.Diff)
        {
            if (InspectProjection.ExplainUnbuildable(source, lsn - 1) is { } refusal)
            {
                error.WriteLine($"melange inspect: --diff needs the world as of LSN {lsn - 1}, just before this record. {refusal}");
                return 1;
            }

            before = InspectAt.Before(source, InspectProjection.Build(source, lsn - 1), record);
        }

        var parameters = options.Manifest is null ? null : ParameterNames.Load(options.Manifest);
        var names = parameters?.For(record.ReducerName);
        var arguments = RowDecoder.DecodeArguments(record.Arguments.Span);
        if (options.Json)
        {
            using var json = new InspectJson(output);
            var w = json.Writer;
            w.WriteStartObject();
            json.Facts(source.Facts);
            w.WritePropertyName("record");
            WriteRecordJson(json, source, record, arguments, names, before);
            json.Notes(InspectFormat.Notes(source.Facts));
            json.Changes(source.CheckForChanges());
            w.WriteEndObject();
            return 0;
        }

        InspectFormat.WriteFacts(output, source.Facts);
        WriteRecordText(output, source, record, arguments, names, parameters is not null, before);
        foreach (var change in InspectFormat.Changes(source.CheckForChanges()))
            output.WriteLine($"note      {change}");
        return 0;
    }

    internal static void WriteRecordText(
        TextWriter output,
        InspectSource source,
        CommitRecord record,
        DecodedArguments arguments,
        IReadOnlyList<string>? names,
        bool manifestGiven,
        IReadOnlyDictionary<(TableId, RowKey), DecodedRow?>? before = null)
    {
        output.WriteLine($"LSN {record.Lsn}  {InspectFormat.Time(record.Timestamp)}  ({InspectFormat.Count(record.SerializedLength, "byte")} on disk)");
        output.WriteLine($"caller    {record.Caller}");
        output.WriteLine($"reducer   {InspectFormat.Reducer(record.ReducerName)}");
        output.WriteLine($"arguments {arguments.Values.Count}" + (manifestGiven && names is null ? " (the manifest does not name this reducer's parameters: it lists client-callable reducers only)" : ""));
        for (var i = 0; i < arguments.Values.Count; i++)
        {
            var name = names is not null && i < names.Count ? $" {names[i]}" : "";
            output.WriteLine($"  [{i}]{name} = {InspectFormat.Value(arguments.Values[i])}");
        }

        if (arguments.Error is not null)
            output.WriteLine($"  (cannot decode the rest: {arguments.Error}; bytes {InspectFormat.Hex(record.Arguments.Span)})");

        output.WriteLine($"writes    {record.WriteSet.Count}");
        foreach (var op in record.WriteSet)
        {
            var table = source.TableAt(record.Lsn, op.Table);
            var tier = table is { IsArchiveTier: true } ? "  (archive tier: lives in Postgres, never in a snapshot)" : "";
            output.WriteLine($"  {InspectFormat.OpName(op.Kind)} {table?.Name ?? op.Table.ToString()}  {InspectFormat.Key(source, record.Lsn, op.Table, op.Key)}{tier}");
            DecodedRow? previous = null;
            var hasBefore = before is not null && before.TryGetValue((op.Table, op.Key), out previous);
            if (op.Kind == RowOpKind.Delete)
            {
                if (hasBefore)
                    WriteBefore(output, previous);
                continue;
            }

            var row = source.DecodeRow(record.Lsn, op.Table, op.Row.Span);
            var width = row.Columns.Count == 0 ? 0 : row.Columns.Max(static c => c.Name.Length);
            foreach (var column in row.Columns)
            {
                var was = hasBefore && previous is { Succeeded: true } old && old.Columns.FirstOrDefault(c => c.Name == column.Name) is { } prior
                    && !SameValue(prior.Value, column.Value)
                        ? $"   (was {InspectFormat.Value(prior.Value)})"
                        : "";
                var isNew = hasBefore && previous is { Succeeded: true } shaped && shaped.Columns.All(c => c.Name != column.Name) ? "   (new column)" : "";
                output.WriteLine($"      {column.Name.PadRight(width)}  {InspectFormat.Value(column.Value)}{was}{isNew}");
            }

            if (row.Error is not null)
                output.WriteLine($"      (cannot decode: {row.Error} Bytes: {InspectFormat.Hex(op.Row.Span)})");
            if (hasBefore && previous is null)
                output.WriteLine("      (no row had this key before this commit)");
        }

        output.WriteLine($"events    {record.Events.Count}");
        foreach (var evt in record.Events)
        {
            output.WriteLine(
                $"  {InspectFormat.Name(evt.EventType)}  depth {evt.Depth}  {InspectFormat.Count(evt.Payload.Length, "byte")}  {InspectFormat.Hex(evt.Payload.Span)}" +
                "  (an event payload is the application's bytes; no shape records it)");
        }

        output.WriteLine();
    }

    private static void WriteBefore(TextWriter output, DecodedRow? previous)
    {
        if (previous is null)
        {
            output.WriteLine("      (no row had this key before this commit)");
            return;
        }

        var width = previous.Columns.Count == 0 ? 0 : previous.Columns.Max(static c => c.Name.Length);
        foreach (var column in previous.Columns)
            output.WriteLine($"      {column.Name.PadRight(width)}  (was {InspectFormat.Value(column.Value)})");
    }

    internal static bool SameValue(object? a, object? b) => a switch
    {
        byte[] left when b is byte[] right => left.AsSpan().SequenceEqual(right),
        _ => Equals(a, b),
    };

    internal static void WriteRecordJson(
        InspectJson json,
        InspectSource source,
        CommitRecord record,
        DecodedArguments arguments,
        IReadOnlyList<string>? names,
        IReadOnlyDictionary<(TableId, RowKey), DecodedRow?>? before = null)
    {
        var w = json.Writer;
        w.WriteStartObject();
        w.WriteNumber("lsn", record.Lsn);
        w.WriteString("timestamp", record.Timestamp.ToDateTimeOffset());
        w.WriteString("caller", record.Caller.ToString());
        w.WriteString("reducer", record.ReducerName);
        w.WriteNumber("formatVersion", record.FormatVersion);
        w.WriteNumber("bytes", record.SerializedLength);
        w.WriteStartArray("arguments");
        for (var i = 0; i < arguments.Values.Count; i++)
            json.Argument(names is not null && i < names.Count ? names[i] : null, arguments.Values[i]);
        w.WriteEndArray();
        if (arguments.Error is not null)
        {
            w.WriteString("argumentsError", arguments.Error);
            w.WriteString("argumentsHex", Convert.ToHexStringLower(record.Arguments.Span));
        }

        w.WriteStartArray("writes");
        foreach (var op in record.WriteSet)
        {
            var table = source.TableAt(record.Lsn, op.Table);
            w.WriteStartObject();
            w.WriteString("op", InspectFormat.OpName(op.Kind));
            w.WriteString("table", table?.Name ?? op.Table.ToString());
            if (table is { IsArchiveTier: true })
                w.WriteBoolean("archiveTier", true);
            WriteKeyJson(json, source, record.Lsn, op.Table, op.Key);
            if (op.Kind != RowOpKind.Delete)
            {
                w.WritePropertyName("row");
                WriteRowJson(json, source.DecodeRow(record.Lsn, op.Table, op.Row.Span), op.Row.Span);
            }

            if (before is not null)
            {
                w.WritePropertyName("before");
                if (before.TryGetValue((op.Table, op.Key), out var previous) && previous is not null)
                    WriteRowJson(json, previous, []);
                else
                    w.WriteNullValue();
            }

            w.WriteEndObject();
        }

        w.WriteEndArray();
        w.WriteStartArray("events");
        foreach (var evt in record.Events)
        {
            w.WriteStartObject();
            w.WriteString("type", evt.EventType);
            w.WriteNumber("depth", evt.Depth);
            w.WriteNumber("payloadBytes", evt.Payload.Length);
            w.WriteString("payloadHex", Convert.ToHexStringLower(evt.Payload.Span));
            w.WriteEndObject();
        }

        w.WriteEndArray();
        w.WriteEndObject();
    }

    internal static void WriteKeyJson(InspectJson json, InspectSource source, ulong lsn, TableId table, in RowKey key)
    {
        var w = json.Writer;
        var decoded = source.DecodeKey(lsn, table, key);
        w.WriteStartObject("key");
        if (decoded.Succeeded)
        {
            w.WriteString("column", decoded.Column);
            w.WritePropertyName("value");
            json.ColumnValue(decoded.Value);
        }
        else
        {
            w.WriteString("error", decoded.Error);
        }

        w.WriteString("hex", Convert.ToHexStringLower(key.Span));
        w.WriteEndObject();
    }

    internal static void WriteRowJson(InspectJson json, DecodedRow row, ReadOnlySpan<byte> bytes)
    {
        var w = json.Writer;
        w.WriteStartObject();
        w.WriteStartObject("columns");
        foreach (var column in row.Columns)
        {
            w.WritePropertyName(column.Name);
            json.ColumnValue(column.Value);
        }

        w.WriteEndObject();
        if (row.Error is not null)
        {
            w.WriteString("error", row.Error);
            if (!bytes.IsEmpty)
                w.WriteString("hex", Convert.ToHexStringLower(bytes));
        }

        w.WriteEndObject();
    }
}

/// <summary>
/// Parameter names from a schema manifest (<c>melange schema</c>'s output), the one thing the log's
/// self-describing arguments do not carry. Client-callable reducers and producers only: the
/// manifest lists nothing a client cannot call, so a scheduled reducer's arguments stay unnamed.
/// </summary>
internal sealed class ParameterNames
{
    private readonly Dictionary<string, string[]> _byReducer;

    private ParameterNames(Dictionary<string, string[]> byReducer) => _byReducer = byReducer;

    public static ParameterNames Load(string path)
    {
        var model = MelangeDB.CodeGen.ManifestParser.Parse(File.ReadAllText(path));
        var names = new Dictionary<string, string[]>(StringComparer.Ordinal);
        foreach (var reducer in model.Reducers.Items)
            names[reducer.Name] = [.. reducer.Parameters.Items.Select(static p => p.Name)];
        foreach (var producer in model.Producers.Items)
            names.TryAdd(producer.Name, [.. producer.Parameters.Items.Select(static p => p.Name)]);
        return new ParameterNames(names);
    }

    public IReadOnlyList<string>? For(string reducer) => _byReducer.TryGetValue(reducer, out var names) ? names : null;
}
