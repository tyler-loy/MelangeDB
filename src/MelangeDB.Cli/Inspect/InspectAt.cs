using MelangeDB.Core;

namespace MelangeDB.Cli;

/// <summary>
/// <c>melange inspect at</c>: the world as of a commit, rebuilt in memory by
/// <see cref="InspectProjection"/>, and the <c>--diff</c> that the same replay gives a window of
/// records. What the view is not (no indexes, no SQL, no policies, no archive-tier tables) is said
/// in its own output, because the failure to avoid is an operator deciding a row is absent when it
/// is only unreachable by the index they had in mind.
/// </summary>
internal static class InspectAt
{
    /// <summary>A <c>--table</c> dump stops here unless <c>--all</c> is given; a single row by key is never guarded.</summary>
    public const int RowGuard = 10_000;

    private const string WhatThisIs =
        "raw rows decoded from the recorded shapes. No indexes, no SQL, no row policies, no residency: a row absent here is absent from the table.";

    public static int Run(InspectSource source, InspectOptions options, TextWriter output, TextWriter error)
    {
        var lsn = options.AtLsn!.Value;
        if (InspectProjection.ExplainUnbuildable(source, lsn) is { } refusal)
        {
            error.WriteLine($"melange inspect: {refusal}");
            return 1;
        }

        (TableId Table, RowKey Key, InspectTable? Known)? wantedKey = null;
        InspectTable? wantedTable = null;
        if (options.Table is { } tableName)
        {
            if (options.Key is not null || options.KeyHex is not null)
            {
                wantedKey = InspectKeys.Resolve(source, options, error);
                if (wantedKey is null)
                    return 1;
            }

            wantedTable = InspectKeys.FindTable(source, tableName);
            if (wantedTable is null && wantedKey is null)
            {
                error.WriteLine($"melange inspect: no shape in this source names a table '{tableName}'. Names are case-sensitive; run without --table to list the tables.");
                return 1;
            }
        }

        var projection = InspectProjection.Build(source, lsn);
        var tableId = wantedKey?.Table ?? wantedTable?.Id;
        var leftOutWanted = tableId is { } id && (projection.IsLeftOut(id) || wantedTable is { IsArchiveTier: true });
        var notes = new List<string>();
        if (leftOutWanted)
            notes.Add($"'{options.Table}' is archive-tier: its rows live in Postgres, never in a snapshot, so this projection does not hold them. Query Postgres for its state.");

        var when = source.Record(lsn)?.Timestamp ?? (lsn == source.Facts.SnapshotLsn ? source.Facts.SnapshotTimestamp : null);
        if (options.Json)
        {
            WriteJson(source, options, projection, lsn, when, tableId, wantedKey, leftOutWanted, notes, output);
            return 0;
        }

        InspectFormat.WriteFacts(output, source.Facts);
        output.WriteLine($"world     as of LSN {lsn}" + (when is { } at ? $" ({InspectFormat.Time(at)})" : ""));
        output.WriteLine(projection.FromSnapshot
            ? $"built     from the snapshot at LSN {projection.StartLsn}, then {InspectFormat.Count(projection.Replayed, "record")} replayed"
            : $"built     from empty, replaying {InspectFormat.Count(projection.Replayed, "record")}");
        output.WriteLine($"this is   {WhatThisIs}");
        if (projection.LeftOut.Count > 0)
            output.WriteLine($"left out  {string.Join(", ", projection.LeftOut.Select(static t => t.Name))}: archive-tier, so their rows live in Postgres, not here");
        foreach (var note in notes)
            output.WriteLine($"note      {note}");
        output.WriteLine();

        if (wantedKey is { } single)
        {
            var name = single.Known?.Name ?? options.Table!;
            if (!leftOutWanted && projection.Find(single.Table, single.Key) is { } row)
                WriteRow(output, source, name, row);
            else if (!leftOutWanted)
                output.WriteLine($"{name}  {InspectFormat.Key(source, lsn, single.Table, single.Key)}: no row has this key as of LSN {lsn}.");
        }
        else if (tableId is { } table)
        {
            var rows = projection.Rows(table).ToList();
            output.WriteLine($"{options.Table}: {InspectFormat.Count(rows.Count, "row")}");
            output.WriteLine();
            foreach (var row in options.All ? rows : rows.Take(RowGuard))
                WriteRow(output, source, options.Table!, row);
            if (!options.All && rows.Count > RowGuard)
                output.WriteLine($"... {InspectFormat.Count(rows.Count - RowGuard, "more row")} not shown. --all prints every row; --key prints one.");
        }
        else
        {
            var counts = projection.Counts;
            var names = counts.Keys
                .Select(t => (Name: InspectFormat.TableName(source, lsn, t), Rows: counts[t]))
                .Concat(source.TablesAt(lsn).Where(t => !counts.ContainsKey(t.Id) && !t.IsArchiveTier).Select(static t => (t.Name, Rows: 0)))
                .OrderBy(static t => t.Name, StringComparer.Ordinal)
                .ToList();
            var width = names.Count == 0 ? 0 : names.Max(static t => t.Name.Length);
            output.WriteLine("tables");
            foreach (var (name, rows) in names)
                output.WriteLine($"  {name.PadRight(width)}  {InspectFormat.Count(rows, "row")}");
            output.WriteLine();
            output.WriteLine("--table <name> dumps one table's rows; --key <value> with it shows one row.");
        }

        foreach (var change in InspectFormat.Changes(source.CheckForChanges()))
            output.WriteLine($"note      {change}");
        return 0;
    }

    private static void WriteRow(TextWriter output, InspectSource source, string table, ProjectedRow row)
    {
        output.WriteLine($"{table}  {InspectFormat.Key(source, row.WrittenAt, row.Table, row.Key)}  (written at LSN {row.WrittenAt})");
        var decoded = source.DecodeRow(row.WrittenAt, row.Table, row.Row.Span);
        var width = decoded.Columns.Count == 0 ? 0 : decoded.Columns.Max(static c => c.Name.Length);
        foreach (var column in decoded.Columns)
            output.WriteLine($"    {column.Name.PadRight(width)}  {InspectFormat.Value(column.Value)}");
        if (decoded.Error is not null)
            output.WriteLine($"    (cannot decode: {decoded.Error} Bytes: {InspectFormat.Hex(row.Row.Span)})");
    }

    private static void WriteJson(
        InspectSource source,
        InspectOptions options,
        InspectProjection projection,
        ulong lsn,
        Timestamp? when,
        TableId? tableId,
        (TableId Table, RowKey Key, InspectTable? Known)? wantedKey,
        bool leftOutWanted,
        List<string> notes,
        TextWriter output)
    {
        using var json = new InspectJson(output);
        var w = json.Writer;
        w.WriteStartObject();
        json.Facts(source.Facts);
        w.WriteStartObject("world");
        w.WriteNumber("atLsn", lsn);
        if (when is { } at)
            w.WriteString("timestamp", at.ToDateTimeOffset());
        w.WriteString("builtFrom", projection.FromSnapshot ? "snapshot" : "empty");
        w.WriteNumber("startLsn", projection.StartLsn);
        w.WriteNumber("replayed", projection.Replayed);
        w.WriteString("thisIs", WhatThisIs);
        w.WriteStartArray("leftOut");
        foreach (var table in projection.LeftOut)
            w.WriteStringValue(table.Name);
        w.WriteEndArray();
        w.WriteStartArray("tables");
        foreach (var (table, rows) in projection.Counts.OrderBy(t => InspectFormat.TableName(source, lsn, t.Key), StringComparer.Ordinal))
        {
            w.WriteStartObject();
            w.WriteString("name", InspectFormat.TableName(source, lsn, table));
            w.WriteNumber("rows", rows);
            w.WriteEndObject();
        }

        w.WriteEndArray();
        w.WriteEndObject();

        if (wantedKey is { } single)
        {
            w.WritePropertyName("row");
            if (!leftOutWanted && projection.Find(single.Table, single.Key) is { } row)
                WriteRowJson(json, source, row);
            else
                w.WriteNullValue();
        }
        else if (tableId is { } table)
        {
            var rows = projection.Rows(table).ToList();
            w.WriteStartArray("rows");
            foreach (var row in options.All ? rows : rows.Take(RowGuard))
            {
                WriteRowJson(json, source, row);
                json.FlushIfLarge();
            }

            w.WriteEndArray();
            w.WriteNumber("omitted", options.All ? 0 : Math.Max(0, rows.Count - RowGuard));
        }

        json.Notes(notes.Concat(InspectFormat.Notes(source.Facts)));
        json.Changes(source.CheckForChanges());
        w.WriteEndObject();
    }

    private static void WriteRowJson(InspectJson json, InspectSource source, ProjectedRow row)
    {
        var w = json.Writer;
        w.WriteStartObject();
        w.WriteNumber("writtenAt", row.WrittenAt);
        InspectCommand.WriteKeyJson(json, source, row.WrittenAt, row.Table, row.Key);
        w.WritePropertyName("row");
        InspectCommand.WriteRowJson(json, source.DecodeRow(row.WrittenAt, row.Table, row.Row.Span), row.Row.Span);
        w.WriteEndObject();
    }

    /// <summary>
    /// What each write in <paramref name="record"/> replaced, read from a projection standing just
    /// before it: a row, or null where no row had the key. Archive-tier writes have no entry, since
    /// the projection does not hold those tables.
    /// </summary>
    public static Dictionary<(TableId, RowKey), DecodedRow?> Before(InspectSource source, InspectProjection projection, CommitRecord record)
    {
        var before = new Dictionary<(TableId, RowKey), DecodedRow?>();
        foreach (var op in record.WriteSet)
        {
            if (projection.IsLeftOut(op.Table) || source.TableAt(record.Lsn, op.Table) is { IsArchiveTier: true })
                continue;
            before[(op.Table, op.Key)] = projection.Find(op.Table, op.Key) is { } row
                ? source.DecodeRow(row.WrittenAt, row.Table, row.Row.Span)
                : null;
        }

        return before;
    }
}
