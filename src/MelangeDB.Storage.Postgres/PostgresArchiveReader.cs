using System.Data;
using MelangeDB.Core;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// <see cref="IArchiveReader"/> over Postgres. One read-only, repeatable-read transaction reads the
/// applier's checkpoint and the page together, so the two describe the same moment; then the log
/// from the checkpoint to the durable head is overlaid — inserts added, deletes removed — for the
/// one table read. The overlay is skipped and the page marked stale when that tail is longer than
/// <see cref="MaxOverlayRecords"/>, so a long Postgres outage cannot turn history reads into log scans.
/// </summary>
internal sealed class PostgresArchiveReader : IArchiveReader
{
    /// <summary>The longest unapplied tail a read overlays; beyond it the page is Postgres alone, marked stale.</summary>
    internal const int MaxOverlayRecords = 50_000;

    private readonly MelangeEngine _engine;
    private readonly PostgresConnectionSource _connections;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly int _maxOverlayRecords;

    public PostgresArchiveReader(MelangeEngine engine, PostgresConnectionSource connections, IOptionsMonitor<MelangeDbOptions> options)
        : this(engine, connections, options, MaxOverlayRecords)
    {
    }

    internal PostgresArchiveReader(MelangeEngine engine, PostgresConnectionSource connections, IOptionsMonitor<MelangeDbOptions> options, int maxOverlayRecords)
    {
        _engine = engine;
        _connections = connections;
        _options = options;
        _maxOverlayRecords = maxOverlayRecords;
    }

    public async Task<ArchivePage<TRow>> ReadAsync<TRow>(ArchiveQuery query, CancellationToken cancellationToken = default)
        where TRow : struct
    {
        ArgumentNullException.ThrowIfNull(query);
        var table = _engine.Schema.Get(typeof(TRow));
        if (table.Tier != StorageTier.Archive)
            throw new ArgumentException($"Table '{table.Name}' is not an archive table; read hot tables in a reducer or a committed view.", nameof(query));
        var limit = Math.Clamp(query.Limit, 1, ArchiveQuery.MaxLimit);
        var filter = Filter.From(table, query);
        var key = table.PrimaryKey;
        var after = query.After is null ? (RowKey?)null : SchemaKeyCodec.Encode(key, query.After);
        if (query.AsOfLsn is { } asOf)
            await WaitDurableAsync(asOf, cancellationToken).ConfigureAwait(false);

        // Overlay first, to know how many tombstones the Postgres page must over-fetch to stay full.
        var schemaName = _options.CurrentValue.Postgres.Schema;
        await using var connection = await _connections.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);
        var (checkpointLsn, checkpointEpoch) = await ReadCheckpointAsync(connection, transaction, schemaName, cancellationToken).ConfigureAwait(false);
        var overlay = checkpointEpoch == _engine.Log.EpochId ? Overlay(table, checkpointLsn, filter, after, query.Descending, query.AsOfLsn) : null;

        var page = await ReadPageAsync(connection, transaction, schemaName, table, filter, query, limit + (overlay?.Tombstones ?? 0), cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        if (overlay is not null)
        {
            foreach (var (rowKey, row) in overlay.Rows)
            {
                if (row is null)
                    page.Remove(rowKey);
                else
                    page[rowKey] = row;
            }
        }

        var ordered = query.Descending ? page.OrderByDescending(static p => p.Key) : page.OrderBy(static p => p.Key);
        var rows = ordered.Take(limit).Select(static p => (TRow)p.Value!).ToList();
        object? next = rows.Count == limit ? key.GetValue(rows[^1]) : null;
        return new ArchivePage<TRow>(rows, next, Stale: overlay is null);
    }

    private sealed record OverlayResult(Dictionary<RowKey, object?> Rows, int Tombstones);

    /// <summary>
    /// Waits until the log is durable through <paramref name="asOf"/>, because the overlay reads only
    /// durable records: a pinned view can include a commit whose fsync is still in flight, and an
    /// overlay stopping short of it would miss a row the view already shows as moved out of hot.
    /// </summary>
    private async Task WaitDurableAsync(ulong asOf, CancellationToken cancellationToken)
    {
        var log = _engine.Log;
        if (asOf > log.HeadLsn)
            throw new ArgumentException($"AsOfLsn {asOf} is past the log head ({log.HeadLsn}); pass a pinned view's Lsn.", "query");
        if (asOf > log.DurableLsn)
            await Task.Run(() => log.WaitDurable(asOf), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The unapplied tail's effect on this table, by key: a row, or null for a delete. Null when it
    /// is too long to read. Stops at <paramref name="asOf"/> when one is given.
    /// </summary>
    private OverlayResult? Overlay(TableSchema table, ulong checkpointLsn, Filter filter, RowKey? after, bool descending, ulong? asOf)
    {
        var durable = Math.Min(_engine.Log.DurableLsn, asOf ?? ulong.MaxValue);
        if (durable <= checkpointLsn)
            return new OverlayResult([], 0);
        if (durable - checkpointLsn > (ulong)_maxOverlayRecords)
            return null;

        var rows = new Dictionary<RowKey, object?>();
        foreach (var raw in _engine.Log.ReadFrom(checkpointLsn + 1))
        {
            if (raw.Lsn > durable)
                break;
            var record = _engine.TransformToCurrentShape(raw);
            foreach (var op in record.WriteSet)
            {
                if (op.Table != table.Id)
                    continue;
                if (op.Kind == RowOpKind.Delete)
                {
                    rows[op.Key] = null;
                    continue;
                }

                var row = RowSerializer.Deserialize(table, op.Row);
                var beyondCursor = after is not { } cursor || (descending ? op.Key.CompareTo(cursor) < 0 : op.Key.CompareTo(cursor) > 0);
                rows[op.Key] = beyondCursor && filter.Matches(row) ? row : null;
            }
        }

        return new OverlayResult(rows, rows.Count(static p => p.Value is null));
    }

    private static async Task<(ulong Lsn, Guid? Epoch)> ReadCheckpointAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string schema, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT \"applied_lsn\", \"log_epoch\" FROM {PostgresIdentifier.Qualify(schema, PostgresSchemaManager.CheckpointTable)} WHERE \"applier\" = 'postgres'",
            connection,
            transaction);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await reader.ReadAsync(ct).ConfigureAwait(false) ? ((ulong)reader.GetInt64(0), reader.GetGuid(1)) : (0, null);
    }

    private static async Task<Dictionary<RowKey, object?>> ReadPageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schema,
        TableSchema table,
        Filter filter,
        ArchiveQuery query,
        int fetch,
        CancellationToken ct)
    {
        var key = table.PrimaryKey;
        var columns = string.Join(", ", table.Columns.Select(static c => PostgresIdentifier.Quote(c.Name)));
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var where = new List<string>();
        if (filter.Column is { } column)
        {
            var quoted = PostgresIdentifier.Quote(column.Name);
            command.Parameters.Add(PostgresTypeMap.Parameter(column, filter.Low));
            if (filter.IsEqual)
            {
                where.Add($"{quoted} = $1");
            }
            else
            {
                command.Parameters.Add(PostgresTypeMap.Parameter(column, filter.High));
                where.Add($"{quoted} BETWEEN $1 AND $2");
            }
        }

        if (query.After is not null)
        {
            command.Parameters.Add(PostgresTypeMap.Parameter(key, query.After));
            where.Add($"{PostgresIdentifier.Quote(key.Name)} {(query.Descending ? "<" : ">")} ${command.Parameters.Count}");
        }

        if (query.AsOfLsn is { } asOf)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = checked((long)asOf) });
            where.Add($"{PostgresIdentifier.Quote(PostgresSchemaManager.LsnColumn)} <= ${command.Parameters.Count}");
        }

        command.CommandText =
            $"SELECT {columns} FROM {PostgresIdentifier.Qualify(schema, table.Name)}" +
            (where.Count > 0 ? " WHERE " + string.Join(" AND ", where) : string.Empty) +
            $" ORDER BY {PostgresIdentifier.Quote(key.Name)} {(query.Descending ? "DESC" : "ASC")} LIMIT {fetch}";

        var page = new Dictionary<RowKey, object?>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var row = Activator.CreateInstance(table.RowType)!;
            for (var i = 0; i < table.Columns.Count; i++)
            {
                var target = table.Columns[i];
                target.SetValue(row, PostgresTypeMap.ToRowValue(reader.GetValue(i), target));
            }

            page[SchemaKeyCodec.Encode(key, key.GetValue(row)!)] = row;
        }

        return page;
    }

    /// <summary>The one-column filter, validated against the schema and comparable as encoded keys.</summary>
    private sealed class Filter
    {
        public ColumnSchema? Column { get; private init; }

        public object? Low { get; private init; }

        public object? High { get; private init; }

        public bool IsEqual { get; private init; }

        private RowKey _low;
        private RowKey _high;

        public static Filter From(TableSchema table, ArchiveQuery query)
        {
            if (query.Column is null)
                return new Filter();
            var column = table.Column(query.Column);
            if (column is { IsPrimaryKey: false, IsIndexed: false, IsUnique: false })
                throw new ArgumentException($"Table '{table.Name}': column '{column.Name}' is not indexed; an archive read filters on the key or an [Index] column.", nameof(query));
            if (query.Equal is not null)
            {
                if (query.From is not null || query.To is not null)
                    throw new ArgumentException("Pass Equal, or From and To — not both.", nameof(query));
                var encoded = SchemaKeyCodec.Encode(column, query.Equal);
                return new Filter { Column = column, Low = query.Equal, High = query.Equal, IsEqual = true, _low = encoded, _high = encoded };
            }

            if (query.From is null || query.To is null)
                throw new ArgumentException("A filtered read needs Equal, or both From and To.", nameof(query));
            return new Filter
            {
                Column = column,
                Low = query.From,
                High = query.To,
                _low = SchemaKeyCodec.Encode(column, query.From),
                _high = SchemaKeyCodec.Encode(column, query.To),
            };
        }

        public bool Matches(object row)
        {
            if (Column is null)
                return true;
            var value = SchemaKeyCodec.Encode(Column, Column.GetValue(row)!);
            return value.CompareTo(_low) >= 0 && value.CompareTo(_high) <= 0;
        }
    }
}
