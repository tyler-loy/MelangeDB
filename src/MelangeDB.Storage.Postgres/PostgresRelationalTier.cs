using System.Diagnostics;
using MelangeDB.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// The relational tier's applier: consumes the commit log and projects relational-tier rows into
/// Postgres, batched per <c>Postgres:ApplyBatchSize</c> with its <b>own durable LSN checkpoint
/// stored in Postgres itself</b> — the checkpoint row commits in the same transaction as the
/// batch, so a resume after any failure is gap-free and duplicate-free by construction. That
/// per-applier checkpoint is the property that lets two storage backends share one commit point
/// with no 2PC.
/// <para>
/// It runs on its own dispatch loop, off the commit path — the event bus's shape: the commit
/// observer only signals, the log is the buffer, and the checkpoint models the lag. Postgres down
/// is therefore <em>not</em> server down: writes continue, subscriptions are unaffected, the lag
/// grows visibly (<c>melange.applier.lag</c>, the <c>melange-applier</c> health check, EventId
/// 1601), and reconnection catches up cleanly. Registered as a decoupled applier so log
/// truncation can never pass its checkpoint.
/// </para>
/// </summary>
public sealed class PostgresRelationalTier : ILogApplier, ICommitObserver, IHostedService, IAsyncDisposable
{
    private const string ApplierName = "postgres";
    private static readonly ActivitySource Source = new("MelangeDB");
    private static readonly TimeSpan StallRelogInterval = TimeSpan.FromSeconds(30);

    private readonly MelangeEngine _engine;
    private readonly PostgresConnectionSource _connections;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _signal = new(0, 1);
    private readonly Lock _waiterLock = new();
    private readonly List<(ulong Lsn, TaskCompletionSource Completion)> _waiters = [];
    private readonly Dictionary<TableId, TableSql> _tableSql = [];
    private IReadOnlyList<TableSchema> _relationalTables = [];
    private PostgresSchemaManager? _schemaManager;
    private Task? _loop;
    private ulong _appliedLsn;
    private bool _initialized;
    private bool _stalled;
    private long _lastStallLogTicks;
    private volatile bool _stopped;

    public PostgresRelationalTier(
        MelangeEngine engine,
        PostgresConnectionSource connections,
        IOptionsMonitor<MelangeDbOptions> options,
        ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(connections);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _connections = connections;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<PostgresRelationalTier>();
    }

    /// <inheritdoc />
    public string Name => ApplierName;

    /// <inheritdoc />
    public ulong AppliedLsn => Volatile.Read(ref _appliedLsn);

    /// <summary>Whether the applier is currently unable to reach or apply to Postgres.</summary>
    public bool IsStalled => Volatile.Read(ref _stalled);

    /// <summary>
    /// Not supported: this applier advances on its own dispatch loop, transactionally with its
    /// Postgres-side checkpoint. Driving it record-by-record from the pipeline would put Postgres
    /// on the commit path, which is exactly what the design forbids.
    /// </summary>
    void ILogApplier.Apply(CommitRecord record) =>
        throw new NotSupportedException("The postgres applier advances on its own dispatch loop, off the commit path.");

    /// <summary>The commit observer: signal only — no I/O and no user code under the write lock.</summary>
    public void OnCommit(CommitRecord record)
    {
        if (_signal.CurrentCount == 0)
        {
            try
            {
                _signal.Release();
            }
            catch (SemaphoreFullException)
            {
                // A concurrent release already woke the loop; one pending wake is enough.
            }
        }
    }

    /// <summary>
    /// Completes when the tier's checkpoint reaches <paramref name="lsn"/> — the narrow primitive
    /// for cross-tier read-after-write flows ("the row I just committed must be visible to SQL").
    /// An honest wait: it can take as long as Postgres is down, so pass a token or a timeout via
    /// <see cref="Task.WaitAsync(TimeSpan)"/>. Most flows should not use this — the lag is the
    /// design, and the hot store already serves read-your-writes.
    /// </summary>
    public Task WaitForAppliedAsync(ulong lsn, CancellationToken cancellationToken = default)
    {
        if (AppliedLsn >= lsn)
            return Task.CompletedTask;
        TaskCompletionSource completion;
        lock (_waiterLock)
        {
            if (AppliedLsn >= lsn)
                return Task.CompletedTask;
            completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((lsn, completion));
        }

        return cancellationToken.CanBeCanceled
            ? completion.Task.WaitAsync(cancellationToken)
            : completion.Task;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _relationalTables = _engine.Schema.Tables.Where(t => t.Tier == StorageTier.Relational).ToList();
        _schemaManager = new PostgresSchemaManager(_options.CurrentValue.Postgres.Schema);
        foreach (var table in _relationalTables)
            _tableSql[table.Id] = TableSql.For(table, _options.CurrentValue.Postgres.Schema);

        _engine.Appliers.RegisterDecoupled(this);
        _engine.AddCommitObserver(this);
        _loop = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stopped)
            return;
        _stopped = true;
        _cts.Cancel();
        if (_loop is { } loop)
        {
            try
            {
                await loop.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
            catch (OperationCanceledException)
            {
            }
        }

        lock (_waiterLock)
        {
            foreach (var (_, completion) in _waiters)
                completion.TrySetCanceled();
            _waiters.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _cts.Dispose();
        _signal.Dispose();
    }

    private async Task RunAsync(CancellationToken ct)
    {
        var failures = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!_initialized)
                {
                    await InitializeAsync(ct).ConfigureAwait(false);
                    _initialized = true;
                }

                await ApplyAvailableAsync(ct).ConfigureAwait(false);
                if (_stalled)
                {
                    Volatile.Write(ref _stalled, false);
                    LogMessages.Recovered(_logger, AppliedLsn, failures);
                }

                failures = 0;
                await _signal.WaitAsync(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                failures++;
                ReportStall(exception, failures);
                var backoff = TimeSpan.FromMilliseconds(Math.Min(500L << Math.Min(failures - 1, 5), 15_000));
                try
                {
                    await Task.Delay(backoff, _time, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// First successful contact: ensure or validate the schema, then anchor the checkpoint. A
    /// fresh checkpoint against an untruncated log replays from the start; against a truncated log
    /// the early records are gone, so the tier bootstraps from the hot store's current rows at a
    /// consistent LSN instead — both paths converge to the same projection. A checkpoint from
    /// another log epoch is meaningless (LSNs only count within one log) and stalls loudly rather
    /// than guessing.
    /// </summary>
    private async Task InitializeAsync(CancellationToken ct)
    {
        var options = _options.CurrentValue.Postgres;
        await using var connection = await _connections.DataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        var appliedDdl = await _schemaManager!.EnsureAsync(connection, _relationalTables, options.AutoMigrate, ct).ConfigureAwait(false);
        if (appliedDdl.Length > 0)
            LogMessages.SchemaMigrated(_logger, appliedDdl);

        var epoch = _engine.Log.EpochId;
        var (checkpointLsn, checkpointEpoch) = await ReadCheckpointAsync(connection, ct).ConfigureAwait(false);
        if (checkpointEpoch is null || (checkpointEpoch != epoch && checkpointLsn == 0))
        {
            // No checkpoint, or a foreign-epoch one that never applied anything — either way the
            // projection has no valid place in this log, and the anchoring must be the same:
            // replay from the start when the log still has its start, bootstrap from the hot
            // store when truncation removed it. Anchoring a truncated log at 0 would silently
            // skip records 1..BaseLsn, because ReadFrom serves permissively from the base.
            await AnchorFreshAsync(connection, epoch, ct).ConfigureAwait(false);
            return;
        }

        if (checkpointEpoch != epoch)
            throw new PostgresEpochMismatchException(checkpointEpoch.Value, epoch, checkpointLsn);

        // Same epoch, checkpoint past the head: the relational tier holds a future the log no
        // longer contains. A restore cannot produce this (it always mints a new epoch, which is
        // the mismatch above); this is the manual version — a data directory swapped for an older
        // copy with its epoch kept — and it is refused just as loudly.
        if (checkpointLsn > _engine.Log.HeadLsn)
            throw new PostgresCheckpointAheadException(checkpointLsn, _engine.Log.HeadLsn);

        if (checkpointLsn < _engine.Log.BaseLsn)
        {
            throw new InvalidOperationException(
                $"The commit log was truncated up to LSN {_engine.Log.BaseLsn}, past the postgres applier's checkpoint " +
                $"at LSN {checkpointLsn}. Truncation floors should make this impossible; the log directory was likely " +
                "replaced. Clear the Postgres schema to re-bootstrap, or restore the matching log.");
        }

        Volatile.Write(ref _appliedLsn, checkpointLsn);
        SignalWaiters(checkpointLsn);
    }

    /// <summary>
    /// Anchors a projection that has no valid checkpoint in this log: replay from the start when
    /// the log is untruncated, bootstrap from the hot store when it is not.
    /// </summary>
    private async Task AnchorFreshAsync(NpgsqlConnection connection, Guid epoch, CancellationToken ct)
    {
        if (_engine.Log.BaseLsn > 0)
        {
            await BootstrapFromStoreAsync(connection, epoch, ct).ConfigureAwait(false);
        }
        else
        {
            await WriteCheckpointAsync(connection, transaction: null, 0, epoch, ct).ConfigureAwait(false);
            Volatile.Write(ref _appliedLsn, 0);
        }
    }

    /// <summary>
    /// Applies every available record in batches: one Postgres transaction per batch, the
    /// checkpoint row updated inside it. Records touching no relational table still advance the
    /// checkpoint — a batch of them is just a checkpoint write.
    /// </summary>
    private async Task ApplyAvailableAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var head = _engine.Log.HeadLsn;
            var applied = AppliedLsn;
            if (applied >= head)
                return;

            // ReadFrom is permissive below the truncation base — it would silently serve from
            // BaseLsn + 1 and this loop would checkpoint right past the gap. Registration as an
            // applier floors truncation at our checkpoint, so this cannot happen in-process; the
            // guard turns any future violation of that invariant into a loud stall instead of a
            // silent hole in the projection.
            if (applied < _engine.Log.BaseLsn)
            {
                throw new InvalidOperationException(
                    $"The commit log was truncated up to LSN {_engine.Log.BaseLsn}, past the postgres applier's " +
                    $"checkpoint at LSN {applied}; applying from here would skip records. Clear the Postgres schema " +
                    "to re-bootstrap, or restore the matching log.");
            }

            var batchSize = Math.Max(1, _options.CurrentValue.Postgres.ApplyBatchSize);
            var batch = new List<CommitRecord>(batchSize);
            foreach (var record in _engine.Log.ReadFrom(applied + 1))
            {
                // A checkpoint that lagged across an additive schema migration reads records whose
                // rows carry the old column order; decoding them under the current schema without
                // this re-encode writes plausible garbage to Postgres. The decoupled-applier half
                // of the contract on MelangeEngine.TransformToCurrentShape.
                batch.Add(_engine.TransformToCurrentShape(record));
                if (batch.Count >= batchSize)
                    break;
            }

            if (batch.Count == 0)
                return;

            using var activity = Source.StartActivity("melange.apply");
            activity?.SetTag("melange.applier", ApplierName);

            var last = batch[^1].Lsn;
            await using (var connection = await _connections.DataSource.OpenConnectionAsync(ct).ConfigureAwait(false))
            {
                await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
                await using var commands = new NpgsqlBatch(connection, transaction);
                foreach (var record in batch)
                {
                    foreach (var op in record.WriteSet)
                    {
                        if (_tableSql.TryGetValue(op.Table, out var sql))
                            commands.BatchCommands.Add(sql.Command(op));
                    }
                }

                if (commands.BatchCommands.Count > 0)
                    await commands.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                await WriteCheckpointAsync(connection, transaction, last, _engine.Log.EpochId, ct).ConfigureAwait(false);
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }

            Volatile.Write(ref _appliedLsn, last);
            SignalWaiters(last);
        }
    }

    /// <summary>
    /// Bootstraps the projection from the hot store when the log alone cannot rebuild it: rows are
    /// captured at one consistent LSN under the engine's read anchor, then written — after existing
    /// projection content is cleared — with the checkpoint, in one Postgres transaction.
    /// </summary>
    private async Task BootstrapFromStoreAsync(NpgsqlConnection connection, Guid epoch, CancellationToken ct)
    {
        List<(TableId Table, byte[] Row)> rows = [];
        var anchor = _engine.ReadConsistent(head =>
        {
            foreach (var table in _relationalTables)
            {
                foreach (var pair in _engine.HotStore.Scan(table.Id))
                    rows.Add((table.Id, pair.Value.ToArray()));
            }

            return head;
        });

        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        var schema = _options.CurrentValue.Postgres.Schema;
        foreach (var table in _relationalTables)
        {
            await using var delete = new NpgsqlCommand($"DELETE FROM {PostgresIdentifier.Qualify(schema, table.Name)}", connection, transaction);
            await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        if (rows.Count > 0)
        {
            await using var commands = new NpgsqlBatch(connection, transaction);
            foreach (var (table, row) in rows)
                commands.BatchCommands.Add(_tableSql[table].Upsert(row));
            await commands.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await WriteCheckpointAsync(connection, transaction, anchor, epoch, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        // Log before publishing the checkpoint: anyone woken by WaitForAppliedAsync must be able
        // to observe that the anchor came from a bootstrap, not replay.
        LogMessages.Bootstrapped(_logger, rows.Count, anchor);
        Volatile.Write(ref _appliedLsn, anchor);
        SignalWaiters(anchor);
    }

    private async Task<(ulong Lsn, Guid? Epoch)> ReadCheckpointAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var table = PostgresIdentifier.Qualify(_options.CurrentValue.Postgres.Schema, PostgresSchemaManager.CheckpointTable);
        await using var command = new NpgsqlCommand($"SELECT \"applied_lsn\", \"log_epoch\" FROM {table} WHERE \"applier\" = $1", connection);
        command.Parameters.AddWithValue(ApplierName);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return (0, null);
        return ((ulong)reader.GetInt64(0), reader.GetGuid(1));
    }

    private async Task WriteCheckpointAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, ulong lsn, Guid epoch, CancellationToken ct)
    {
        var table = PostgresIdentifier.Qualify(_options.CurrentValue.Postgres.Schema, PostgresSchemaManager.CheckpointTable);
        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {table} ("applier", "applied_lsn", "log_epoch", "updated_at") VALUES ($1, $2, $3, now())
            ON CONFLICT ("applier") DO UPDATE SET
                "applied_lsn" = EXCLUDED."applied_lsn",
                "log_epoch" = EXCLUDED."log_epoch",
                "updated_at" = EXCLUDED."updated_at"
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue(ApplierName);
        command.Parameters.AddWithValue((long)lsn);
        command.Parameters.AddWithValue(epoch);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private void SignalWaiters(ulong applied)
    {
        lock (_waiterLock)
        {
            for (var i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Lsn <= applied)
                {
                    _waiters[i].Completion.TrySetResult();
                    _waiters.RemoveAt(i);
                }
            }
        }
    }

    /// <summary>
    /// The loud half of the stall contract: the first failure always logs; while the stall lasts,
    /// the growing lag is re-logged every 30 seconds under <c>Diagnostics:ReportApplierLag</c>.
    /// Migration refusals and epoch mismatches get their own EventIds — they need an operator, not
    /// a retry, though the loop retries anyway so a manual fix recovers without a restart.
    /// </summary>
    private void ReportStall(Exception exception, int failures)
    {
        var wasStalled = Volatile.Read(ref _stalled);
        Volatile.Write(ref _stalled, true);
        var now = _time.GetTimestamp();
        if (wasStalled)
        {
            if (!_options.CurrentValue.Diagnostics.ReportApplierLag)
                return;
            if (_time.GetElapsedTime(Interlocked.Read(ref _lastStallLogTicks), now) < StallRelogInterval)
                return;
        }

        Interlocked.Exchange(ref _lastStallLogTicks, now);
        var lag = (long)(_engine.Log.HeadLsn - Math.Min(_engine.Log.HeadLsn, AppliedLsn));
        switch (exception)
        {
            case PostgresMigrationRefusedException refused:
                LogMessages.MigrationRefused(_logger, refused.Message, refused.Ddl, lag);
                break;
            case PostgresEpochMismatchException mismatch:
                LogMessages.EpochMismatch(_logger, mismatch.CheckpointEpoch, mismatch.LogEpoch, mismatch.CheckpointLsn, lag);
                break;
            case PostgresCheckpointAheadException ahead:
                LogMessages.CheckpointAhead(_logger, ahead.CheckpointLsn, ahead.HeadLsn);
                break;
            default:
                LogMessages.Stalled(_logger, lag, AppliedLsn, failures, exception);
                break;
        }
    }

    /// <summary>Per-table cached SQL: the upsert and delete the applier issues for that table's ops.</summary>
    private sealed class TableSql
    {
        private readonly TableSchema _table;
        private readonly string _upsertSql;
        private readonly string _deleteSql;

        private TableSql(TableSchema table, string upsertSql, string deleteSql)
        {
            _table = table;
            _upsertSql = upsertSql;
            _deleteSql = deleteSql;
        }

        public static TableSql For(TableSchema table, string schema)
        {
            var qualified = PostgresIdentifier.Qualify(schema, table.Name);
            var columns = string.Join(", ", table.Columns.Select(c => PostgresIdentifier.Quote(c.Name)));
            var values = string.Join(", ", table.Columns.Select((_, i) => $"${i + 1}"));
            var updates = table.Columns
                .Where(c => !c.IsPrimaryKey)
                .Select(c => $"{PostgresIdentifier.Quote(c.Name)} = EXCLUDED.{PostgresIdentifier.Quote(c.Name)}")
                .ToList();
            var conflict = updates.Count > 0
                ? $"DO UPDATE SET {string.Join(", ", updates)}"
                : "DO NOTHING";
            var upsert =
                $"INSERT INTO {qualified} ({columns}) VALUES ({values}) " +
                $"ON CONFLICT ({PostgresIdentifier.Quote(table.PrimaryKey.Name)}) {conflict}";
            var delete = $"DELETE FROM {qualified} WHERE {PostgresIdentifier.Quote(table.PrimaryKey.Name)} = $1";
            return new TableSql(table, upsert, delete);
        }

        public NpgsqlBatchCommand Command(in RowOp op) =>
            op.Kind == RowOpKind.Delete ? Delete(op.Key) : Upsert(op.Row);

        public NpgsqlBatchCommand Upsert(ReadOnlyMemory<byte> row)
        {
            var command = new NpgsqlBatchCommand(_upsertSql);
            var boxed = RowSerializer.Deserialize(_table, row);
            foreach (var column in _table.Columns)
                command.Parameters.Add(PostgresTypeMap.Parameter(column, column.GetValue(boxed)));
            return command;
        }

        private NpgsqlBatchCommand Delete(in RowKey key)
        {
            var command = new NpgsqlBatchCommand(_deleteSql);
            command.Parameters.Add(PostgresTypeMap.Parameter(_table.PrimaryKey, SchemaKeyCodec.Decode(_table.PrimaryKey, key)));
            return command;
        }
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, long, ulong, int, Exception?> StalledMessage =
            LoggerMessage.Define<long, ulong, int>(
                LogLevel.Error,
                new EventId(1601, "PostgresApplierStalled"),
                "The postgres applier is stalled {Lag} transaction(s) behind the log head (checkpoint LSN {AppliedLsn}, " +
                "attempt {Failures}). Writes and subscriptions are unaffected; the log holds everything and catch-up is " +
                "automatic on reconnect. Ad-hoc SQL sees the tier as of the checkpoint.");

        public static void Stalled(ILogger logger, long lag, ulong appliedLsn, int failures, Exception failure) =>
            StalledMessage(logger, lag, appliedLsn, failures, failure);

        private static readonly Action<ILogger, ulong, int, Exception?> RecoveredMessage =
            LoggerMessage.Define<ulong, int>(
                LogLevel.Information,
                new EventId(1602, "PostgresApplierRecovered"),
                "The postgres applier recovered after {Failures} failed attempt(s) and is caught up at LSN {AppliedLsn} " +
                "with no gaps and no duplicates — the checkpoint committed with each batch.");

        public static void Recovered(ILogger logger, ulong appliedLsn, int failures) =>
            RecoveredMessage(logger, appliedLsn, failures, null);

        private static readonly Action<ILogger, string, Exception?> SchemaMigratedMessage =
            LoggerMessage.Define<string>(
                LogLevel.Information,
                new EventId(1603, "PostgresSchemaMigrated"),
                "Postgres:AutoMigrate applied additive schema changes — automatic must not mean silent:\n{Ddl}");

        public static void SchemaMigrated(ILogger logger, string ddl) =>
            SchemaMigratedMessage(logger, ddl, null);

        private static readonly Action<ILogger, string, string, long, Exception?> MigrationRefusedMessage =
            LoggerMessage.Define<string, string, long>(
                LogLevel.Error,
                new EventId(1604, "PostgresMigrationRefused"),
                "Relational schema migration refused: {Reason} The applier is stalled {Lag} transaction(s) behind until " +
                "the schema is reconciled; it retries, so a manual fix recovers without a restart. Pending additive DDL:\n{Ddl}");

        public static void MigrationRefused(ILogger logger, string reason, string ddl, long lag) =>
            MigrationRefusedMessage(logger, reason, ddl, lag, null);

        private static readonly Action<ILogger, Guid, Guid, ulong, long, Exception?> EpochMismatchMessage =
            LoggerMessage.Define<Guid, Guid, ulong, long>(
                LogLevel.Error,
                new EventId(1605, "PostgresEpochMismatch"),
                "The Postgres checkpoint belongs to log epoch {CheckpointEpoch} at LSN {CheckpointLsn}, but the current " +
                "log's epoch is {LogEpoch}; LSNs are meaningless across epochs, so the applier is stalled {Lag} " +
                "transaction(s) behind rather than guessing. Clear the Postgres schema to re-bootstrap from current " +
                "state, or restore the log this projection belongs to. After `melange restore` this is the expected " +
                "refusal when the old projection is still present — a restore is a rewind, the relational tier holds " +
                "a future the restored log does not contain, and the clean path is an empty schema, which bootstrap " +
                "refills from the restored log.")
            ;

        public static void EpochMismatch(ILogger logger, Guid checkpointEpoch, Guid logEpoch, ulong checkpointLsn, long lag) =>
            EpochMismatchMessage(logger, checkpointEpoch, logEpoch, checkpointLsn, lag, null);

        private static readonly Action<ILogger, ulong, ulong, Exception?> CheckpointAheadMessage =
            LoggerMessage.Define<ulong, ulong>(
                LogLevel.Error,
                new EventId(1608, "PostgresCheckpointAhead"),
                "The Postgres applier checkpoint (LSN {CheckpointLsn}) is ahead of the log's head ({HeadLsn}): the " +
                "relational tier holds a future this log does not contain. This happens when a data directory is " +
                "replaced by an older copy that kept its epoch. Clear the Postgres schema to re-bootstrap from this " +
                "log, or restore the newer log this projection belongs to.");

        public static void CheckpointAhead(ILogger logger, ulong checkpointLsn, ulong headLsn) =>
            CheckpointAheadMessage(logger, checkpointLsn, headLsn, null);

        private static readonly Action<ILogger, int, ulong, Exception?> BootstrappedMessage =
            LoggerMessage.Define<int, ulong>(
                LogLevel.Information,
                new EventId(1606, "PostgresTierBootstrapped"),
                "The relational tier was bootstrapped from the hot store: {Rows} row(s) captured consistent at LSN {Lsn}. " +
                "The log had been truncated before this applier first ran, so replay alone could not rebuild the projection.");

        public static void Bootstrapped(ILogger logger, int rows, ulong lsn) =>
            BootstrappedMessage(logger, rows, lsn, null);
    }
}

/// <summary>Thrown when the Postgres checkpoint names a different log epoch than the current log.</summary>
/// <summary>
/// The applier's checkpoint sits past the log's head within one epoch: the projection recorded
/// history the log does not hold. Refused loudly (EventId 1608) rather than re-anchored, because
/// destructive disagreement is never automatic — the <c>AutoMigrate</c> posture.
/// </summary>
public sealed class PostgresCheckpointAheadException(ulong checkpointLsn, ulong headLsn)
    : Exception(
        $"The Postgres applier checkpoint (LSN {checkpointLsn}) is ahead of the log's head ({headLsn}): the relational " +
        "tier holds a future this log does not contain. Clear the Postgres schema to re-bootstrap from this log, or " +
        "restore the newer log this projection belongs to.")
{
    public ulong CheckpointLsn { get; } = checkpointLsn;

    public ulong HeadLsn { get; } = headLsn;
}

public sealed class PostgresEpochMismatchException : Exception
{
    public PostgresEpochMismatchException(Guid checkpointEpoch, Guid logEpoch, ulong checkpointLsn)
        : base($"Postgres checkpoint epoch {checkpointEpoch} (LSN {checkpointLsn}) does not match log epoch {logEpoch}.")
    {
        CheckpointEpoch = checkpointEpoch;
        LogEpoch = logEpoch;
        CheckpointLsn = checkpointLsn;
    }

    public Guid CheckpointEpoch { get; }

    public Guid LogEpoch { get; }

    public ulong CheckpointLsn { get; }
}
