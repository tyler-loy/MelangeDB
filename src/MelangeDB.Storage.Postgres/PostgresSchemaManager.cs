using System.Text;
using MelangeDB.Core;
using Npgsql;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// Thrown when the Postgres schema does not match the declared tables and the applier is not
/// allowed to fix it: <c>Postgres:AutoMigrate</c> is off, or the fix would be destructive.
/// Carries the exact DDL that would reconcile the additive part, so the operator can review and
/// run it deliberately — which is the whole point of the gate.
/// </summary>
public sealed class PostgresMigrationRefusedException : Exception
{
    public PostgresMigrationRefusedException(string message, string ddl)
        : base(message)
        => Ddl = ddl;

    /// <summary>The DDL script that would apply the additive changes.</summary>
    public string Ddl { get; }
}

/// <summary>
/// Schema generation and migration for relational-backplane tables, driven by the same
/// <see cref="TableSchema"/> the hot store uses — one definition serves both tiers. The contract
/// (DESIGN.md §10, settled in phase 08): creating missing tables and <em>adding</em> missing
/// columns is automatic under <c>Postgres:AutoMigrate</c>; anything destructive — a changed column
/// type, a dropped column, a narrowed constraint — is refused loudly in every setting and stays a
/// manual, deliberate migration. Added NOT NULL columns backfill existing rows with the kind's
/// zero value, so an additive migration never drops or nulls data. Columns present in Postgres
/// but absent from the schema are left untouched.
/// </summary>
internal sealed class PostgresSchemaManager
{
    /// <summary>The applier checkpoint's table name, inside the configured schema.</summary>
    public const string CheckpointTable = "__melange_applier";

    /// <summary>
    /// The tables this projection holds a complete copy of, inside the configured schema. A
    /// backplane table missing from it is new to the projection — declared since the checkpoint
    /// was anchored — and is copied from the hot store before the applier resumes.
    /// </summary>
    public const string ProjectedTable = "__melange_projected";

    /// <summary>
    /// Where an archive insert whose primary key Postgres already holds is set aside, in the same
    /// transaction as the batch and its checkpoint — so nothing is dropped and nothing overwritten.
    /// </summary>
    public const string DeadLetterTable = "__melange_archive_dead_letter";

    /// <summary>The log position an archive row was committed at, so a restore can trim the archive back to its history.</summary>
    public const string LsnColumn = "_melange_lsn";

    /// <summary>The log epoch an archive row was committed under; LSNs mean nothing across epochs.</summary>
    public const string EpochColumn = "_melange_epoch";

    private readonly string _schema;

    public PostgresSchemaManager(string schema) => _schema = schema;

    /// <summary>
    /// Ensures the schema namespace and the checkpoint table exist (always — they are the tier's
    /// own plumbing, not a user schema change), then creates or validates every relational table
    /// per the migration contract. Returns the DDL that was applied, empty when the schema already
    /// matched — the caller logs it, because an automatic schema change should never be silent.
    /// </summary>
    public async Task<string> EnsureAsync(NpgsqlConnection connection, IReadOnlyList<TableSchema> tables, bool autoMigrate, CancellationToken ct)
    {
        await ExecuteAsync(connection, $"CREATE SCHEMA IF NOT EXISTS {PostgresIdentifier.Quote(_schema)}", ct).ConfigureAwait(false);
        await ExecuteAsync(
            connection,
            $"""
            CREATE TABLE IF NOT EXISTS {PostgresIdentifier.Qualify(_schema, CheckpointTable)} (
                "applier" text PRIMARY KEY,
                "applied_lsn" bigint NOT NULL,
                "log_epoch" uuid NOT NULL,
                "updated_at" timestamp with time zone NOT NULL
            )
            """,
            ct).ConfigureAwait(false);
        await ExecuteAsync(
            connection,
            $"""
            CREATE TABLE IF NOT EXISTS {PostgresIdentifier.Qualify(_schema, ProjectedTable)} (
                "table_name" text PRIMARY KEY,
                "since_lsn" bigint NOT NULL,
                "log_epoch" uuid NOT NULL,
                "projected_at" timestamp with time zone NOT NULL
            )
            """,
            ct).ConfigureAwait(false);
        if (tables.Any(static t => t.Tier == StorageTier.Archive))
        {
            await ExecuteAsync(
                connection,
                $"""
                CREATE TABLE IF NOT EXISTS {PostgresIdentifier.Qualify(_schema, DeadLetterTable)} (
                    "id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    "table_name" text NOT NULL,
                    "row_key" text NOT NULL,
                    "lsn" bigint NOT NULL,
                    "log_epoch" uuid NOT NULL,
                    "reducer" text,
                    "row_values" jsonb NOT NULL,
                    "dead_lettered_at" timestamp with time zone NOT NULL
                )
                """,
                ct).ConfigureAwait(false);
        }

        var pendingDdl = new StringBuilder();
        var appliedDdl = new StringBuilder();
        var refusals = new List<string>();

        async Task ApplyOrCollectAsync(string ddl)
        {
            if (autoMigrate)
            {
                await ExecuteScriptAsync(connection, ddl, ct).ConfigureAwait(false);
                appliedDdl.AppendLine(ddl);
            }
            else
            {
                pendingDdl.AppendLine(ddl);
            }
        }

        foreach (var table in tables)
        {
            var existing = await ExistingColumnsAsync(connection, table.Name, ct).ConfigureAwait(false);
            if (existing.Count == 0)
            {
                await ApplyOrCollectAsync(CreateTableDdl(table)).ConfigureAwait(false);
                continue;
            }

            foreach (var column in table.Columns)
            {
                if (existing.TryGetValue(column.Name, out var actualType))
                {
                    var expected = PostgresTypeMap.SqlType(column);
                    if (!TypeMatches(expected, actualType))
                    {
                        refusals.Add(
                            $"{table.Name}.{column.Name}: declared {expected}, database has {actualType}. " +
                            "Changing a column's type is destructive and is never automatic.");
                    }

                    continue;
                }

                await ApplyOrCollectAsync(AddColumnDdl(table, column)).ConfigureAwait(false);
            }

            if (table.Tier == StorageTier.Archive && !existing.ContainsKey(LsnColumn))
                await ApplyOrCollectAsync(ArchiveColumnsDdl(table)).ConfigureAwait(false);

            if (autoMigrate)
                await ExecuteScriptAsync(connection, IndexDdl(table), ct).ConfigureAwait(false);
        }

        if (refusals.Count > 0)
        {
            throw new PostgresMigrationRefusedException(
                "The Postgres schema disagrees destructively with the declared tables; migrate it manually: "
                + string.Join(" ", refusals),
                pendingDdl.ToString());
        }

        if (pendingDdl.Length > 0)
        {
            throw new PostgresMigrationRefusedException(
                "The Postgres schema is missing tables or columns and Postgres:AutoMigrate is off. "
                + "Run the DDL below (or enable AutoMigrate) — schema changes against production should be deliberate.",
                pendingDdl.ToString());
        }

        return appliedDdl.ToString();
    }

    /// <summary>The full CREATE TABLE (plus indexes) for one relational table.</summary>
    public string CreateTableDdl(TableSchema table)
    {
        var builder = new StringBuilder();
        builder.Append($"CREATE TABLE IF NOT EXISTS {PostgresIdentifier.Qualify(_schema, table.Name)} (");
        var first = true;
        foreach (var column in table.Columns)
        {
            if (!first)
                builder.Append(',');
            first = false;
            builder.Append($"\n    {PostgresIdentifier.Quote(column.Name)} {PostgresTypeMap.SqlType(column)}");
            if (!PostgresTypeMap.IsNullable(column))
                builder.Append(" NOT NULL");
            if (column.IsPrimaryKey)
                builder.Append(" PRIMARY KEY");
        }

        if (table.Tier == StorageTier.Archive)
        {
            builder.Append($",\n    {PostgresIdentifier.Quote(LsnColumn)} bigint NOT NULL");
            builder.Append($",\n    {PostgresIdentifier.Quote(EpochColumn)} uuid NOT NULL");
        }

        builder.Append("\n);");
        var indexes = IndexDdl(table);
        if (indexes.Length > 0)
            builder.Append('\n').Append(indexes);
        return builder.ToString();
    }

    private string AddColumnDdl(TableSchema table, ColumnSchema column)
    {
        var builder = new StringBuilder(
            $"ALTER TABLE {PostgresIdentifier.Qualify(_schema, table.Name)} ADD COLUMN IF NOT EXISTS " +
            $"{PostgresIdentifier.Quote(column.Name)} {PostgresTypeMap.SqlType(column)}");
        if (!PostgresTypeMap.IsNullable(column))
            builder.Append($" NOT NULL DEFAULT {PostgresTypeMap.ZeroLiteral(column)}");
        builder.Append(';');
        return builder.ToString();
    }

    /// <summary>The archive's hidden columns, for an archive table created before they existed.</summary>
    private string ArchiveColumnsDdl(TableSchema table) =>
        $"ALTER TABLE {PostgresIdentifier.Qualify(_schema, table.Name)} " +
        $"ADD COLUMN IF NOT EXISTS {PostgresIdentifier.Quote(LsnColumn)} bigint NOT NULL DEFAULT 0, " +
        $"ADD COLUMN IF NOT EXISTS {PostgresIdentifier.Quote(EpochColumn)} uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';";

    private string IndexDdl(TableSchema table)
    {
        var builder = new StringBuilder();
        if (table.Tier == StorageTier.Archive)
        {
            builder.Append(
                $"CREATE INDEX IF NOT EXISTS {PostgresIdentifier.Quote($"ix_{table.Name}_{LsnColumn}")} " +
                $"ON {PostgresIdentifier.Qualify(_schema, table.Name)} ({PostgresIdentifier.Quote(EpochColumn)}, {PostgresIdentifier.Quote(LsnColumn)});\n");
        }

        foreach (var index in table.Indexes)
        {
            var name = $"{(index.Unique ? "ux" : "ix")}_{table.Name}_{index.Column}";
            builder.Append(
                $"CREATE {(index.Unique ? "UNIQUE " : string.Empty)}INDEX IF NOT EXISTS {PostgresIdentifier.Quote(name)} " +
                $"ON {PostgresIdentifier.Qualify(_schema, table.Name)} ({PostgresIdentifier.Quote(index.Column)});\n");
        }

        return builder.ToString().TrimEnd('\n');
    }

    /// <summary>Which of <paramref name="tables"/> exist in the configured schema right now.</summary>
    public async Task<HashSet<string>> ExistingTablesAsync(NpgsqlConnection connection, IEnumerable<string> tables, CancellationToken ct)
    {
        var existing = new HashSet<string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = ANY($2)",
            connection);
        command.Parameters.AddWithValue(_schema);
        command.Parameters.AddWithValue(tables.ToArray());
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            existing.Add(reader.GetString(0));
        return existing;
    }

    private async Task<Dictionary<string, string>> ExistingColumnsAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        var columns = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
            connection);
        command.Parameters.AddWithValue(_schema);
        command.Parameters.AddWithValue(table);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            columns[reader.GetString(0)] = reader.GetString(1);
        return columns;
    }

    private static bool TypeMatches(string expected, string actual) => expected switch
    {
        "numeric(20,0)" => actual.Equals("numeric", StringComparison.OrdinalIgnoreCase),
        _ => actual.Equals(expected, StringComparison.OrdinalIgnoreCase),
    };

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task ExecuteScriptAsync(NpgsqlConnection connection, string script, CancellationToken ct)
    {
        foreach (var statement in script.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (statement.Length > 0)
                await ExecuteAsync(connection, statement, ct).ConfigureAwait(false);
        }
    }
}
