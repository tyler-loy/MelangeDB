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
/// Schema generation and migration for relational-tier tables, driven by the same
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

    private readonly string _schema;

    public PostgresSchemaManager(string schema) => _schema = schema;

    /// <summary>
    /// Ensures the schema namespace and the checkpoint table exist (always — they are the tier's
    /// own plumbing, not a user schema change), then creates or validates every relational table
    /// per the migration contract.
    /// </summary>
    public async Task EnsureAsync(NpgsqlConnection connection, IReadOnlyList<TableSchema> tables, bool autoMigrate, CancellationToken ct)
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

        var pendingDdl = new StringBuilder();
        var refusals = new List<string>();
        foreach (var table in tables)
        {
            var existing = await ExistingColumnsAsync(connection, table.Name, ct).ConfigureAwait(false);
            if (existing.Count == 0)
            {
                var ddl = CreateTableDdl(table);
                if (autoMigrate)
                    await ExecuteScriptAsync(connection, ddl, ct).ConfigureAwait(false);
                else
                    pendingDdl.AppendLine(ddl);
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

                var addDdl = AddColumnDdl(table, column);
                if (autoMigrate)
                    await ExecuteScriptAsync(connection, addDdl, ct).ConfigureAwait(false);
                else
                    pendingDdl.AppendLine(addDdl);
            }

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

    private string IndexDdl(TableSchema table)
    {
        var builder = new StringBuilder();
        foreach (var index in table.Indexes)
        {
            var name = $"{(index.Unique ? "ux" : "ix")}_{table.Name}_{index.Column}";
            builder.Append(
                $"CREATE {(index.Unique ? "UNIQUE " : string.Empty)}INDEX IF NOT EXISTS {PostgresIdentifier.Quote(name)} " +
                $"ON {PostgresIdentifier.Qualify(_schema, table.Name)} ({PostgresIdentifier.Quote(index.Column)});\n");
        }

        return builder.ToString().TrimEnd('\n');
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
