using MelangeDB.Core;
using Npgsql;
using NpgsqlTypes;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// The one place a <see cref="ColumnKind"/> maps to a Postgres type and a row value maps to a
/// parameter. Unsigned kinds widen so their full range fits a signed SQL type; <c>UInt64</c> maps
/// to <c>numeric(20,0)</c>, the only lossless home for its upper half. <c>Timestamp</c> maps to
/// <c>timestamptz</c>, whose microsecond precision matches exactly — which is what makes
/// <c>date_trunc</c> bucketing work on the Postgres side. Enums store as their underlying integer,
/// same as the row format.
/// </summary>
internal static class PostgresTypeMap
{
    /// <summary>The zero-value for a ScheduleAt column stored in its text form.</summary>
    private const string ScheduleAtZero = "at:0";

    public static string SqlType(ColumnSchema column) => column.Kind switch
    {
        ColumnKind.Bool => "boolean",
        ColumnKind.Int8 or ColumnKind.UInt8 or ColumnKind.Int16 => "smallint",
        ColumnKind.UInt16 or ColumnKind.Int32 => "integer",
        ColumnKind.UInt32 or ColumnKind.Int64 => "bigint",
        ColumnKind.UInt64 => "numeric(20,0)",
        ColumnKind.Float32 => "real",
        ColumnKind.Float64 => "double precision",
        ColumnKind.String => "text",
        ColumnKind.Bytes or ColumnKind.Identity => "bytea",
        ColumnKind.Timestamp => "timestamp with time zone",
        ColumnKind.ScheduleAt => "text",
        _ => throw new NotSupportedException($"Unknown column kind {column.Kind}."),
    };

    /// <summary>Whether the Postgres column allows NULL — only kinds whose CLR form is nullable.</summary>
    public static bool IsNullable(ColumnSchema column) => column.Kind is ColumnKind.String or ColumnKind.Bytes;

    /// <summary>The SQL literal a NOT NULL column added by migration defaults existing rows to.</summary>
    public static string ZeroLiteral(ColumnSchema column) => column.Kind switch
    {
        ColumnKind.Bool => "false",
        ColumnKind.Int8 or ColumnKind.UInt8 or ColumnKind.Int16 or ColumnKind.UInt16 or ColumnKind.Int32
            or ColumnKind.UInt32 or ColumnKind.Int64 or ColumnKind.UInt64 => "0",
        ColumnKind.Float32 or ColumnKind.Float64 => "0",
        ColumnKind.Identity => $"decode('{new string('0', Identity.Size * 2)}', 'hex')",
        ColumnKind.Timestamp => "'epoch'::timestamptz",
        ColumnKind.ScheduleAt => $"'{ScheduleAtZero}'",
        _ => throw new NotSupportedException($"Column kind {column.Kind} has no zero literal."),
    };

    /// <summary>The explicit Npgsql parameter type — set on every parameter so a NULL still types.</summary>
    public static NpgsqlDbType DbType(ColumnSchema column) => column.Kind switch
    {
        ColumnKind.Bool => NpgsqlDbType.Boolean,
        ColumnKind.Int8 or ColumnKind.UInt8 or ColumnKind.Int16 => NpgsqlDbType.Smallint,
        ColumnKind.UInt16 or ColumnKind.Int32 => NpgsqlDbType.Integer,
        ColumnKind.UInt32 or ColumnKind.Int64 => NpgsqlDbType.Bigint,
        ColumnKind.UInt64 => NpgsqlDbType.Numeric,
        ColumnKind.Float32 => NpgsqlDbType.Real,
        ColumnKind.Float64 => NpgsqlDbType.Double,
        ColumnKind.String or ColumnKind.ScheduleAt => NpgsqlDbType.Text,
        ColumnKind.Bytes or ColumnKind.Identity => NpgsqlDbType.Bytea,
        ColumnKind.Timestamp => NpgsqlDbType.TimestampTz,
        _ => throw new NotSupportedException($"Unknown column kind {column.Kind}."),
    };

    /// <summary>Builds a fully typed parameter for a boxed row value.</summary>
    public static NpgsqlParameter Parameter(ColumnSchema column, object? value) => new()
    {
        NpgsqlDbType = DbType(column),
        Value = ToParameter(column, value),
    };

    /// <summary>Converts a boxed row value to its Npgsql parameter form; null becomes DBNull.</summary>
    public static object ToParameter(ColumnSchema column, object? value)
    {
        if (value is null)
            return DBNull.Value;
        if (column.IsEnum)
            value = Convert.ChangeType(value, Enum.GetUnderlyingType(column.ClrType));
        return column.Kind switch
        {
            ColumnKind.Bool => value,
            ColumnKind.Int8 => (short)(sbyte)value,
            ColumnKind.UInt8 => (short)(byte)value,
            ColumnKind.Int16 => value,
            ColumnKind.UInt16 => (int)(ushort)value,
            ColumnKind.Int32 => value,
            ColumnKind.UInt32 => (long)(uint)value,
            ColumnKind.Int64 => value,
            ColumnKind.UInt64 => (decimal)(ulong)value,
            ColumnKind.Float32 or ColumnKind.Float64 or ColumnKind.String or ColumnKind.Bytes => value,
            ColumnKind.Identity => ((Identity)value).ToByteArray(),
            ColumnKind.Timestamp => ((Timestamp)value).ToDateTimeOffset().UtcDateTime,
            ColumnKind.ScheduleAt => Format((ScheduleAt)value),
            _ => throw new NotSupportedException($"Unknown column kind {column.Kind}."),
        };
    }

    /// <summary>
    /// Canonicalizes a value read back from Postgres for the ad-hoc result path: timestamps come
    /// back as <see cref="Timestamp"/>, identity columns as <see cref="Identity"/>. Aggregate
    /// outputs whose SQL type widened (COUNT's bigint, SUM's numeric) pass through as read.
    /// </summary>
    public static object? FromReader(object value, ColumnSchema? column)
    {
        if (value is DBNull)
            return null;
        if (value is DateTime dateTime)
            return Timestamp.FromDateTimeOffset(new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)));
        if (column is { Kind: ColumnKind.Identity } && value is byte[] bytes)
            return new Identity(bytes);
        if (column is { IsEnum: true })
            return Enum.ToObject(column.ClrType, value);
        return value;
    }

    private static string Format(ScheduleAt schedule) =>
        schedule.IsInterval ? $"interval:{schedule.Microseconds}" : $"at:{schedule.Microseconds}";
}
