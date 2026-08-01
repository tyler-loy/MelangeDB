namespace MelangeDB.Client;

/// <summary>
/// Thrown when a wire row does not match the schema the bindings were generated from — a missing
/// column or a value of the wrong wire kind. This is the loud form of schema drift; compare the
/// bindings' schema hash against the server module's manifest to find the stale side.
/// </summary>
public sealed class MelangeSchemaMismatchException : Exception
{
    public MelangeSchemaMismatchException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The one coercion table from wire column values to CLR values. MessagePack decoding is lossy by
/// design — every integer surfaces as <see cref="long"/> (<see cref="ulong"/> only beyond
/// <see cref="long.MaxValue"/>), <see cref="Identity"/> as 32 raw bytes, <see cref="Timestamp"/>
/// as its microsecond count — and this class is the single place that knowledge lives. Generated
/// row codecs call these methods per column; nothing else in a typed client converts a wire value.
/// Every method throws <see cref="MelangeSchemaMismatchException"/> rather than guessing: a
/// missing column or an unexpected wire kind is schema drift, not data.
/// </summary>
public static class ClientWireValues
{
    public static bool ReadBool(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            bool value => value,
            var value => throw WrongKind(column, table, "Bool", value),
        };

    public static sbyte ReadInt8(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        (sbyte)ReadIntegerInRange(columns, column, table, sbyte.MinValue, sbyte.MaxValue, "Int8");

    public static byte ReadUInt8(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        (byte)ReadIntegerInRange(columns, column, table, byte.MinValue, byte.MaxValue, "UInt8");

    public static short ReadInt16(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        (short)ReadIntegerInRange(columns, column, table, short.MinValue, short.MaxValue, "Int16");

    public static ushort ReadUInt16(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        (ushort)ReadIntegerInRange(columns, column, table, ushort.MinValue, ushort.MaxValue, "UInt16");

    public static int ReadInt32(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        (int)ReadIntegerInRange(columns, column, table, int.MinValue, int.MaxValue, "Int32");

    public static uint ReadUInt32(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        (uint)ReadIntegerInRange(columns, column, table, uint.MinValue, uint.MaxValue, "UInt32");

    public static long ReadInt64(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        ReadIntegerInRange(columns, column, table, long.MinValue, long.MaxValue, "Int64");

    public static ulong ReadUInt64(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            ulong value => value,
            long value and >= 0 => (ulong)value,
            var value => throw WrongKind(column, table, "UInt64", value),
        };

    public static float ReadFloat32(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            float value => value,
            var value => throw WrongKind(column, table, "Float32", value),
        };

    public static double ReadFloat64(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            double value => value,
            var value => throw WrongKind(column, table, "Float64", value),
        };

    public static string? ReadString(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            null => null,
            string value => value,
            var value => throw WrongKind(column, table, "String", value),
        };

    public static byte[]? ReadBytes(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            null => null,
            byte[] value => value,
            var value => throw WrongKind(column, table, "Bytes", value),
        };

    public static Identity ReadIdentity(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            byte[] { Length: Identity.Size } value => new Identity(value),
            var value => throw WrongKind(column, table, $"Identity ({Identity.Size} bytes)", value),
        };

    public static Timestamp ReadTimestamp(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        Get(columns, column, table) switch
        {
            long value => new Timestamp(value),
            var value => throw WrongKind(column, table, "Timestamp", value),
        };

    private static long ReadIntegerInRange(
        IReadOnlyDictionary<string, object?> columns,
        string column,
        string table,
        long min,
        long max,
        string declared)
    {
        var value = Get(columns, column, table);
        if (value is not long integer)
            throw WrongKind(column, table, declared, value);
        if (integer < min || integer > max)
        {
            throw new MelangeSchemaMismatchException(
                $"Column '{table}.{column}' carries {integer}, out of range for the declared {declared} — the bindings were generated from a different schema.");
        }

        return integer;
    }

    private static object? Get(IReadOnlyDictionary<string, object?> columns, string column, string table) =>
        columns.TryGetValue(column, out var value)
            ? value
            : throw new MelangeSchemaMismatchException(
                $"Row for table '{table}' carries no column '{column}' — the bindings were generated from a different schema than the server is running.");

    private static MelangeSchemaMismatchException WrongKind(string column, string table, string declared, object? value) =>
        new($"Column '{table}.{column}' carries a {value?.GetType().Name ?? "null"} where the bindings expect {declared} — the bindings were generated from a different schema than the server is running.");
}
