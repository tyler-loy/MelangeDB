using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Thrown when a producer's encoded result would pass <c>Producers:MaxResponseBytes</c>. Raised
/// while encoding, so an oversized answer is never built in full.
/// </summary>
public sealed class ProducerResultTooLargeException(int limit)
    : Exception($"The producer's result is larger than Producers:MaxResponseBytes ({limit} bytes). Page it with an argument.")
{
    public int Limit { get; } = limit;
}

/// <summary>
/// Encodes a producer's result: <b>one</b> self-describing tagged value in the reducer-argument
/// encoding (<see cref="ArgsCodec"/>), with no argument-count header. Integers ride as Int64 or
/// UInt64, floats as Float64, enums as their underlying integer, a list or array as an
/// <c>Array</c> of its elements, and a record as an <c>Array</c> of its fields in declaration order
/// — the schema manifest carries the field names, so the client decodes positionally. Null is the
/// <c>Null</c> tag. Generated code drives it; the client's <c>ProducerResultReader</c> mirrors it,
/// and a compatibility test holds the two together.
/// </summary>
public sealed class ProducerResultWriter
{
    private readonly ArrayBufferWriter<byte> _buffer = new(256);
    private readonly int _maxBytes;

    public ProducerResultWriter(int maxBytes = int.MaxValue)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);
        _maxBytes = maxBytes;
    }

    /// <summary>Bytes written so far.</summary>
    public int Length => _buffer.WrittenCount;

    /// <summary>The encoded result.</summary>
    public ReadOnlyMemory<byte> Written => _buffer.WrittenMemory;

    public void WriteNull() => Tag(ArgsCodec.Tag.Null, 0);

    public void WriteBool(bool value) => Tag(ArgsCodec.Tag.Bool, 1)[0] = value ? (byte)1 : (byte)0;

    public void WriteInt64(long value) => BinaryPrimitives.WriteInt64LittleEndian(Tag(ArgsCodec.Tag.Int64, 8), value);

    public void WriteUInt64(ulong value) => BinaryPrimitives.WriteUInt64LittleEndian(Tag(ArgsCodec.Tag.UInt64, 8), value);

    public void WriteFloat64(double value) => BinaryPrimitives.WriteDoubleLittleEndian(Tag(ArgsCodec.Tag.Float64, 8), value);

    public void WriteString(string? value)
    {
        if (value is null)
        {
            WriteNull();
            return;
        }

        var length = Encoding.UTF8.GetByteCount(value);
        var span = Tag(ArgsCodec.Tag.String, 4 + length);
        BinaryPrimitives.WriteInt32LittleEndian(span, length);
        Encoding.UTF8.GetBytes(value, span[4..]);
    }

    public void WriteBytes(byte[]? value)
    {
        if (value is null)
        {
            WriteNull();
            return;
        }

        var span = Tag(ArgsCodec.Tag.Bytes, 4 + value.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span, value.Length);
        value.CopyTo(span[4..]);
    }

    public void WriteIdentity(Identity value) => value.WriteTo(Tag(ArgsCodec.Tag.Identity, Identity.Size));

    public void WriteTimestamp(Timestamp value) =>
        BinaryPrimitives.WriteInt64LittleEndian(Tag(ArgsCodec.Tag.Timestamp, 8), value.UnixTimeMicroseconds);

    /// <summary>Starts a list, an array, or a record: the element (or field) count, then that many values.</summary>
    public void WriteArrayHeader(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        BinaryPrimitives.WriteInt32LittleEndian(Tag(ArgsCodec.Tag.Array, 4), count);
    }

    /// <summary>
    /// Writes the tag and reserves the payload, returning the payload's span for the caller to fill.
    /// The span stays valid after <c>Advance</c>: the buffer only moves on the next <c>GetSpan</c>.
    /// </summary>
    private Span<byte> Tag(ArgsCodec.Tag tag, int payload)
    {
        if ((long)_buffer.WrittenCount + 1 + payload > _maxBytes)
            throw new ProducerResultTooLargeException(_maxBytes);
        var span = _buffer.GetSpan(1 + payload)[..(1 + payload)];
        span[0] = (byte)tag;
        _buffer.Advance(1 + payload);
        return span[1..];
    }
}

/// <summary>Helpers the generated producer invokers and encoders call.</summary>
public static class ProducerResults
{
    /// <summary>Adapts a producer's return value to the host's awaitable.</summary>
    public static ValueTask<object?> Box<T>(Task<T> task)
    {
        ArgumentNullException.ThrowIfNull(task);
        return task.IsCompletedSuccessfully ? new ValueTask<object?>(task.Result) : Await(task);

        static async ValueTask<object?> Await(Task<T> task) => await task.ConfigureAwait(false);
    }

    /// <inheritdoc cref="Box{T}(Task{T})"/>
    public static ValueTask<object?> Box<T>(ValueTask<T> task) =>
        task.IsCompletedSuccessfully ? new ValueTask<object?>(task.Result) : Await(task);

    /// <inheritdoc cref="Box{T}(Task{T})"/>
    public static ValueTask<object?> Box<T>(T value) => new(value);

    /// <summary>
    /// A sequence with a count, for the array header. Arrays and lists pass through; any other
    /// enumerable — a lazy query over <c>ctx.Db</c> — is materialized here, while the pin is held.
    /// </summary>
    public static IReadOnlyCollection<T> Materialize<T>(IEnumerable<T> items) =>
        items as IReadOnlyCollection<T> ?? items.ToList();

    private static async ValueTask<object?> Await<T>(ValueTask<T> task) => await task.ConfigureAwait(false);
}
