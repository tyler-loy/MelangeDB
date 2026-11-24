using System.Buffers.Binary;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MelangeDB.Cluster;

/// <summary>
/// A message body received on a node link: its JSON, and, on a binary frame, the attachment its
/// row bytes point into. <see cref="Read{T}"/> is the only way a body becomes a message: a binary
/// body's <c>byte[]</c> and <see cref="WireOp"/> fields are offsets that mean nothing without it.
/// </summary>
internal readonly struct LinkBody
{
    private readonly byte[]? _frame;
    private readonly int _attachmentStart;

    public LinkBody(JsonElement json, byte[]? frame = null, int attachmentStart = 0)
    {
        Json = json;
        _frame = frame;
        _attachmentStart = attachmentStart;
    }

    /// <summary>The body's JSON: the whole message on a JSON frame, its structure on a binary one.</summary>
    public JsonElement Json { get; }

    /// <summary>Whether this body arrived on a binary frame, so its bytes live in an attachment.</summary>
    public bool HasAttachment => _frame is not null;

    /// <summary>Decodes the body as <typeparamref name="T"/>, reading row bytes from the attachment when it has one.</summary>
    public T? Read<T>() => LinkCodec.Read<T>(Json, _frame, _attachmentStart);
}

/// <summary>
/// How a node-link body is encoded. Control messages are small JSON either way. What makes a frame
/// big is row data: <see cref="WireOp"/> arrays (border batches and resets, replica batches and
/// reads, handoff rows) and <c>byte[]</c> fields (event payloads, bulk rows). On a JSON frame those
/// travel as base64 inside the JSON, as they always have, which costs a third more bytes, a
/// base64 encode and decode, and a pass of the JSON writer and parser over every row byte.
/// <para>
/// On a binary frame they travel after the JSON, in an attachment: the JSON holds an offset where
/// the value would be, and the attachment holds the bytes, a <see cref="WireOp"/> array packed as
/// <c>count, (kind, table, key length, key, row length + 1, row)...</c> with lengths as varints and
/// 0 marking a null row. A frame is then <c>0xB1, JSON length (int32), JSON, attachment</c>; a
/// JSON frame starts with <c>{</c>, so a reader tells them apart by the first byte and accepts
/// both. Which one a link sends is negotiated at authentication (<see cref="NodeLink.SendsBinary"/>),
/// so a node and a hub of different versions keep talking JSON.
/// </para>
/// <para>
/// The converters know which kind of frame they are writing or reading from thread-local state
/// set around one synchronous serialize or deserialize call, which is the narrowest scope there is.
/// </para>
/// </summary>
internal static class LinkCodec
{
    /// <summary>The first byte of a binary frame's payload; a JSON frame's is <c>{</c>.</summary>
    public const byte BinaryMarker = 0xB1;

    /// <summary>The link format a build that reads and writes binary frames advertises at authentication.</summary>
    public const int BinaryFormat = 2;

    /// <summary>Bytes before a binary frame's JSON: the marker and the JSON's length.</summary>
    public const int BinaryHeaderBytes = 5;

    public static readonly JsonSerializerOptions Options = new()
    {
        Converters = { new BytesConverter(), new WireOpsConverter() },
    };

    [ThreadStatic]
    private static AttachmentWriter? t_writer;

    [ThreadStatic]
    private static byte[]? t_frame;

    [ThreadStatic]
    private static int t_attachmentStart;

    [ThreadStatic]
    private static int t_attachmentEnd;

    /// <summary>
    /// Serializes <paramref name="body"/> into <paramref name="json"/>; with an <paramref name="attachment"/>,
    /// row bytes go there instead of into the JSON.
    /// </summary>
    public static void Write(Utf8JsonWriter json, object body, AttachmentWriter? attachment)
    {
        var previous = t_writer;
        t_writer = attachment;
        try
        {
            JsonSerializer.Serialize(json, body, body.GetType(), Options);
        }
        finally
        {
            t_writer = previous;
        }
    }

    public static T? Read<T>(JsonElement json, byte[]? frame, int attachmentStart)
    {
        var (previousFrame, previousStart, previousEnd) = (t_frame, t_attachmentStart, t_attachmentEnd);
        (t_frame, t_attachmentStart, t_attachmentEnd) = (frame, attachmentStart, frame?.Length ?? 0);
        try
        {
            return json.Deserialize<T>(Options);
        }
        finally
        {
            (t_frame, t_attachmentStart, t_attachmentEnd) = (previousFrame, previousStart, previousEnd);
        }
    }

    /// <summary>The attachment bytes from <paramref name="offset"/> on, or a malformed-frame failure.</summary>
    private static ReadOnlySpan<byte> Attachment(int offset)
    {
        if (t_frame is null)
            throw new JsonException("A node-link body names an attachment offset, but its frame has no attachment.");
        if (offset < 0 || offset > t_attachmentEnd - t_attachmentStart)
            throw new JsonException($"A node-link attachment offset {offset} is out of range.");
        return t_frame.AsSpan(t_attachmentStart + offset, t_attachmentEnd - t_attachmentStart - offset);
    }

    private static ulong ReadVarint(ref ReadOnlySpan<byte> span)
    {
        ulong value = 0;
        for (var shift = 0; shift < 64; shift += 7)
        {
            if (span.IsEmpty)
                throw new JsonException("A node-link attachment ends inside a length.");
            var b = span[0];
            span = span[1..];
            value |= (ulong)(b & 0x7F) << shift;
            if (b < 0x80)
                return value;
        }

        throw new JsonException("A node-link attachment length is malformed.");
    }

    private static byte[] ReadBytes(ref ReadOnlySpan<byte> span, ulong length)
    {
        if (length > (ulong)span.Length)
            throw new JsonException("A node-link attachment ends inside a value.");
        var bytes = span[..(int)length].ToArray();
        span = span[(int)length..];
        return bytes;
    }

    /// <summary><c>byte[]</c>: base64 on a JSON frame, an attachment offset (to a length-prefixed run) on a binary one.</summary>
    private sealed class BytesConverter : JsonConverter<byte[]>
    {
        public override bool HandleNull => false;

        public override byte[]? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
                return reader.GetBytesFromBase64();
            if (reader.TokenType != JsonTokenType.Number)
                throw new JsonException($"A node-link byte array is a {reader.TokenType}.");
            var span = Attachment(reader.GetInt32());
            var length = ReadVarint(ref span);
            return ReadBytes(ref span, length);
        }

        public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options)
        {
            if (t_writer is not { } attachment)
            {
                writer.WriteBase64StringValue(value);
                return;
            }

            writer.WriteNumberValue(attachment.Length);
            attachment.WriteVarint((ulong)value.Length);
            attachment.WriteBytes(value);
        }
    }

    /// <summary><see cref="WireOp"/> arrays: JSON objects on a JSON frame, one packed attachment run on a binary one.</summary>
    private sealed class WireOpsConverter : JsonConverter<WireOp[]>
    {
        public override bool HandleNull => false;

        public override WireOp[]? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.StartArray)
            {
                var ops = new List<WireOp>();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    ops.Add(JsonSerializer.Deserialize<WireOp>(ref reader, options) ?? throw new JsonException("A null node-link row op."));
                return [.. ops];
            }

            if (reader.TokenType != JsonTokenType.Number)
                throw new JsonException($"A node-link row op array is a {reader.TokenType}.");
            var span = Attachment(reader.GetInt32());
            var count = ReadVarint(ref span);
            if (count > (ulong)span.Length)
                throw new JsonException("A node-link row op count is larger than its attachment.");
            var result = new WireOp[count];
            for (var i = 0; i < result.Length; i++)
            {
                if (span.Length < 5)
                    throw new JsonException("A node-link attachment ends inside a row op.");
                var kind = span[0];
                var table = BinaryPrimitives.ReadUInt32LittleEndian(span[1..]);
                span = span[5..];
                var key = ReadBytes(ref span, ReadVarint(ref span));
                var rowLength = ReadVarint(ref span);
                var row = rowLength == 0 ? null : ReadBytes(ref span, rowLength - 1);
                result[i] = new WireOp(kind, table, key, row);
            }

            return result;
        }

        public override void Write(Utf8JsonWriter writer, WireOp[] value, JsonSerializerOptions options)
        {
            if (t_writer is not { } attachment)
            {
                writer.WriteStartArray();
                foreach (var op in value)
                    JsonSerializer.Serialize(writer, op, options);
                writer.WriteEndArray();
                return;
            }

            writer.WriteNumberValue(attachment.Length);
            attachment.WriteVarint((ulong)value.Length);
            foreach (var op in value)
            {
                attachment.WriteByte(op.Kind);
                attachment.WriteUInt32(op.Table);
                attachment.WriteVarint((ulong)op.Key.Length);
                attachment.WriteBytes(op.Key);
                if (op.Row is null)
                {
                    attachment.WriteVarint(0);
                }
                else
                {
                    attachment.WriteVarint((ulong)op.Row.Length + 1);
                    attachment.WriteBytes(op.Row);
                }
            }
        }
    }
}

/// <summary>
/// A binary frame's attachment while it is being written: small fields gathered into chunks, and
/// row bytes kept as references to the arrays the message already holds, so each row byte is
/// copied once, into the frame. The message must not change until the frame is encoded, which
/// <see cref="NodeLink"/> does synchronously before a send returns.
/// </summary>
internal sealed class AttachmentWriter
{
    /// <summary>Values this long or shorter are copied into a chunk; longer ones are referenced.</summary>
    private const int ReferenceThreshold = 64;

    private readonly List<ReadOnlyMemory<byte>> _segments = [];
    private byte[] _chunk = [];
    private int _chunkStart;
    private int _chunkUsed;

    public int Length { get; private set; }

    public void WriteByte(byte value)
    {
        Reserve(1);
        _chunk[_chunkUsed++] = value;
        Length++;
    }

    public void WriteUInt32(uint value)
    {
        Reserve(4);
        BinaryPrimitives.WriteUInt32LittleEndian(_chunk.AsSpan(_chunkUsed), value);
        _chunkUsed += 4;
        Length += 4;
    }

    public void WriteVarint(ulong value)
    {
        Reserve(10);
        while (value >= 0x80)
        {
            _chunk[_chunkUsed++] = (byte)(value | 0x80);
            value >>= 7;
            Length++;
        }

        _chunk[_chunkUsed++] = (byte)value;
        Length++;
    }

    public void WriteBytes(byte[] value)
    {
        if (value.Length <= ReferenceThreshold)
        {
            Reserve(value.Length);
            value.CopyTo(_chunk, _chunkUsed);
            _chunkUsed += value.Length;
        }
        else
        {
            Seal();
            _segments.Add(value);
        }

        Length += value.Length;
    }

    public void CopyTo(Span<byte> destination)
    {
        Seal();
        foreach (var segment in _segments)
        {
            segment.Span.CopyTo(destination);
            destination = destination[segment.Length..];
        }
    }

    private void Reserve(int bytes)
    {
        if (_chunk.Length - _chunkUsed >= bytes)
            return;
        Seal();
        _chunk = new byte[Math.Max(4096, bytes)];
        _chunkStart = 0;
        _chunkUsed = 0;
    }

    /// <summary>Ends the current chunk's run, so what follows lands after it in order.</summary>
    private void Seal()
    {
        if (_chunkUsed > _chunkStart)
            _segments.Add(_chunk.AsMemory(_chunkStart, _chunkUsed - _chunkStart));
        _chunkStart = _chunkUsed;
    }
}
