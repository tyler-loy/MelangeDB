using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Serializes commit-record payloads. Every payload begins with a format version — written from
/// record one — so a later serializer can supersede this one while existing logs still read.
/// Version 2 is version 1 plus a trailing domain-event section; version-1 records read back with
/// no events, so logs written before the event bus existed stay readable with no migration.
/// </summary>
internal static class LogRecordCodec
{
    /// <summary>
    /// The record format this build writes. Distinct from <see cref="RowSerializer.FormatVersion"/>
    /// on purpose: row bytes inside a record are still format 1.
    /// </summary>
    public const ushort RecordFormatVersion = 2;

    /// <summary>
    /// The payload buffer pool. Deliberately <b>not</b> <see cref="ArrayPool{T}.Shared"/>, and the
    /// residency test is why: a shared pool retains whatever it is handed, so one bulk load of
    /// hundred-kilobyte blobs parks megabytes of buffers outside the declared memory budget for the
    /// life of the process. This database's memory budget is a computable, reported artifact, and
    /// quietly holding buffers beside it is exactly the kind of drift that makes the report a lie.
    /// <para>
    /// A bounded pool fixes it without a threshold check at the call site: a request larger than
    /// <see cref="MaxPooledPayload"/> is simply allocated, and returning it is ignored. That is the
    /// behaviour worth having — the commit-path benchmark puts a typical payload between 700 bytes
    /// and 45 KB, so the steady state pools, and the rare oversized bulk record allocates once and
    /// becomes collectable rather than resident.
    /// </para>
    /// </summary>
    private const int MaxPooledPayload = 256 * 1024;

    private static readonly ArrayPool<byte> Payloads = ArrayPool<byte>.Create(MaxPooledPayload, maxArraysPerBucket: 4);

    /// <summary>
    /// Writes a record's payload into a buffer rented from <see cref="ArrayPool{T}"/>, returning
    /// its length. The caller <b>must</b> hand the buffer back through <see cref="Release"/> —
    /// <c>FileCommitLog.Append</c> does so in a finally, because it is the only caller and the
    /// buffer is dead the moment the bytes reach the stream.
    /// <para>
    /// This used to be a <c>MemoryStream</c> plus a <c>BinaryWriter</c> plus a final <c>ToArray</c>
    /// copy, and it is here rather than at the other five allocation sites finding #6 lists because
    /// the commit-path benchmark says so: the payload is 15–19% of everything a commit allocates,
    /// steady across write-set sizes from one row to a hundred, and framing plus CRC add barely a
    /// hundred bytes on top of it. Pooling the rest on principle would have bought less and risked
    /// more.
    /// </para>
    /// </summary>
    public static int WritePayload(ulong lsn, in CommitRequest request, out byte[] buffer)
    {
        var writer = new PayloadWriter(Estimate(request));
        writer.WriteUInt16(RecordFormatVersion);
        writer.WriteUInt64(lsn);
        writer.WriteInt64(request.Timestamp.UnixTimeMicroseconds);
        Span<byte> identity = stackalloc byte[Identity.Size];
        request.Caller.WriteTo(identity);
        writer.WriteBytes(identity);
        writer.WriteUtf8WithUInt16Length(request.ReducerName);
        writer.WriteInt32(request.Arguments.Length);
        writer.WriteBytes(request.Arguments.Span);
        writer.WriteInt32(request.WriteSet.Count);
        foreach (var op in request.WriteSet)
        {
            writer.WriteByte((byte)op.Kind);
            writer.WriteUInt32(op.Table.Value);
            writer.WriteUInt16((ushort)op.Key.Length);
            writer.WriteBytes(op.Key.Span);
            if (op.Kind != RowOpKind.Delete)
            {
                writer.WriteInt32(op.Row.Length);
                writer.WriteBytes(op.Row.Span);
            }
        }

        var events = request.Events ?? [];
        writer.WriteInt32(events.Count);
        foreach (var evt in events)
        {
            writer.WriteUtf8WithUInt16Length(evt.EventType);
            writer.WriteByte(evt.Depth);
            writer.WriteInt32(evt.Payload.Length);
            writer.WriteBytes(evt.Payload.Span);
        }

        return writer.Detach(out buffer);
    }

    /// <summary>Returns a buffer from <see cref="WritePayload"/> to the pool.</summary>
    public static void Release(byte[] buffer) => Payloads.Return(buffer);

    /// <summary>
    /// A starting size that usually avoids a single growth. Row and key bytes dominate, and the
    /// fixed header plus per-op overhead is small and bounded, so a generous per-op constant beats
    /// a second pass to measure exactly.
    /// </summary>
    private static int Estimate(in CommitRequest request)
    {
        var size = 64 + request.ReducerName.Length + request.Arguments.Length;
        foreach (var op in request.WriteSet)
            size += 16 + op.Key.Length + op.Row.Length;
        foreach (var evt in request.Events ?? [])
            size += 16 + evt.EventType.Length + evt.Payload.Length;
        return size;
    }

    /// <summary>
    /// A little-endian span writer over a pooled buffer, matching byte for byte what
    /// <see cref="BinaryWriter"/> wrote before it — which is the whole requirement, since every log
    /// ever written by an earlier build has to keep reading.
    /// </summary>
    private ref struct PayloadWriter(int sizeHint)
    {
        private byte[] _buffer = Payloads.Rent(Math.Max(sizeHint, 128));
        private int _position = 0;

        public void WriteByte(byte value)
        {
            Ensure(1);
            _buffer[_position++] = value;
        }

        public void WriteUInt16(ushort value)
        {
            Ensure(2);
            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(_position), value);
            _position += 2;
        }

        public void WriteUInt32(uint value)
        {
            Ensure(4);
            BinaryPrimitives.WriteUInt32LittleEndian(_buffer.AsSpan(_position), value);
            _position += 4;
        }

        public void WriteInt32(int value)
        {
            Ensure(4);
            BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(_position), value);
            _position += 4;
        }

        public void WriteUInt64(ulong value)
        {
            Ensure(8);
            BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(_position), value);
            _position += 8;
        }

        public void WriteInt64(long value)
        {
            Ensure(8);
            BinaryPrimitives.WriteInt64LittleEndian(_buffer.AsSpan(_position), value);
            _position += 8;
        }

        // scoped: the writer copies the bytes and never keeps the span, which the compiler cannot
        // infer for a ref struct receiver — without it a stackalloc'd caller span is rejected.
        public void WriteBytes(scoped ReadOnlySpan<byte> value)
        {
            if (value.IsEmpty)
                return;
            Ensure(value.Length);
            value.CopyTo(_buffer.AsSpan(_position));
            _position += value.Length;
        }

        /// <summary>
        /// A UTF-8 string behind a two-byte length. Encoded straight into the buffer rather than
        /// into an intermediate array, which is one of the two allocations per record this removes.
        /// </summary>
        public void WriteUtf8WithUInt16Length(string value)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            Ensure(2 + byteCount);
            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(_position), (ushort)byteCount);
            _position += 2;
            Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position));
            _position += byteCount;
        }

        /// <summary>Hands the buffer to the caller, who owns returning it to the pool.</summary>
        public int Detach(out byte[] buffer)
        {
            buffer = _buffer;
            return _position;
        }

        private void Ensure(int count)
        {
            if (_position + count <= _buffer.Length)
                return;
            var grown = Payloads.Rent(Math.Max(_buffer.Length * 2, _position + count));
            _buffer.AsSpan(0, _position).CopyTo(grown);
            Payloads.Return(_buffer);
            _buffer = grown;
        }
    }

    public static CommitRecord ReadPayload(byte[] payload, int serializedLength)
    {
        using var stream = new MemoryStream(payload, writable: false);
        using var reader = new BinaryReader(stream);
        var formatVersion = reader.ReadUInt16();
        if (formatVersion is not (1 or RecordFormatVersion))
            throw new InvalidDataException($"Unknown record format version {formatVersion}.");
        var lsn = reader.ReadUInt64();
        var timestamp = new Timestamp(reader.ReadInt64());
        var caller = new Identity(reader.ReadBytes(Identity.Size));
        var reducerName = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadUInt16()));
        var arguments = reader.ReadBytes(reader.ReadInt32());
        var opCount = reader.ReadInt32();
        var writeSet = new List<RowOp>(opCount);
        for (var i = 0; i < opCount; i++)
        {
            var kind = (RowOpKind)reader.ReadByte();
            var table = new TableId(reader.ReadUInt32());
            var key = new RowKey(reader.ReadBytes(reader.ReadUInt16()));
            var row = kind == RowOpKind.Delete
                ? ReadOnlyMemory<byte>.Empty
                : reader.ReadBytes(reader.ReadInt32());
            writeSet.Add(new RowOp(kind, table, key, row));
        }

        IReadOnlyList<EventRecord> events = [];
        if (formatVersion >= 2)
        {
            var eventCount = reader.ReadInt32();
            var list = new List<EventRecord>(eventCount);
            for (var i = 0; i < eventCount; i++)
            {
                var eventType = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadUInt16()));
                var depth = reader.ReadByte();
                var eventPayload = reader.ReadBytes(reader.ReadInt32());
                list.Add(new EventRecord(eventType, depth, eventPayload));
            }

            events = list;
        }

        return new CommitRecord
        {
            Lsn = lsn,
            FormatVersion = formatVersion,
            Timestamp = timestamp,
            Caller = caller,
            ReducerName = reducerName,
            Arguments = arguments,
            WriteSet = writeSet,
            Events = events,
            SerializedLength = serializedLength,
        };
    }
}
