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

    public static byte[] WritePayload(ulong lsn, in CommitRequest request)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(RecordFormatVersion);
        writer.Write(lsn);
        writer.Write(request.Timestamp.UnixTimeMicroseconds);
        Span<byte> identity = stackalloc byte[Identity.Size];
        request.Caller.WriteTo(identity);
        writer.Write(identity);
        var name = Encoding.UTF8.GetBytes(request.ReducerName);
        writer.Write((ushort)name.Length);
        writer.Write(name);
        writer.Write(request.Arguments.Length);
        writer.Write(request.Arguments.Span);
        writer.Write(request.WriteSet.Count);
        foreach (var op in request.WriteSet)
        {
            writer.Write((byte)op.Kind);
            writer.Write(op.Table.Value);
            writer.Write((ushort)op.Key.Length);
            writer.Write(op.Key.Span);
            if (op.Kind != RowOpKind.Delete)
            {
                writer.Write(op.Row.Length);
                writer.Write(op.Row.Span);
            }
        }

        var events = request.Events ?? [];
        writer.Write(events.Count);
        foreach (var evt in events)
        {
            var typeName = Encoding.UTF8.GetBytes(evt.EventType);
            writer.Write((ushort)typeName.Length);
            writer.Write(typeName);
            writer.Write(evt.Depth);
            writer.Write(evt.Payload.Length);
            writer.Write(evt.Payload.Span);
        }

        writer.Flush();
        return stream.ToArray();
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
