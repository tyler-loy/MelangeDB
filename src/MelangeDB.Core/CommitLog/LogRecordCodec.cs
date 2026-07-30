using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Serializes commit-record payloads. Every payload begins with a format version — written from
/// record one — so a later serializer can supersede this one while existing logs still read.
/// </summary>
internal static class LogRecordCodec
{
    public static byte[] WritePayload(ulong lsn, in CommitRequest request)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(RowSerializer.FormatVersion);
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

        writer.Flush();
        return stream.ToArray();
    }

    public static CommitRecord ReadPayload(byte[] payload, int serializedLength)
    {
        using var stream = new MemoryStream(payload, writable: false);
        using var reader = new BinaryReader(stream);
        var formatVersion = reader.ReadUInt16();
        if (formatVersion != RowSerializer.FormatVersion)
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

        return new CommitRecord
        {
            Lsn = lsn,
            FormatVersion = formatVersion,
            Timestamp = timestamp,
            Caller = caller,
            ReducerName = reducerName,
            Arguments = arguments,
            WriteSet = writeSet,
            SerializedLength = serializedLength,
        };
    }
}
