using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// A growable little-endian writer implementing row format v1 — the byte-for-byte format
/// <see cref="RowSerializer"/> writes. Generated codecs use it so their output is
/// indistinguishable from the reflection path's, which is what keeps existing logs readable.
/// </summary>
public struct RowWriter
{
    private byte[] _buffer;
    private int _position;

    public RowWriter(int sizeHint) => _buffer = new byte[Math.Max(sizeHint, 16)];

    public void WriteBool(bool value) => WriteUInt8(value ? (byte)1 : (byte)0);

    public void WriteInt8(sbyte value) => WriteUInt8(unchecked((byte)value));

    public void WriteUInt8(byte value)
    {
        Ensure(1);
        _buffer[_position++] = value;
    }

    public void WriteInt16(short value) => WriteUInt16(unchecked((ushort)value));

    public void WriteUInt16(ushort value)
    {
        Ensure(2);
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(_position), value);
        _position += 2;
    }

    public void WriteInt32(int value) => WriteUInt32(unchecked((uint)value));

    public void WriteUInt32(uint value)
    {
        Ensure(4);
        BinaryPrimitives.WriteUInt32LittleEndian(_buffer.AsSpan(_position), value);
        _position += 4;
    }

    public void WriteInt64(long value) => WriteUInt64(unchecked((ulong)value));

    public void WriteUInt64(ulong value)
    {
        Ensure(8);
        BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(_position), value);
        _position += 8;
    }

    public void WriteFloat32(float value) => WriteUInt32(BitConverter.SingleToUInt32Bits(value));

    public void WriteFloat64(double value) => WriteUInt64(BitConverter.DoubleToUInt64Bits(value));

    /// <summary>Null flag byte, then int32 byte length and UTF-8 payload when present.</summary>
    public void WriteString(string? value)
    {
        if (value is null)
        {
            WriteUInt8(0);
            return;
        }

        WriteUInt8(1);
        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteInt32(byteCount);
        Ensure(byteCount);
        Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position));
        _position += byteCount;
    }

    /// <summary>Null flag byte, then int32 length and raw payload when present.</summary>
    public void WriteBytes(byte[]? value)
    {
        if (value is null)
        {
            WriteUInt8(0);
            return;
        }

        WriteUInt8(1);
        WriteInt32(value.Length);
        Ensure(value.Length);
        value.CopyTo(_buffer.AsSpan(_position));
        _position += value.Length;
    }

    public void WriteIdentity(Identity value)
    {
        Ensure(Identity.Size);
        value.WriteTo(_buffer.AsSpan(_position));
        _position += Identity.Size;
    }

    public void WriteTimestamp(Timestamp value) => WriteInt64(value.UnixTimeMicroseconds);

    /// <summary>One discriminant byte (0 instant, 1 interval), then the int64 microsecond payload.</summary>
    public void WriteScheduleAt(ScheduleAt value)
    {
        WriteUInt8(value.IsInterval ? (byte)1 : (byte)0);
        WriteInt64(value.Microseconds);
    }

    /// <summary>The written bytes as a right-sized array.</summary>
    public readonly byte[] ToArray() => _buffer.AsSpan(0, _position).ToArray();

    private void Ensure(int count)
    {
        if (_position + count <= _buffer.Length)
            return;
        var grown = new byte[Math.Max(_buffer.Length * 2, _position + count)];
        _buffer.AsSpan(0, _position).CopyTo(grown);
        _buffer = grown;
    }
}

/// <summary>The reading half of row format v1; see <see cref="RowWriter"/>.</summary>
public ref struct RowReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;

    public RowReader(ReadOnlySpan<byte> data) => _data = data;

    public bool ReadBool() => ReadUInt8() != 0;

    public sbyte ReadInt8() => unchecked((sbyte)ReadUInt8());

    public byte ReadUInt8() => _data[_position++];

    public short ReadInt16() => unchecked((short)ReadUInt16());

    public ushort ReadUInt16()
    {
        var value = BinaryPrimitives.ReadUInt16LittleEndian(_data[_position..]);
        _position += 2;
        return value;
    }

    public int ReadInt32() => unchecked((int)ReadUInt32());

    public uint ReadUInt32()
    {
        var value = BinaryPrimitives.ReadUInt32LittleEndian(_data[_position..]);
        _position += 4;
        return value;
    }

    public long ReadInt64() => unchecked((long)ReadUInt64());

    public ulong ReadUInt64()
    {
        var value = BinaryPrimitives.ReadUInt64LittleEndian(_data[_position..]);
        _position += 8;
        return value;
    }

    public float ReadFloat32() => BitConverter.UInt32BitsToSingle(ReadUInt32());

    public double ReadFloat64() => BitConverter.UInt64BitsToDouble(ReadUInt64());

    public string? ReadString()
    {
        if (ReadUInt8() == 0)
            return null;
        var length = ReadInt32();
        var value = Encoding.UTF8.GetString(_data.Slice(_position, length));
        _position += length;
        return value;
    }

    public byte[]? ReadBytes()
    {
        if (ReadUInt8() == 0)
            return null;
        var length = ReadInt32();
        var value = _data.Slice(_position, length).ToArray();
        _position += length;
        return value;
    }

    public Identity ReadIdentity()
    {
        var value = new Identity(_data.Slice(_position, Identity.Size));
        _position += Identity.Size;
        return value;
    }

    public Timestamp ReadTimestamp() => new(ReadInt64());

    public ScheduleAt ReadScheduleAt()
    {
        var interval = ReadUInt8() != 0;
        return ScheduleAt.FromMicroseconds(interval, ReadInt64());
    }
}
