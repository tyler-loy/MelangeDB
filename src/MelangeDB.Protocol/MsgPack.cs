using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Protocol;

/// <summary>
/// A minimal MessagePack writer implementing the subset of the spec the MelangeDB framing uses.
/// The format is standard MessagePack — any client language's implementation reads it — which is
/// the whole reason MessagePack is the v1 wire format; the encoder is hand-rolled only so the
/// protocol and client assemblies carry no package dependency.
/// </summary>
public struct MsgPackWriter
{
    private byte[] _buffer;
    private int _position;

    public MsgPackWriter(int sizeHint) => _buffer = new byte[Math.Max(sizeHint, 32)];

    /// <summary>The number of bytes written so far.</summary>
    public readonly int Length => _position;

    public void WriteNil() => WriteByte(0xc0);

    public void WriteBool(bool value) => WriteByte(value ? (byte)0xc3 : (byte)0xc2);

    public void WriteInt64(long value)
    {
        if (value >= 0)
        {
            WriteUInt64((ulong)value);
            return;
        }

        if (value >= -32)
        {
            WriteByte(unchecked((byte)value));
        }
        else if (value >= sbyte.MinValue)
        {
            WriteByte(0xd0);
            WriteByte(unchecked((byte)(sbyte)value));
        }
        else if (value >= short.MinValue)
        {
            WriteByte(0xd1);
            WriteBigEndian(unchecked((ushort)(short)value));
        }
        else if (value >= int.MinValue)
        {
            WriteByte(0xd2);
            WriteBigEndian(unchecked((uint)(int)value));
        }
        else
        {
            WriteByte(0xd3);
            WriteBigEndian(unchecked((ulong)value));
        }
    }

    public void WriteUInt64(ulong value)
    {
        if (value <= 0x7f)
        {
            WriteByte((byte)value);
        }
        else if (value <= byte.MaxValue)
        {
            WriteByte(0xcc);
            WriteByte((byte)value);
        }
        else if (value <= ushort.MaxValue)
        {
            WriteByte(0xcd);
            WriteBigEndian((ushort)value);
        }
        else if (value <= uint.MaxValue)
        {
            WriteByte(0xce);
            WriteBigEndian((uint)value);
        }
        else
        {
            WriteByte(0xcf);
            WriteBigEndian(value);
        }
    }

    public void WriteFloat32(float value)
    {
        WriteByte(0xca);
        WriteBigEndian(BitConverter.SingleToUInt32Bits(value));
    }

    public void WriteFloat64(double value)
    {
        WriteByte(0xcb);
        WriteBigEndian(BitConverter.DoubleToUInt64Bits(value));
    }

    public void WriteString(string? value)
    {
        if (value is null)
        {
            WriteNil();
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount <= 31)
        {
            WriteByte((byte)(0xa0 | byteCount));
        }
        else if (byteCount <= byte.MaxValue)
        {
            WriteByte(0xd9);
            WriteByte((byte)byteCount);
        }
        else if (byteCount <= ushort.MaxValue)
        {
            WriteByte(0xda);
            WriteBigEndian((ushort)byteCount);
        }
        else
        {
            WriteByte(0xdb);
            WriteBigEndian((uint)byteCount);
        }

        Ensure(byteCount);
        Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position));
        _position += byteCount;
    }

    public void WriteBinary(ReadOnlySpan<byte> value)
    {
        if (value.Length <= byte.MaxValue)
        {
            WriteByte(0xc4);
            WriteByte((byte)value.Length);
        }
        else if (value.Length <= ushort.MaxValue)
        {
            WriteByte(0xc5);
            WriteBigEndian((ushort)value.Length);
        }
        else
        {
            WriteByte(0xc6);
            WriteBigEndian((uint)value.Length);
        }

        Ensure(value.Length);
        value.CopyTo(_buffer.AsSpan(_position));
        _position += value.Length;
    }

    public void WriteArrayHeader(int count)
    {
        if (count <= 15)
        {
            WriteByte((byte)(0x90 | count));
        }
        else if (count <= ushort.MaxValue)
        {
            WriteByte(0xdc);
            WriteBigEndian((ushort)count);
        }
        else
        {
            WriteByte(0xdd);
            WriteBigEndian((uint)count);
        }
    }

    public void WriteMapHeader(int count)
    {
        if (count <= 15)
        {
            WriteByte((byte)(0x80 | count));
        }
        else if (count <= ushort.MaxValue)
        {
            WriteByte(0xde);
            WriteBigEndian((ushort)count);
        }
        else
        {
            WriteByte(0xdf);
            WriteBigEndian((uint)count);
        }
    }

    /// <summary>The written bytes as a right-sized array.</summary>
    public readonly byte[] ToArray() => _buffer.AsSpan(0, _position).ToArray();

    private void WriteByte(byte value)
    {
        Ensure(1);
        _buffer[_position++] = value;
    }

    private void WriteBigEndian(ushort value)
    {
        Ensure(2);
        BinaryPrimitives.WriteUInt16BigEndian(_buffer.AsSpan(_position), value);
        _position += 2;
    }

    private void WriteBigEndian(uint value)
    {
        Ensure(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(_position), value);
        _position += 4;
    }

    private void WriteBigEndian(ulong value)
    {
        Ensure(8);
        BinaryPrimitives.WriteUInt64BigEndian(_buffer.AsSpan(_position), value);
        _position += 8;
    }

    private void Ensure(int count)
    {
        if (_position + count <= _buffer.Length)
            return;
        var grown = new byte[Math.Max(_buffer.Length * 2, _position + count)];
        _buffer.AsSpan(0, _position).CopyTo(grown);
        _buffer = grown;
    }
}

/// <summary>Thrown when an inbound frame is not well-formed MessagePack or not a known frame shape.</summary>
public sealed class MelangeProtocolException : Exception
{
    public MelangeProtocolException(string message)
        : base(message)
    {
    }
}

/// <summary>The reading half of <see cref="MsgPackWriter"/>. Malformed input throws, never tears.</summary>
public ref struct MsgPackReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;

    public MsgPackReader(ReadOnlySpan<byte> data) => _data = data;

    /// <summary>Whether every byte has been consumed.</summary>
    public readonly bool End => _position >= _data.Length;

    public bool TryReadNil()
    {
        if (Peek() != 0xc0)
            return false;
        _position++;
        return true;
    }

    /// <summary>The next format code, without consuming it.</summary>
    public readonly byte PeekCode()
    {
        if (_position >= _data.Length)
            throw new MelangeProtocolException("Frame is truncated.");
        return _data[_position];
    }

    public bool ReadBool() => ReadByte() switch
    {
        0xc2 => false,
        0xc3 => true,
        var code => throw Unexpected(code, "bool"),
    };

    public long ReadInt64()
    {
        var code = ReadByte();
        if (code <= 0x7f)
            return code;
        if (code >= 0xe0)
            return unchecked((sbyte)code);
        return code switch
        {
            0xcc => ReadRawByte(),
            0xcd => ReadBigEndian16(),
            0xce => ReadBigEndian32(),
            0xcf => checked((long)ReadBigEndian64()),
            0xd0 => unchecked((sbyte)ReadRawByte()),
            0xd1 => unchecked((short)ReadBigEndian16()),
            0xd2 => unchecked((int)ReadBigEndian32()),
            0xd3 => unchecked((long)ReadBigEndian64()),
            _ => throw Unexpected(code, "integer"),
        };
    }

    public ulong ReadUInt64()
    {
        var code = ReadByte();
        if (code <= 0x7f)
            return code;
        return code switch
        {
            0xcc => ReadRawByte(),
            0xcd => ReadBigEndian16(),
            0xce => ReadBigEndian32(),
            0xcf => ReadBigEndian64(),
            0xd0 => checked((ulong)unchecked((sbyte)ReadRawByte())),
            0xd1 => checked((ulong)unchecked((short)ReadBigEndian16())),
            0xd2 => checked((ulong)unchecked((int)ReadBigEndian32())),
            0xd3 => checked((ulong)unchecked((long)ReadBigEndian64())),
            _ => throw Unexpected(code, "unsigned integer"),
        };
    }

    public double ReadFloat64() => ReadByte() switch
    {
        0xca => BitConverter.UInt32BitsToSingle(ReadBigEndian32()),
        0xcb => BitConverter.UInt64BitsToDouble(ReadBigEndian64()),
        var code => throw Unexpected(code, "float"),
    };

    public string? ReadString()
    {
        if (TryReadNil())
            return null;
        var code = ReadByte();
        int length;
        if ((code & 0xe0) == 0xa0)
            length = code & 0x1f;
        else
        {
            length = code switch
            {
                0xd9 => ReadRawByte(),
                0xda => ReadBigEndian16(),
                0xdb => checked((int)ReadBigEndian32()),
                _ => throw Unexpected(code, "string"),
            };
        }

        var value = Encoding.UTF8.GetString(Slice(length));
        return value;
    }

    public byte[] ReadBinary()
    {
        var code = ReadByte();
        var length = code switch
        {
            0xc4 => (int)ReadRawByte(),
            0xc5 => ReadBigEndian16(),
            0xc6 => checked((int)ReadBigEndian32()),
            _ => throw Unexpected(code, "binary"),
        };
        return Slice(length).ToArray();
    }

    public int ReadArrayHeader()
    {
        var code = ReadByte();
        if ((code & 0xf0) == 0x90)
            return code & 0x0f;
        return code switch
        {
            0xdc => ReadBigEndian16(),
            0xdd => checked((int)ReadBigEndian32()),
            _ => throw Unexpected(code, "array"),
        };
    }

    public int ReadMapHeader()
    {
        var code = ReadByte();
        if ((code & 0xf0) == 0x80)
            return code & 0x0f;
        return code switch
        {
            0xde => ReadBigEndian16(),
            0xdf => checked((int)ReadBigEndian32()),
            _ => throw Unexpected(code, "map"),
        };
    }

    private readonly byte Peek()
    {
        if (_position >= _data.Length)
            throw new MelangeProtocolException("Frame is truncated.");
        return _data[_position];
    }

    private byte ReadByte()
    {
        if (_position >= _data.Length)
            throw new MelangeProtocolException("Frame is truncated.");
        return _data[_position++];
    }

    private byte ReadRawByte() => ReadByte();

    private ushort ReadBigEndian16()
    {
        var value = BinaryPrimitives.ReadUInt16BigEndian(Slice(2));
        return value;
    }

    private uint ReadBigEndian32() => BinaryPrimitives.ReadUInt32BigEndian(Slice(4));

    private ulong ReadBigEndian64() => BinaryPrimitives.ReadUInt64BigEndian(Slice(8));

    private ReadOnlySpan<byte> Slice(int length)
    {
        if (length < 0 || _data.Length - _position < length)
            throw new MelangeProtocolException("Frame is truncated.");
        var slice = _data.Slice(_position, length);
        _position += length;
        return slice;
    }

    private static MelangeProtocolException Unexpected(byte code, string expected) =>
        new($"Unexpected MessagePack code 0x{code:x2}; expected {expected}.");
}
