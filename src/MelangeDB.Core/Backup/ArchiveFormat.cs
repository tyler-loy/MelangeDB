using System.Buffers.Binary;

namespace MelangeDB.Core;

/// <summary>
/// The <c>.mbak</c> archive format: magic, version, and CRC-checked frames, in the log and
/// snapshot formats' conventions. An archive is a header, a manifest frame, then per engine an
/// identity frame, that engine's snapshot rows, its log tail above the snapshot LSN, its sidecars,
/// and a counted end frame; a final archive-end frame makes truncation detectable. Everything is
/// written and read as a stream — an archive larger than memory never needs a materialized copy.
/// <para>
/// The archive carries the <b>truth, not the projections</b>: logical snapshot rows and verbatim
/// log-record payloads, never store files. That is what makes it store-engine agnostic — a backup
/// taken from a FASTER deployment restores into an in-memory one, and vice versa — and it also
/// means the archive is versioned independently of the files it was read from.
/// </para>
/// <para>
/// This format is public API from byte zero: the moment one nightly job depends on it, every later
/// build must keep reading it (the log's own <c>FileFormatVersion</c> discipline, applied to a
/// file that leaves the machine).
/// </para>
/// </summary>
internal static class ArchiveFormat
{
    public const uint Magic = 0x4B41424Du; // "MBAK"
    public const ushort FormatVersion = 1;
    public const int HeaderSize = 8; // u32 magic, u16 version, u16 reserved(0)

    /// <summary>
    /// u8 type, u32 payload length, u32 crc. The CRC covers the type byte and the length bytes as
    /// well as the payload — unlike the log's frames, whose header corruption is caught by the
    /// downstream parse, an archive must catch <em>every</em> flipped bit at the frame that
    /// carries it, because "fails verify with the frame named" is the format's contract.
    /// </summary>
    public const int FrameHeaderSize = 9;

    /// <summary>The log's own record ceiling; no archive frame has a reason to exceed it.</summary>
    public const uint MaxFramePayload = FileCommitLog.MaxRecordBytes;

    /// <summary>The engine key a single-node archive uses; cluster archives use hub/shard keys.</summary>
    public const string SingleNodeEngineKey = "node";
}

internal enum ArchiveFrameType : byte
{
    /// <summary>JSON <see cref="ArchiveManifest"/>: capture time and the engine keys to expect.</summary>
    Manifest = 1,

    /// <summary>JSON <see cref="ArchiveEngineIdentity"/>: one engine's identity and positions.</summary>
    EngineBegin = 2,

    /// <summary>Binary: u32 table, i32 key length, i32 row length, key bytes, row bytes.</summary>
    SnapshotRow = 3,

    /// <summary>A commit-log record's payload, verbatim as <c>LogRecordCodec</c> framed it.</summary>
    LogRecord = 4,

    /// <summary>Binary: u16 name length, UTF-8 name, then the sidecar's bytes verbatim.</summary>
    Sidecar = 5,

    /// <summary>JSON <see cref="ArchiveEngineFooter"/>: the counts the engine's frames must total.</summary>
    EngineEnd = 6,

    /// <summary>JSON <see cref="ArchiveFooter"/>: the engine count; nothing may follow it.</summary>
    ArchiveEnd = 7,
}

/// <summary>The manifest: what the archive holds, written before any engine streams.</summary>
internal sealed class ArchiveManifest
{
    public required long CapturedAtUnixMs { get; init; }

    public required IReadOnlyList<string> Engines { get; init; }
}

/// <summary>
/// One engine's identity: where its capture sits in its history. The source epoch is diagnostic
/// only — restore always mints a fresh epoch, because a restore is a rewind and a client whose
/// resume cursor sits past the restored head must full-resync, not resume into history that no
/// longer happened.
/// </summary>
internal sealed class ArchiveEngineIdentity
{
    public required string Key { get; init; }

    public required Guid SourceEpoch { get; init; }

    public required ulong BaseLsn { get; init; }

    /// <summary>The LSN the archived snapshot covers; 0 when the engine had no snapshot.</summary>
    public required ulong SnapshotLsn { get; init; }

    public required ulong HeadLsn { get; init; }

    /// <summary>The snapshot's tail-record timestamp, carried so restore can rewrite the header.</summary>
    public required long SnapshotTimestampMicros { get; init; }

    /// <summary>
    /// The AutoInc sequences from the snapshot header, so a restored world allocates ids above
    /// everything it has ever handed out. Sequences allocated after the snapshot are re-observed
    /// from the log tail during recovery — the same machinery every restart uses.
    /// </summary>
    public required IReadOnlyList<ArchiveSequence> Sequences { get; init; }
}

internal sealed class ArchiveSequence
{
    public required uint Table { get; init; }

    public required ulong Next { get; init; }
}

/// <summary>The counts an engine's frames must have totalled — the per-engine completeness check.</summary>
internal sealed class ArchiveEngineFooter
{
    public required long SnapshotRows { get; init; }

    public required long TailRecords { get; init; }
}

/// <summary>The final frame: the engine count, and the promise that the stream ended on purpose.</summary>
internal sealed class ArchiveFooter
{
    public required int Engines { get; init; }
}

/// <summary>Writes archive frames to a stream; every frame is CRC-guarded including its header.</summary>
internal sealed class ArchiveFrameWriter(Stream stream)
{
    /// <summary>Total bytes written so far — the backup summary's size, and the metric's source.</summary>
    public long BytesWritten { get; private set; }

    public void WriteHeader()
    {
        Span<byte> header = stackalloc byte[ArchiveFormat.HeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(header, ArchiveFormat.Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], ArchiveFormat.FormatVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(header[6..], 0);
        stream.Write(header);
        BytesWritten += ArchiveFormat.HeaderSize;
    }

    public void WriteFrame(ArchiveFrameType type, ReadOnlySpan<byte> payload)
    {
        Span<byte> frame = stackalloc byte[ArchiveFormat.FrameHeaderSize];
        frame[0] = (byte)type;
        BinaryPrimitives.WriteUInt32LittleEndian(frame[1..], (uint)payload.Length);
        var crc = Crc32.Begin();
        crc = Crc32.Append(crc, frame[..5]);
        crc = Crc32.Append(crc, payload);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[5..], Crc32.End(crc));
        stream.Write(frame);
        stream.Write(payload);
        BytesWritten += ArchiveFormat.FrameHeaderSize + payload.Length;
    }
}

/// <summary>
/// Reads archive frames from a stream, validating every CRC as it goes. Any corruption throws
/// <see cref="InvalidDataException"/> naming the frame — verify's contract is that every flipped
/// bit fails with the frame named, and restore refuses the same archive rather than materializing
/// a partial world.
/// </summary>
internal sealed class ArchiveFrameReader(Stream stream, string source)
{
    private (ArchiveFrameType Type, byte[] Payload)? _pushedBack;

    /// <summary>The index of the most recently read frame, for error messages. Header is frame 0.</summary>
    public long FrameIndex { get; private set; }

    public void ReadHeader()
    {
        Span<byte> header = stackalloc byte[ArchiveFormat.HeaderSize];
        if (!TryReadExactly(header))
            throw new InvalidDataException($"'{source}' is shorter than an archive header.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != ArchiveFormat.Magic)
            throw new InvalidDataException($"'{source}' is not a MelangeDB backup archive.");
        var version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != ArchiveFormat.FormatVersion)
            throw new InvalidDataException($"'{source}' has archive format version {version}; this build reads version {ArchiveFormat.FormatVersion}.");
        if (BinaryPrimitives.ReadUInt16LittleEndian(header[6..]) != 0)
            throw new InvalidDataException($"'{source}': corrupt archive header (reserved bytes are not zero).");
    }

    /// <summary>Reads the next frame, or null at a clean end of stream.</summary>
    public (ArchiveFrameType Type, byte[] Payload)? ReadFrame()
    {
        if (_pushedBack is { } held)
        {
            _pushedBack = null;
            return held;
        }

        Span<byte> frame = stackalloc byte[ArchiveFormat.FrameHeaderSize];
        var first = stream.ReadByte();
        if (first < 0)
            return null;
        FrameIndex++;
        frame[0] = (byte)first;
        if (!TryReadExactly(frame[1..]))
            throw new InvalidDataException($"'{source}': frame {FrameIndex} has a torn header; the archive is truncated or corrupt.");
        var length = BinaryPrimitives.ReadUInt32LittleEndian(frame[1..]);
        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(frame[5..]);
        if (length > ArchiveFormat.MaxFramePayload)
            throw new InvalidDataException($"'{source}': frame {FrameIndex} declares an impossible length; the archive is corrupt.");
        var payload = new byte[length];
        if (!TryReadExactly(payload))
            throw new InvalidDataException($"'{source}': frame {FrameIndex} extends past the end of the archive; the archive is truncated or corrupt.");
        var crc = Crc32.Begin();
        crc = Crc32.Append(crc, frame[..5]);
        crc = Crc32.Append(crc, payload);
        if (Crc32.End(crc) != expectedCrc)
            throw new InvalidDataException($"'{source}': CRC mismatch at frame {FrameIndex}; the archive is corrupt.");
        var type = (ArchiveFrameType)frame[0];
        if (type is < ArchiveFrameType.Manifest or > ArchiveFrameType.ArchiveEnd)
            throw new InvalidDataException($"'{source}': frame {FrameIndex} has unknown type {frame[0]}; the archive is corrupt.");
        return (type, payload);
    }

    /// <summary>
    /// Returns a frame so the next <see cref="ReadFrame"/> yields it again — how a streamed row
    /// enumerable discovers its section ended without consuming the frame that ended it.
    /// </summary>
    public void PushBack((ArchiveFrameType Type, byte[] Payload) frame) => _pushedBack = frame;

    private bool TryReadExactly(Span<byte> buffer)
    {
        var filled = 0;
        while (filled < buffer.Length)
        {
            var read = stream.Read(buffer[filled..]);
            if (read == 0)
                return false;
            filled += read;
        }

        return true;
    }
}
