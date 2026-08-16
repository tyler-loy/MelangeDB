using System.Buffers.Binary;

namespace MelangeDB.Core;

/// <summary>
/// Read-only access to a commit-log <em>file</em>, for callers that must never mutate it.
/// Constructing a <see cref="FileCommitLog"/> runs recovery — which mints epochs, deletes stale
/// base sidecars, and truncates torn tails — and a backup's whole contract is that it changes
/// nothing it reads. The walk applies the same judgements recovery does, without acting on them:
/// a torn tail (which recovery would truncate) simply ends the walk, and corruption before the
/// tail (which recovery would refuse to boot) refuses the backup, because archiving damaged
/// history as if it were good would turn a bad day into a silent one.
/// </summary>
internal static class LogFileFormat
{
    /// <summary>One surviving record: its LSN and its verbatim codec payload.</summary>
    public readonly record struct RawRecord(ulong Lsn, byte[] Payload);

    public static void ValidateHeader(Stream stream, string path)
    {
        Span<byte> header = stackalloc byte[FileCommitLog.HeaderSize];
        stream.Seek(0, SeekOrigin.Begin);
        stream.ReadExactly(header);
        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != FileCommitLog.Magic)
            throw new InvalidDataException($"'{path}' is not a MelangeDB commit log.");
        var version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != FileCommitLog.FileFormatVersion)
            throw new InvalidDataException($"'{path}' has log format version {version}; this build reads version {FileCommitLog.FileFormatVersion}.");
    }

    /// <summary>
    /// Walks every intact record in file order. Torn-tail conditions (recovery's exact four) end
    /// the walk silently; a CRC mismatch with intact records after it throws, mirroring the
    /// "corrupt beyond a torn tail" refusal. A file shorter than its header yields nothing — a
    /// created-but-never-recovered log is an empty engine, not an error.
    /// </summary>
    public static IEnumerable<RawRecord> WalkRecords(Stream stream, string path)
    {
        if (stream.Length < FileCommitLog.HeaderSize)
            yield break;
        ValidateHeader(stream, path);
        var frame = new byte[FileCommitLog.FrameSize];
        var length = stream.Length;
        var position = (long)FileCommitLog.HeaderSize;
        while (position < length)
        {
            if (position + FileCommitLog.FrameSize > length)
                yield break; // Torn frame header.
            stream.Seek(position, SeekOrigin.Begin);
            stream.ReadExactly(frame);
            var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(frame);
            var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(4));
            if (payloadLength == 0)
                yield break; // Zero-filled torn tail; CRC32 of zero bytes is zero.
            if (payloadLength > FileCommitLog.MaxRecordBytes || position + FileCommitLog.FrameSize + payloadLength > length)
                yield break; // Record extends past end of file.
            var payload = new byte[payloadLength];
            stream.ReadExactly(payload);
            if (Crc32.Compute(payload) != expectedCrc)
            {
                if (position + FileCommitLog.FrameSize + payloadLength == length)
                    yield break; // CRC mismatch on the trailing record: a torn tail.
                throw new InvalidDataException(
                    $"'{path}': CRC mismatch at offset {position} with intact records after it. " +
                    "The log is corrupt beyond a torn tail and cannot be backed up; restore from an earlier backup.");
            }

            // The LSN sits at a fixed offset in every payload version: u16 format, then u64 LSN.
            yield return new RawRecord(BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(2)), payload);
            position += FileCommitLog.FrameSize + payloadLength;
        }
    }

    /// <summary>
    /// Materializes a commit-log file from verbatim record payloads — restore's writing half.
    /// The frame CRCs are recomputed from the payloads, which the archive already CRC-verified.
    /// </summary>
    public static void WriteLogFile(string path, IEnumerable<byte[]> payloads)
    {
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        Span<byte> scratch = stackalloc byte[FileCommitLog.HeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(scratch, FileCommitLog.Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(scratch[4..], FileCommitLog.FileFormatVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(scratch[6..], 0);
        stream.Write(scratch);
        foreach (var payload in payloads)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(scratch, (uint)payload.Length);
            BinaryPrimitives.WriteUInt32LittleEndian(scratch[4..], Crc32.Compute(payload));
            stream.Write(scratch);
            stream.Write(payload);
        }

        stream.Flush(flushToDisk: true);
    }
}
