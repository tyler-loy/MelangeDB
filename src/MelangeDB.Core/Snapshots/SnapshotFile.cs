using System.Buffers.Binary;

namespace MelangeDB.Core;

/// <summary>
/// The snapshot file: a full materialized state capture at one LSN, letting the log behind it be
/// truncated. Format is <b>full, not incremental</b> — settled in phase 07: at this scale a full
/// dump is seconds of sequential I/O, an incremental chain would put a second replay mechanism
/// beside the log (which already is one), and restart cost stays flat instead of growing with
/// chain length. One current snapshot exists at a time, written to a temp file and atomically
/// swapped in, so a crash mid-write leaves the previous snapshot intact. CRC-guarded like the log;
/// a corrupt snapshot fails recovery loudly rather than silently rebuilding partial state.
/// </summary>
internal static class SnapshotFile
{
    private const uint Magic = 0x504E534Du; // "MSNP"
    private const ushort FormatVersion = 1;

    public const string FileName = "melange.snapshot";

    /// <summary>
    /// The durable floor a data directory's snapshot proves: the snapshot's LSN when one exists
    /// and counts against the directory's current epoch, else zero. Sound because the snapshot
    /// path forces the log durable through the pending LSN before this file is written — so a
    /// record at or below the returned LSN provably survived an fsync, which is what lets log
    /// recovery tell damaged committed history (fatal) from a crash's torn tail (truncated).
    /// Zero on any doubt: a missing or unreadable snapshot or epoch just makes recovery lenient,
    /// never wrong in the fatal direction.
    /// </summary>
    public static ulong DurableFloor(string logDirectory)
    {
        var snapshotPath = System.IO.Path.Combine(logDirectory, FileName);
        var epochPath = System.IO.Path.Combine(logDirectory, FileCommitLog.EpochFileName);
        if (!File.Exists(snapshotPath) || !File.Exists(epochPath))
            return 0;
        try
        {
            var epochBytes = File.ReadAllBytes(epochPath);
            if (epochBytes.Length != 16)
                return 0;
            using var reader = new SnapshotReader(snapshotPath);
            return reader.Header.Epoch == new Guid(epochBytes) ? reader.Header.Lsn : 0;
        }
        catch (Exception exception) when (exception is IOException or InvalidDataException)
        {
            return 0;
        }
    }

    /// <summary>What a snapshot carries besides rows: identity, position, and sequencer state.</summary>
    public sealed class Header
    {
        public required Guid Epoch { get; init; }

        public required ulong Lsn { get; init; }

        /// <summary>The tail record's timestamp at capture — the downtime anchor when the log tail is empty.</summary>
        public required Timestamp Timestamp { get; init; }

        public required IReadOnlyList<KeyValuePair<TableId, ulong>> Sequences { get; init; }
    }

    /// <summary>
    /// Writes a snapshot atomically: temp file, flush to disk, then replace. The row source is
    /// streamed, so a snapshot never buffers the whole store in memory on top of the store itself.
    /// </summary>
    public static void Write(
        string path,
        Header header,
        IEnumerable<(TableId Table, IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Rows)> tables)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);
        var tempPath = path + ".tmp";
        using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
        {
            var crc = Crc32.Begin();
            Span<byte> scratch = stackalloc byte[16];

            BinaryPrimitives.WriteUInt32LittleEndian(scratch, Magic);
            BinaryPrimitives.WriteUInt16LittleEndian(scratch[4..], FormatVersion);
            BinaryPrimitives.WriteUInt16LittleEndian(scratch[6..], 0);
            stream.Write(scratch[..8]);

            crc = WriteChecked(stream, crc, header.Epoch.ToByteArray());
            BinaryPrimitives.WriteUInt64LittleEndian(scratch, header.Lsn);
            BinaryPrimitives.WriteInt64LittleEndian(scratch[8..], header.Timestamp.UnixTimeMicroseconds);
            crc = WriteChecked(stream, crc, scratch);

            BinaryPrimitives.WriteInt32LittleEndian(scratch, header.Sequences.Count);
            crc = WriteChecked(stream, crc, scratch[..4]);
            foreach (var (table, next) in header.Sequences)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(scratch, table.Value);
                BinaryPrimitives.WriteUInt64LittleEndian(scratch[4..], next);
                crc = WriteChecked(stream, crc, scratch[..12]);
            }

            foreach (var (table, rows) in tables)
            {
                foreach (var (key, row) in rows)
                {
                    scratch[0] = 1; // Row-entry tag; the stream is terminated by tag 0.
                    BinaryPrimitives.WriteUInt32LittleEndian(scratch[1..], table.Value);
                    BinaryPrimitives.WriteInt32LittleEndian(scratch[5..], key.Length);
                    BinaryPrimitives.WriteInt32LittleEndian(scratch[9..], row.Length);
                    crc = WriteChecked(stream, crc, scratch[..13]);
                    crc = WriteChecked(stream, crc, key.Span);
                    crc = WriteChecked(stream, crc, row.Span);
                }
            }

            scratch[0] = 0;
            crc = WriteChecked(stream, crc, scratch[..1]);

            BinaryPrimitives.WriteUInt32LittleEndian(scratch, Crc32.End(crc));
            stream.Write(scratch[..4]);
            stream.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
    }

    /// <summary>Opens a snapshot for streamed reading; the header is parsed eagerly.</summary>
    public static SnapshotReader Open(string path) => new(path);

    private static uint WriteChecked(FileStream stream, uint crc, ReadOnlySpan<byte> data)
    {
        stream.Write(data);
        return Crc32.Append(crc, data);
    }

    internal static uint ReadChecked(FileStream stream, uint crc, Span<byte> buffer)
    {
        stream.ReadExactly(buffer);
        return Crc32.Append(crc, buffer);
    }

    internal const uint MagicValue = Magic;

    internal const ushort FormatVersionValue = FormatVersion;
}

/// <summary>
/// The reading half of <see cref="SnapshotFile"/>: the header up front, then rows as a stream so a
/// store can load a snapshot larger than memory without a materialized row list in between. The
/// CRC covers the whole file and is validated when the row stream completes; any corruption throws
/// <see cref="InvalidDataException"/> — a damaged snapshot with a truncated log behind it is
/// unrecoverable state and must fail loudly, not rebuild partial state.
/// </summary>
internal sealed class SnapshotReader : IDisposable
{
    private readonly string _path;
    private readonly FileStream _stream;
    private uint _crc;

    public SnapshotReader(string path)
        : this(path, new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
    {
    }

    /// <summary>
    /// Reads through a caller-opened handle. The online backup opens the snapshot with write and
    /// delete sharing so a concurrent snapshot completion can atomically replace the file mid-read
    /// — the open handle keeps the old, complete content, the log's lazy-reader convention.
    /// </summary>
    internal SnapshotReader(string path, FileStream stream)
    {
        _path = path;
        _stream = stream;
        try
        {
            _crc = Crc32.Begin();
            Span<byte> scratch = stackalloc byte[16];

            _stream.ReadExactly(scratch[..8]);
            if (BinaryPrimitives.ReadUInt32LittleEndian(scratch) != SnapshotFile.MagicValue)
                throw new InvalidDataException($"'{path}' is not a MelangeDB snapshot.");
            var version = BinaryPrimitives.ReadUInt16LittleEndian(scratch[4..]);
            if (version != SnapshotFile.FormatVersionValue)
                throw new InvalidDataException($"'{path}' has snapshot format version {version}; this build reads version {SnapshotFile.FormatVersionValue}.");

            _crc = SnapshotFile.ReadChecked(_stream, _crc, scratch);
            var epoch = new Guid(scratch);
            _crc = SnapshotFile.ReadChecked(_stream, _crc, scratch);
            var lsn = BinaryPrimitives.ReadUInt64LittleEndian(scratch);
            var timestamp = new Timestamp(BinaryPrimitives.ReadInt64LittleEndian(scratch[8..]));

            _crc = SnapshotFile.ReadChecked(_stream, _crc, scratch[..4]);
            var sequenceCount = BinaryPrimitives.ReadInt32LittleEndian(scratch);
            if (sequenceCount < 0)
                throw new InvalidDataException($"'{path}': negative sequence count.");
            var sequences = new List<KeyValuePair<TableId, ulong>>(sequenceCount);
            for (var i = 0; i < sequenceCount; i++)
            {
                _crc = SnapshotFile.ReadChecked(_stream, _crc, scratch[..12]);
                sequences.Add(new KeyValuePair<TableId, ulong>(
                    new TableId(BinaryPrimitives.ReadUInt32LittleEndian(scratch)),
                    BinaryPrimitives.ReadUInt64LittleEndian(scratch[4..])));
            }

            Header = new SnapshotFile.Header { Epoch = epoch, Lsn = lsn, Timestamp = timestamp, Sequences = sequences };
        }
        catch
        {
            _stream.Dispose();
            throw;
        }
    }

    public SnapshotFile.Header Header { get; }

    /// <summary>
    /// Streams the snapshot's rows in file order, validating the trailing CRC after the last one.
    /// Enumerate exactly once, fully.
    /// </summary>
    public IEnumerable<SnapshotRow> Rows()
    {
        var scratch = new byte[16];
        while (true)
        {
            _crc = SnapshotFile.ReadChecked(_stream, _crc, scratch.AsSpan(0, 1));
            if (scratch[0] == 0)
                break;
            _crc = SnapshotFile.ReadChecked(_stream, _crc, scratch.AsSpan(0, 12));
            var tableValue = BinaryPrimitives.ReadUInt32LittleEndian(scratch);
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(scratch.AsSpan(4));
            var rowLength = BinaryPrimitives.ReadInt32LittleEndian(scratch.AsSpan(8));
            if (keyLength < 0 || rowLength < 0)
                throw new InvalidDataException($"'{_path}': negative row framing.");
            var keyBytes = new byte[keyLength];
            _stream.ReadExactly(keyBytes);
            _crc = Crc32.Append(_crc, keyBytes);
            var rowBytes = new byte[rowLength];
            _stream.ReadExactly(rowBytes);
            _crc = Crc32.Append(_crc, rowBytes);
            yield return new SnapshotRow(new TableId(tableValue), new RowKey(keyBytes), rowBytes);
        }

        _stream.ReadExactly(scratch.AsSpan(0, 4));
        if (BinaryPrimitives.ReadUInt32LittleEndian(scratch) != Crc32.End(_crc))
            throw new InvalidDataException($"'{_path}': snapshot CRC mismatch; the snapshot is corrupt. Restore from backup.");
    }

    public void Dispose() => _stream.Dispose();
}
