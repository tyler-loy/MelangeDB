namespace MelangeDB;

/// <summary>
/// One committed transaction as recorded in the commit log: the LSN, timestamp, and caller, the
/// reducer name and arguments as metadata, and the write set as the authoritative payload.
/// </summary>
public sealed class CommitRecord
{
    /// <summary>Log sequence number; monotonic within a shard's log, assigned at append.</summary>
    public required ulong Lsn { get; init; }

    /// <summary>The record format version this record was written with.</summary>
    public required ushort FormatVersion { get; init; }

    public required Timestamp Timestamp { get; init; }

    public required Identity Caller { get; init; }

    /// <summary>The reducer that produced this transaction. Metadata for audit, never replayed.</summary>
    public required string ReducerName { get; init; }

    /// <summary>The reducer's serialized arguments. Metadata for audit, never replayed.</summary>
    public required ReadOnlyMemory<byte> Arguments { get; init; }

    /// <summary>The authoritative payload: the collapsed, ordered row operations of the transaction.</summary>
    public required IReadOnlyList<RowOp> WriteSet { get; init; }

    /// <summary>The record's size on disk in bytes, framing included.</summary>
    public required int SerializedLength { get; init; }
}

/// <summary>The payload handed to <see cref="ICommitLog.Append"/>; the log assigns the LSN.</summary>
public readonly record struct CommitRequest(
    Timestamp Timestamp,
    Identity Caller,
    string ReducerName,
    ReadOnlyMemory<byte> Arguments,
    IReadOnlyList<RowOp> WriteSet);
