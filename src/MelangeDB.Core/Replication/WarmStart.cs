namespace MelangeDB.Core;

/// <summary>
/// A follower's projection handed to a new owner's engine (road-to-0.4 phase 30, step 2e): the
/// store, the AutoInc sequences and the cursor they stand at, so the engine replays only the
/// records after <see cref="AppliedLsn"/> instead of recovering from the snapshot. The engine
/// takes ownership of <see cref="Store"/> whether it uses it or falls back to a cold recovery.
/// </summary>
internal sealed class WarmStart
{
    public required IHotStore Store { get; init; }

    public required AutoIncSequencer Sequencer { get; init; }

    /// <summary>The newest LSN the store reflects.</summary>
    public required ulong AppliedLsn { get; init; }

    /// <summary>The log epoch the projection was built from; any other epoch's log is not its history.</summary>
    public required Guid Epoch { get; init; }

    /// <summary>The applied tail's commit timestamp; null when nothing has been applied.</summary>
    public Timestamp? TailTimestamp { get; init; }
}
