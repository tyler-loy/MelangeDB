namespace MelangeDB;

/// <summary>
/// A component consuming the commit log to advance one projection. Each applier holds its own LSN
/// checkpoint, so appliers lag independently and resume from their own position.
/// </summary>
public interface ILogApplier
{
    /// <summary>
    /// The applier's stable name. Bounded — it is used as a metric dimension
    /// (<c>melange.applier.lag</c>).
    /// </summary>
    string Name { get; }

    /// <summary>This applier's checkpoint: the LSN of the last record it has applied.</summary>
    ulong AppliedLsn { get; }

    /// <summary>Applies one record and advances the checkpoint.</summary>
    void Apply(CommitRecord record);
}
