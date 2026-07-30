namespace MelangeDB.Core;

/// <summary>
/// Drives registered <see cref="ILogApplier"/>s. Each applier holds its own LSN checkpoint, so a
/// paused or slow applier lags independently and resumes from its own position; the gap between
/// the log head and each checkpoint is exported as <c>melange.applier.lag</c>.
/// </summary>
public sealed class ApplierPipeline
{
    private readonly ICommitLog _log;
    private readonly EngineTelemetry? _telemetry;
    private readonly List<Entry> _entries = [];
    private readonly Lock _lock = new();

    public ApplierPipeline(ICommitLog log)
        : this(log, null)
    {
    }

    internal ApplierPipeline(ICommitLog log, EngineTelemetry? telemetry)
    {
        ArgumentNullException.ThrowIfNull(log);
        _log = log;
        _telemetry = telemetry;
    }

    public IReadOnlyList<ILogApplier> Appliers
    {
        get
        {
            lock (_lock)
            {
                return _entries.Select(e => e.Applier).ToArray();
            }
        }
    }

    /// <summary>Registers an applier. It catches up from its own checkpoint on the next advance.</summary>
    public void Register(ILogApplier applier)
    {
        ArgumentNullException.ThrowIfNull(applier);
        lock (_lock)
        {
            if (_entries.Any(e => e.Applier.Name == applier.Name))
                throw new ArgumentException($"An applier named '{applier.Name}' is already registered.", nameof(applier));
            _entries.Add(new Entry(applier));
        }
    }

    /// <summary>Stops advancing the named applier. Its checkpoint holds; its lag grows.</summary>
    public void Pause(string name)
    {
        lock (_lock)
        {
            Find(name).Paused = true;
        }
    }

    /// <summary>Resumes the named applier and immediately catches it up from its checkpoint.</summary>
    public void Resume(string name)
    {
        Entry entry;
        lock (_lock)
        {
            entry = Find(name);
            entry.Paused = false;
        }

        CatchUp(entry);
    }

    /// <summary>Advances every unpaused applier to the log head.</summary>
    public void CatchUpAll()
    {
        foreach (var entry in Snapshot())
        {
            if (!entry.Paused)
                CatchUp(entry);
        }
    }

    /// <summary>
    /// Hands a freshly appended record to every unpaused, caught-up applier; an applier that fell
    /// behind is caught up from the log instead.
    /// </summary>
    internal void NotifyAppended(CommitRecord record)
    {
        foreach (var entry in Snapshot())
        {
            if (entry.Paused)
                continue;
            if (entry.Applier.AppliedLsn == record.Lsn - 1)
            {
                using var activity = _telemetry?.StartApply(entry.Applier.Name);
                entry.Applier.Apply(record);
            }
            else
            {
                CatchUp(entry);
            }
        }
    }

    internal IEnumerable<(string Applier, long Lag)> Lags()
    {
        var head = _log.HeadLsn;
        foreach (var entry in Snapshot())
            yield return (entry.Applier.Name, (long)(head - Math.Min(head, entry.Applier.AppliedLsn)));
    }

    private void CatchUp(Entry entry)
    {
        if (entry.Applier.AppliedLsn >= _log.HeadLsn)
            return;
        using var activity = _telemetry?.StartApply(entry.Applier.Name);
        foreach (var record in _log.ReadFrom(entry.Applier.AppliedLsn + 1))
            entry.Applier.Apply(record);
    }

    private Entry Find(string name) =>
        _entries.FirstOrDefault(e => e.Applier.Name == name)
        ?? throw new ArgumentException($"No applier named '{name}' is registered.", nameof(name));

    private Entry[] Snapshot()
    {
        lock (_lock)
        {
            return [.. _entries];
        }
    }

    private sealed class Entry(ILogApplier applier)
    {
        public ILogApplier Applier { get; } = applier;

        public bool Paused { get; set; }
    }
}
