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
    private readonly Func<CommitRecord, CommitRecord>? _transform;
    private readonly Func<ulong, IEnumerable<CommitRecord>> _catchUpReader;
    private readonly List<Entry> _entries = [];
    private readonly Lock _lock = new();

    public ApplierPipeline(ICommitLog log)
        : this(log, null)
    {
    }

    internal ApplierPipeline(
        ICommitLog log,
        EngineTelemetry? telemetry,
        Func<CommitRecord, CommitRecord>? transform = null,
        Func<ulong, IEnumerable<CommitRecord>>? catchUpReader = null)
    {
        ArgumentNullException.ThrowIfNull(log);
        _log = log;
        _telemetry = telemetry;
        _transform = transform;

        // Pipeline-driven appliers are in-process projections, so their catch-up must not stop at
        // the durability watermark: NotifyAppended hands them a record whose commit is still
        // waiting for its fsync, and a capped read would silently skip it. The engine wires the
        // log's uncapped read here; decoupled appliers read the log themselves through the capped
        // public path, which is exactly right for effects that leave the process.
        _catchUpReader = catchUpReader ?? log.ReadFrom;
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

    /// <summary>
    /// Registers an applier that advances on its own dispatch loop, off the commit path — the
    /// Postgres applier's shape. The pipeline never calls <see cref="ILogApplier.Apply"/> on it;
    /// it only tracks the checkpoint, so the applier still shows in <c>melange.applier.lag</c>
    /// and still floors log truncation like any other applier.
    /// </summary>
    public void RegisterDecoupled(ILogApplier applier)
    {
        ArgumentNullException.ThrowIfNull(applier);
        lock (_lock)
        {
            if (_entries.Any(e => e.Applier.Name == applier.Name))
                throw new ArgumentException($"An applier named '{applier.Name}' is already registered.", nameof(applier));
            _entries.Add(new Entry(applier) { Decoupled = true });
        }
    }

    /// <summary>Stops advancing the named applier. Its checkpoint holds; its lag grows.</summary>
    public void Pause(string name)
    {
        lock (_lock)
        {
            var entry = Find(name);
            if (entry.Decoupled)
                throw new InvalidOperationException($"Applier '{name}' advances on its own dispatch loop; the pipeline cannot pause it.");
            entry.Paused = true;
        }
    }

    /// <summary>Resumes the named applier and immediately catches it up from its checkpoint.</summary>
    public void Resume(string name)
    {
        Entry entry;
        lock (_lock)
        {
            entry = Find(name);
            if (entry.Decoupled)
                throw new InvalidOperationException($"Applier '{name}' advances on its own dispatch loop; the pipeline cannot resume it.");
            entry.Paused = false;
        }

        CatchUp(entry);
    }

    /// <summary>Advances every unpaused, pipeline-driven applier to the log head.</summary>
    public void CatchUpAll()
    {
        foreach (var entry in Snapshot())
        {
            if (!entry.Paused && !entry.Decoupled)
                CatchUp(entry);
        }
    }

    /// <summary>
    /// Hands a freshly appended record to every unpaused, caught-up applier; an applier that fell
    /// behind is caught up from the log instead. Decoupled appliers are skipped — their own loops
    /// advance them, and blocking the commit path on them is exactly what decoupling forbids.
    /// </summary>
    internal void NotifyAppended(CommitRecord record)
    {
        foreach (var entry in Snapshot())
        {
            if (entry.Paused || entry.Decoupled)
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

        // Catch-up reads may reach below the current shape's reign (a checkpoint that lagged
        // across a migration boot), so records go through the shape transform here — the freshly
        // appended records in NotifyAppended are current-shape by construction and skip it.
        // Decoupled appliers read the log themselves and own the same obligation; see
        // MelangeEngine.TransformToCurrentShape.
        foreach (var record in _catchUpReader(entry.Applier.AppliedLsn + 1))
            entry.Applier.Apply(_transform is null ? record : _transform(record));
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

        /// <summary>True for appliers the pipeline tracks but never drives; see <see cref="RegisterDecoupled"/>.</summary>
        public bool Decoupled { get; init; }
    }
}
