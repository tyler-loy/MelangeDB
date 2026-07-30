namespace MelangeDB.Core;

/// <summary>
/// Root options for a MelangeDB engine. Binds from the <c>MelangeDb:</c> configuration section;
/// see docs/CONFIGURATION.md for every key, default, and reload semantic.
/// </summary>
public sealed class MelangeDbOptions
{
    public HotStoreOptions HotStore { get; set; } = new();

    public CommitLogOptions CommitLog { get; set; } = new();

    public TelemetryOptions Telemetry { get; set; } = new();
}

/// <summary>Options for the hot store (<c>MelangeDb:HotStore:*</c>).</summary>
public sealed class HotStoreOptions
{
    /// <summary>
    /// Directory for the hot store's files. The in-memory engine persists nothing here — it is a
    /// projection rebuilt from the log — but the directory is created so the setting is honest
    /// before the paging engine lands.
    /// </summary>
    public string Path { get; set; } = "./data/hot";
}

/// <summary>When the commit log forces appended records to stable storage.</summary>
public enum FsyncPolicy
{
    /// <summary>
    /// Fsync inside every append. The only durable choice: a record is on stable storage before
    /// the commit returns.
    /// </summary>
    OnCommit,

    /// <summary>
    /// Fsync on a timer (<see cref="CommitLogOptions.FsyncIntervalMs"/>). Trades a bounded window
    /// of committed-but-lost transactions for throughput; the interval is the size of that window.
    /// </summary>
    Interval,

    /// <summary>
    /// Never fsync explicitly; the OS flushes when it pleases. The whole page cache is the
    /// data-loss window. For tests and workloads that can rebuild.
    /// </summary>
    OsBuffered,
}

/// <summary>Options for the commit log (<c>MelangeDb:CommitLog:*</c>).</summary>
public sealed class CommitLogOptions
{
    /// <summary>Directory holding the log file.</summary>
    public string Path { get; set; } = "./data/log";

    /// <summary>Read per operation, so a change takes effect on the next commit.</summary>
    public FsyncPolicy FsyncPolicy { get; set; } = FsyncPolicy.OnCommit;

    /// <summary>Only read when <see cref="FsyncPolicy"/> is <see cref="FsyncPolicy.Interval"/>.</summary>
    public int FsyncIntervalMs { get; set; } = 100;
}

/// <summary>Options for what MelangeDB emits (<c>MelangeDb:Telemetry:*</c>). Exporters are the host's business.</summary>
public sealed class TelemetryOptions
{
    /// <summary>Off short-circuits instrumentation entirely. Read at engine startup.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Adds caller identity to spans only — never a metric dimension.</summary>
    public bool IncludeCallerIdentity { get; set; } = true;

    /// <summary>
    /// Off by default: arguments can contain anything, including secrets, and the commit log
    /// already records them.
    /// </summary>
    public bool IncludeReducerArguments { get; set; }
}
