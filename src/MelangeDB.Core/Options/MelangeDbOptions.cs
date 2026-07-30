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

    public ValidationOptions Validation { get; set; } = new();

    public TransportOptions Transport { get; set; } = new();

    public SubscriptionsOptions Subscriptions { get; set; } = new();

    public ResumeOptions Resume { get; set; } = new();
}

/// <summary>Options for the websocket and HTTP transport (<c>MelangeDb:Transport:*</c>).</summary>
public sealed class TransportOptions
{
    /// <summary>The endpoint path <c>MapMelangeSocket</c> maps when the host passes none.</summary>
    public string Path { get; set; } = "/melange";

    /// <summary>Maximum size of one inbound frame. Read per message, so a change is live.</summary>
    public int MaxMessageBytes { get; set; } = 4 * 1024 * 1024;

    /// <summary>The wire serializer behind <c>IMelangeSerializer</c>.</summary>
    public WireSerializer Serializer { get; set; } = WireSerializer.MessagePack;

    /// <summary>
    /// Enables <c>permessage-deflate</c> on accepted sockets. Terrain blobs are already
    /// RLE-compressed; delta frames of many small rows are what benefit.
    /// </summary>
    public bool CompressionEnabled { get; set; } = true;

    /// <summary>How often the server pings an idle connection.</summary>
    public int HeartbeatIntervalMs { get; set; } = 15_000;

    /// <summary>
    /// Silence longer than this closes the connection. A closed socket is not the only way a
    /// client goes away; this is what makes ungraceful drops observable.
    /// </summary>
    public int HeartbeatTimeoutMs { get; set; } = 45_000;

    /// <summary>One-shot reducer calls, bulk ingestion, ad-hoc SQL, and tickets over plain HTTP.</summary>
    public bool HttpEndpointsEnabled { get; set; } = true;

    /// <summary>
    /// Initial result sets are chunked at this size and interleaved with interactive frames, so a
    /// large terrain subscription cannot head-of-line block a reducer response.
    /// </summary>
    public int MaxInitialSetChunkBytes { get; set; } = 256 * 1024;
}

/// <summary>The wire serializer implementations <c>Transport:Serializer</c> may name.</summary>
public enum WireSerializer
{
    /// <summary>MessagePack framing — implementations exist in every client language we target.</summary>
    MessagePack,
}

/// <summary>What happens when a connection's send buffer exceeds <see cref="SubscriptionsOptions.MaxBufferedBytes"/>.</summary>
public enum BackpressurePolicy
{
    /// <summary>
    /// Drop the connection's queued delta frames and tell the client to re-establish its
    /// subscriptions. Bounded memory, and the client converges through the same full-resync path
    /// Resume already needs. The default.
    /// </summary>
    DropAndResync,

    /// <summary>
    /// Keep buffering past the limit. Memory is unbounded in effect; an explicit opt-in for
    /// trusted links where a transient stall is known to clear.
    /// </summary>
    Buffer,

    /// <summary>Close the connection. The bluntest answer; the client reconnects from scratch.</summary>
    Disconnect,
}

/// <summary>Subscription caps (<c>MelangeDb:Subscriptions:*</c>) — the denial-of-service surface.</summary>
public sealed class SubscriptionsOptions
{
    /// <summary>How many live subscriptions one connection may hold.</summary>
    public int MaxPerConnection { get; set; } = 64;

    /// <summary>Applied when a connection's buffered outbound bytes exceed <see cref="MaxBufferedBytes"/>.</summary>
    public BackpressurePolicy BackpressurePolicy { get; set; } = BackpressurePolicy.DropAndResync;

    /// <summary>Per-connection ceiling on buffered outbound bytes; the trigger for the policy above.</summary>
    public long MaxBufferedBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>Ceiling on an initial result set's row count. Rejected before execution, not mid-stream.</summary>
    public long MaxRowsPerSubscription { get; set; } = 100_000;

    /// <summary>Ceiling on an initial result set's stored bytes — the one that matters for blob tables.</summary>
    public long MaxBytesPerSubscription { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    /// Maximum width of a <c>BETWEEN</c> predicate on an integer column: a client streams a ring
    /// around itself, not the map.
    /// </summary>
    public long MaxRangeSpan { get; set; } = 1024;

    /// <summary>
    /// Tables where an unconstrained subscription is rejected. An entry is either a table name
    /// (any predicate required) or <c>Table.Column</c> (the predicate must constrain that column).
    /// </summary>
    public string[] RequirePredicateOn { get; set; } = [];
}

/// <summary>Resume retention (<c>MelangeDb:Resume:*</c>).</summary>
public sealed class ResumeOptions
{
    /// <summary>
    /// How far back, in seconds, a reconnecting client may resume. A time window rather than a
    /// transaction count, because what matters is surviving a plausible network outage. Gaps whose
    /// oldest missed record is older than this answer full resync instead.
    /// </summary>
    public int RetentionWindowSeconds { get; set; } = 300;
}

/// <summary>
/// Caps applied to reducer arguments while they are decoded (<c>MelangeDb:Validation:*</c>) —
/// before any transaction opens, so a rejected call appends nothing. The framework cannot check
/// semantics, but it rejects the inputs that corrupt state regardless of game rules.
/// </summary>
public sealed class ValidationOptions
{
    /// <summary>
    /// Rejects <see cref="double.NaN"/> and ±infinity float arguments. A NaN position propagates
    /// through terrain and chunk math and poisons rows that then replicate to every client.
    /// Turning this off should feel alarming.
    /// </summary>
    public bool RejectNonFiniteFloats { get; set; } = true;

    /// <summary>Maximum length, in characters, of a string argument.</summary>
    public int MaxStringLength { get; set; } = 4096;

    /// <summary>Maximum element count of an array or blob argument.</summary>
    public int MaxCollectionLength { get; set; } = 4096;
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

    /// <summary>
    /// Reducers running longer than this many milliseconds get a span event and a warning log
    /// entry. Read per invocation, so a changed value takes effect on the next call.
    /// </summary>
    public int SlowReducerMs { get; set; } = 50;

    /// <summary>
    /// The fraction of delta fan-outs that get a <c>melange.subscription.delta</c> span. Deltas
    /// are the highest-frequency operation in the system; tracing every one would cost more than
    /// the work.
    /// </summary>
    public double DeltaSpanSampleRatio { get; set; } = 0.01;
}
