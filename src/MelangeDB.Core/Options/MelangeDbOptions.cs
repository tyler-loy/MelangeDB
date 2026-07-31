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

    public AuthOptions Auth { get; set; } = new();

    public PoliciesOptions Policies { get; set; } = new();

    public RateLimitOptions RateLimit { get; set; } = new();

    public SqlOptions Sql { get; set; } = new();

    public SchedulerOptions Scheduler { get; set; } = new();

    public EventsOptions Events { get; set; } = new();

    public ResidencyOptions Residency { get; set; } = new();

    public SnapshotsOptions Snapshots { get; set; } = new();

    public PostgresOptions Postgres { get; set; } = new();

    public DiagnosticsOptions Diagnostics { get; set; } = new();

    public HealthChecksOptions HealthChecks { get; set; } = new();

    public ClusterOptions Cluster { get; set; } = new();
}

/// <summary>
/// Options for the relational tier (<c>MelangeDb:Postgres:*</c>). The tier is opt-in: an absent
/// connection string means no Postgres at all, and a deployment with no relational tables needs
/// none. The implementation lives in the MelangeDB.Storage.Postgres package; these options bind
/// here so the whole <c>MelangeDb:</c> section stays one configuration surface.
/// </summary>
public sealed class PostgresOptions
{
    /// <summary>
    /// The Npgsql connection string. Absent means no relational tier — the zero-infra single-file
    /// story stays intact. Set by <c>AddPostgres(...)</c> or bound from configuration.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>The Postgres schema (namespace) relational tables and the applier checkpoint live in.</summary>
    public string Schema { get; set; } = "melange";

    /// <summary>
    /// Log records per Postgres transaction. The applier checkpoint advances only with the batch —
    /// batch and checkpoint commit atomically — so batching trades nothing for its speed.
    /// </summary>
    public int ApplyBatchSize { get; set; } = 100;

    /// <summary>
    /// Whether the applier creates missing tables and adds missing columns itself. Off by default:
    /// schema changes against a production database should be deliberate. Only additive changes
    /// are ever automatic — destructive ones are refused loudly in both settings.
    /// </summary>
    public bool AutoMigrate { get; set; }
}

/// <summary>Diagnostics toggles (<c>MelangeDb:Diagnostics:*</c>).</summary>
public sealed class DiagnosticsOptions
{
    /// <summary>
    /// Re-logs a stalled applier's growing lag periodically while the stall lasts. A silently
    /// stalled Postgres applier — writes succeeding while the tier falls hours behind — is the
    /// dangerous failure mode; the first stall always logs regardless of this setting.
    /// </summary>
    public bool ReportApplierLag { get; set; } = true;
}

/// <summary>Health check thresholds (<c>MelangeDb:HealthChecks:*</c>).</summary>
public sealed class HealthChecksOptions
{
    /// <summary>
    /// How many transactions behind an applier may be before the <c>melange-applier</c> check
    /// reports unhealthy.
    /// </summary>
    public long ApplierLagThreshold { get; set; } = 10_000;
}

/// <summary>
/// Per-table residency configuration (<c>MelangeDb:Residency:*</c>), overriding the
/// <c>[Table(Residency = ...)]</c> attribute. Configurable and not code-only on purpose: the right
/// residency set depends on deployment size, and an operator hitting a slow scan should be able to
/// fix it without a code change and a redeploy.
/// </summary>
public sealed class ResidencyOptions
{
    /// <summary>
    /// The residency for tables whose attribute leaves it unspecified. <b>Leaving this at
    /// <see cref="MelangeDB.Residency.Paged"/> is deliberate</b>: a resident-by-default store
    /// reproduces the RAM ceiling MelangeDB exists to remove, as a cliff arriving under production
    /// load. Because <see cref="MelangeDB.Residency.Paged"/> is also the attribute's default value,
    /// an attribute that explicitly says Paged is indistinguishable from one that says nothing —
    /// under a non-Paged default, pin a table back down with its per-table override.
    /// </summary>
    public Residency Default { get; set; } = Residency.Paged;

    /// <summary>
    /// The size at which an <see cref="MelangeDB.Residency.Auto"/> table stops being resident and
    /// starts paging. Only read for tables resolved to Auto.
    /// </summary>
    public long AutoThresholdBytes { get; set; } = 8_388_608;

    /// <summary>
    /// Logs each resident table's row count and measured bytes plus the buffer-pool cap at startup
    /// (EventId 1501). The memory budget has to be observable, not theoretical.
    /// </summary>
    public bool ReportOnStartup { get; set; } = true;

    /// <summary>
    /// Per-table overrides, keyed by table name — bound from the <c>MelangeDb:Residency</c>
    /// section's remaining keys (<c>Residency:&lt;TableName&gt;</c>), where config wins over the
    /// attribute.
    /// </summary>
    public Dictionary<string, Residency> PerTable { get; } = new(StringComparer.Ordinal);
}

/// <summary>Snapshot and log-compaction options (<c>MelangeDb:Snapshots:*</c>).</summary>
public sealed class SnapshotsOptions
{
    /// <summary>Whether snapshots are taken at all. Off leaves the log growing without bound.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>How many committed transactions between automatic snapshots.</summary>
    public long IntervalTransactions { get; set; } = 100_000;

    /// <summary>
    /// Whether a successful snapshot truncates the log behind it. Truncation never passes the
    /// slowest applier checkpoint, the slowest live event-subscriber checkpoint, or the Resume
    /// retention window, regardless of this setting.
    /// </summary>
    public bool TruncateLog { get; set; } = true;
}

/// <summary>The hot-store engines <c>HotStore:Engine</c> may name.</summary>
public enum HotStoreEngine
{
    /// <summary>
    /// Picks <see cref="Faster"/> when a FASTER store provider is registered
    /// (<c>UseFasterHotStore()</c>), else <see cref="InMemory"/> — selection by registration, not
    /// by path, since <c>HotStore:Path</c> always has a default.
    /// </summary>
    Auto,

    /// <summary>The dictionary-backed store. A legitimate choice, not just a test double.</summary>
    InMemory,

    /// <summary>The FASTER-backed paging store. Requires the MelangeDB.Storage.Faster package.</summary>
    Faster,
}

/// <summary>Options for the event bus (<c>MelangeDb:Events:*</c>).</summary>
public sealed class EventsOptions
{
    /// <summary>
    /// Ceiling on events held in the in-memory delivery window. Bounded on purpose: a slow handler
    /// must not grow memory without limit. Overflow evicts the oldest window entries — nothing is
    /// lost, because the commit log is the real buffer and a lagging subscriber replays from it,
    /// its checkpoint lag saying honestly how far behind it is.
    /// </summary>
    public int MaxQueueDepth { get; set; } = 10_000;

    /// <summary>Retries after a handler's first failed attempt, before the event dead-letters.</summary>
    public int HandlerRetries { get; set; } = 3;

    /// <summary>
    /// The first retry's backoff in milliseconds; each further retry doubles it, capped at 30
    /// seconds.
    /// </summary>
    public int RetryBackoffMs { get; set; } = 500;

    /// <summary>Directory for the dead-letter records (<c>melange.deadletter.ndjson</c>).</summary>
    public string DeadLetterPath { get; set; } = "./data/deadletter";

    /// <summary>
    /// Cycle guard for handlers that call reducers that publish. Each event carries the depth it
    /// was published at — one more than the event whose handler published it — and a publish at
    /// this depth throws, aborting the reducer. Bounds the event → reducer → event loop durably,
    /// across restarts included.
    /// </summary>
    public int MaxPublishDepth { get; set; } = 4;

    /// <summary>
    /// A checkpoint whose subscriber no longer exists — handler deleted from the code, service
    /// retired — would pin log truncation at a frozen LSN forever: a full disk on a timer. Idle
    /// past this window it is evicted with a loud log (EventId 1403); a subscriber returning after
    /// eviction has lost its place and starts from current state, told so (EventId 1404).
    /// Deliberate, bounded data loss, chosen over unbounded disk growth. Seven days default.
    /// </summary>
    public int SubscriberExpirySeconds { get; set; } = 604_800;
}

/// <summary>What happens when a tick takes longer than its timer's interval.</summary>
public enum SchedulerOverrunPolicy
{
    /// <summary>
    /// Fires missed by the overrun are skipped and logged; the next fire is one full interval
    /// after the slow tick completed. The default, because silent pile-up is how a simulation
    /// death-spirals under load.
    /// </summary>
    Skip,

    /// <summary>Every missed fire runs, back to back, until the timer catches up.</summary>
    RunImmediately,

    /// <summary>All missed fires collapse into one immediate fire, then the cadence resumes.</summary>
    Coalesce,
}

/// <summary>How overdue repeating timers behave after process downtime.</summary>
public enum SchedulerCatchUp
{
    /// <summary>
    /// An overdue repeating timer fires once at recovery, then resumes its cadence — downtime
    /// collapses any number of missed ticks into one. Right for a simulation, whose world was
    /// simply paused. The default.
    /// </summary>
    FireOnce,

    /// <summary>
    /// An overdue repeating timer fires once per interval the process was down, back to back.
    /// Right for billing-shaped work. Downtime is measured from the recovered log's tail record,
    /// since repeating timers deliberately persist no per-fire bookkeeping; exact catch-up
    /// accounting wants a self-rescheduling one-shot timer instead.
    /// </summary>
    CatchUpAll,
}

/// <summary>Options for the timer scheduler (<c>MelangeDb:Scheduler:*</c>).</summary>
public sealed class SchedulerOptions
{
    /// <summary>Off is useful for tooling processes that must not tick the world.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>See <see cref="SchedulerOverrunPolicy"/>. Read per fire, so a change is live.</summary>
    public SchedulerOverrunPolicy OverrunPolicy { get; set; } = SchedulerOverrunPolicy.Skip;

    /// <summary>See <see cref="SchedulerCatchUp"/>. Read at scheduler start after recovery.</summary>
    public SchedulerCatchUp CatchUpAfterDowntime { get; set; } = SchedulerCatchUp.FireOnce;

    /// <summary>
    /// The scheduler ships as a single-threaded dispatch loop, so this is effectively a bound of
    /// one: reducer transactions serialize on the engine's single-writer lock, and a tick worker
    /// pool would parallelize nothing that matters. Values above 1 are accepted and reserved.
    /// </summary>
    public int MaxConcurrentTicks { get; set; } = 1;
}

/// <summary>
/// Identity and session options (<c>MelangeDb:Auth:*</c>). <b>The IdP is the gate</b>: every
/// connection presents a valid JWT, validated against the host's own ASP.NET Core JWT bearer
/// configuration — MelangeDB mints no identities and owns no issuer, audience, or key settings.
/// </summary>
public sealed class AuthOptions
{
    /// <summary>
    /// The authentication scheme whose <c>JwtBearerOptions</c> tokens are validated against —
    /// the host's <c>AddAuthentication().AddJwtBearer(...)</c> registration. There are
    /// deliberately no MelangeDB settings for authority, audience, or keys: those live on the
    /// host's scheme, and duplicating them here would eventually disagree with it.
    /// </summary>
    public string Scheme { get; set; } = "Bearer";

    /// <summary>
    /// The role claim value marking IdP-issued guest tokens. A guest is an ordinary identity that
    /// policies and caps may treat differently, nothing more; empty disables guest treatment.
    /// </summary>
    public string GuestRole { get; set; } = "guest";

    /// <summary>
    /// Lifetime of a connect ticket. Tickets are single-use and short-lived so a leaked one is
    /// near-worthless; they exist because browsers cannot set WebSocket headers.
    /// </summary>
    public int TicketTtlSeconds { get; set; } = 30;

    /// <summary>
    /// How long past token expiry a connection survives while awaiting <c>Reauthenticate</c>.
    /// Zero means expiry drops the socket — correct for a bank, wrong for a game.
    /// </summary>
    public int ReauthGraceSeconds { get; set; } = 120;

    /// <summary>
    /// How many live sockets one identity may hold. Without this, a valid token holds unlimited
    /// sockets, subscriptions, and rate-limit buckets.
    /// </summary>
    public int MaxConnectionsPerIdentity { get; set; } = 4;
}

/// <summary>What the unpoliced-reducer report does at startup.</summary>
public enum UnpolicedReducerReport
{
    /// <summary>No report.</summary>
    Off,

    /// <summary>Log the client-callable reducers with no authorization policy. The default.</summary>
    Warn,

    /// <summary>Refuse to start while any client-callable reducer has no policy.</summary>
    Fail,
}

/// <summary>What happens when a client calls a reducer that declares no policy.</summary>
public enum ReducerPolicyPosture
{
    /// <summary>
    /// The call is allowed. Pair with the unpoliced-reducer report so the omission is visible
    /// without being fatal. The default.
    /// </summary>
    Allow,

    /// <summary>The call is denied. Safer, but annotates every ordinary gameplay reducer.</summary>
    Deny,
}

/// <summary>Reducer authorization options (<c>MelangeDb:Policies:*</c>).</summary>
public sealed class PoliciesOptions
{
    /// <summary>
    /// The startup report listing every client-callable reducer with no authorization policy —
    /// turns "did we forget one?" into a build artifact.
    /// </summary>
    public UnpolicedReducerReport UnpolicedReducerReport { get; set; } = UnpolicedReducerReport.Warn;

    /// <summary>Whether an unpoliced reducer is callable by clients.</summary>
    public ReducerPolicyPosture DefaultReducerPosture { get; set; } = ReducerPolicyPosture.Allow;
}

/// <summary>
/// Reducer rate limiting (<c>MelangeDb:RateLimit:*</c>): a token bucket per identity per reducer,
/// rejected <b>before</b> a transaction opens — so an over-limit call costs no log volume and no
/// schema, unlike rate limits implemented as table rows.
/// </summary>
public sealed class RateLimitOptions
{
    public bool Enabled { get; set; } = true;

    /// <summary>The default sustained rate per identity. Sustained rates are what stop macros.</summary>
    public int ReducerCallsPerSecond { get; set; } = 20;

    /// <summary>The bucket capacity: bursts up to this pass at human click speed.</summary>
    public int BurstCapacity { get; set; } = 60;

    /// <summary>Per-reducer overrides of <see cref="ReducerCallsPerSecond"/>, keyed by reducer name.</summary>
    public Dictionary<string, int> PerReducer { get; set; } = new(StringComparer.Ordinal);
}

/// <summary>Whether ad-hoc SQL results pass through row and column policies.</summary>
public enum AdHocSqlMode
{
    /// <summary>
    /// Row and column policies apply exactly as they do to subscriptions. The default — there is
    /// no third mode and no default-to-owner, because ambiguity here is a security hole.
    /// </summary>
    PolicyEnforced,

    /// <summary>
    /// Row and column policies are deliberately bypassed — the operator's path. <c>[ServerOnly]</c>
    /// columns stay excluded even here: "never leaves the process" has no modes.
    /// </summary>
    Owner,
}

/// <summary>Ad-hoc SQL options (<c>MelangeDb:Sql:*</c>).</summary>
public sealed class SqlOptions
{
    /// <summary>
    /// Whether <c>{path}/sql</c> answers at all. Off by default: ad-hoc SQL is a tooling surface,
    /// and a deployment that never opted in should not be exposing one.
    /// </summary>
    public bool AdHocEnabled { get; set; }

    /// <summary>See <see cref="AdHocSqlMode"/>. Applies to <c>{path}/sql</c>.</summary>
    public AdHocSqlMode AdHocMode { get; set; } = AdHocSqlMode.PolicyEnforced;

    /// <summary>
    /// The role claim that authorizes a caller in <see cref="AdHocSqlMode.Owner"/> mode — the
    /// per-caller half of the two-mode contract, following the <c>Auth:GuestRole</c> precedent:
    /// the IdP is the gate, and owner capability is a claim it issues, not a list MelangeDB keeps.
    /// In Owner mode a caller without this role is refused outright — never silently downgraded
    /// to policy-enforced, because ambiguity here is a security hole. Empty makes Owner mode
    /// unusable by everyone.
    /// </summary>
    public string OwnerRole { get; set; } = "melange-owner";
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
    /// projection rebuilt from the log — while the FASTER engine keeps its hybrid log here.
    /// </summary>
    public string Path { get; set; } = "./data/hot";

    /// <summary>Which storage engine backs the hot tier; see <see cref="HotStoreEngine"/>.</summary>
    public HotStoreEngine Engine { get; set; } = HotStoreEngine.Auto;

    /// <summary>
    /// Cap on the paging buffer pool in bytes. <b>Excludes</b> resident tables, which are
    /// accounted separately — the total declared footprint is this plus the residency report.
    /// Ignored by the in-memory engine, which does not page.
    /// </summary>
    public long MemoryBudgetBytes { get; set; } = 128 * 1024 * 1024;
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

    /// <summary>
    /// Batch concurrent commits into one fsync. Shipped accepted-and-reserved at its default: the
    /// engine's single-writer lock serializes commits, so no two appends are ever in flight for
    /// one fsync to cover — the bulk path is the batching that actually exists. The knob binds and
    /// validates so a future concurrent commit path can honor it without a config break.
    /// </summary>
    public bool GroupCommit { get; set; } = true;
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
