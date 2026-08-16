namespace MelangeDB.Protocol;

/// <summary>The frame types of protocol version 2. Wire values are contract; never renumber.</summary>
public enum FrameType : byte
{
    Hello = 1,
    Welcome = 2,
    CallReducer = 3,
    ReducerResult = 4,
    Subscribe = 5,
    Unsubscribe = 6,
    Unsubscribed = 7,
    SubscriptionApplied = 8,
    TransactionUpdate = 9,
    Error = 10,
    Ping = 11,
    Pong = 12,
    Resume = 13,
    ResumeResult = 14,
    Reauthenticate = 15,
    ReauthenticateResult = 16,
}

/// <summary>
/// Channel assignments. Every frame carries a channel tag from version one, and ordering is
/// guaranteed only <em>within</em> a channel — the constraint that lets one interleaved socket,
/// several HTTP/2 sockets, or QUIC streams all carry the same protocol without a version bump.
/// </summary>
public static class MelangeChannels
{
    /// <summary>Handshake, heartbeat, resume, re-auth, and connection-scoped errors.</summary>
    public const int Control = 0;

    /// <summary>Reducer calls and their results — the interactive lane.</summary>
    public const int Calls = 1;

    /// <summary>Committed deltas, in LSN order, for every subscription on the connection.</summary>
    public const int Data = 2;

    /// <summary>First bulk channel; each subscription's initial set streams on its own channel.</summary>
    public const int BulkBase = 16;

    /// <summary>The bulk channel carrying one subscription's initial set.</summary>
    public static int BulkFor(uint subscriptionId) => BulkBase + (int)subscriptionId;
}

/// <summary>Stable error codes; messages are for humans, codes are for programs.</summary>
public static class MelangeErrorCodes
{
    public const string UnsupportedVersion = "unsupported_version";
    public const string Protocol = "protocol";
    public const string MessageTooLarge = "message_too_large";
    public const string UnknownReducer = "unknown_reducer";
    public const string InvalidArguments = "invalid_args";
    public const string Rejected = "rejected";

    /// <summary>
    /// The call was refused for a condition the system designed and expects to clear — a row
    /// frozen mid-handoff, a border copy just after the shard map flips, a fenced node awaiting
    /// re-registration. The retry contract: try again next tick, unchanged. Distinct from
    /// <see cref="Rejected"/>, which is reserved for what reducer code itself decided.
    /// </summary>
    public const string Transient = "transient";

    public const string Internal = "internal";
    public const string UnknownTable = "unknown_table";
    public const string UnknownColumn = "unknown_column";
    public const string UnindexedColumn = "unindexed_column";
    public const string ParseError = "parse";
    public const string PredicateRequired = "predicate_required";
    public const string RangeTooWide = "range_too_wide";
    public const string TooManyRows = "too_many_rows";
    public const string TooManyBytes = "too_many_bytes";
    public const string TooManySubscriptions = "too_many_subscriptions";
    public const string OverflowResync = "overflow_resync";
    public const string Unauthorized = "unauthorized";
    public const string ConnectionCap = "connection_cap";
    public const string RateLimited = "rate_limited";
    public const string Denied = "denied";
    public const string ServerOnlyColumn = "server_only_column";
    public const string TokenExpired = "token_expired";
    public const string IdentityChanged = "identity_changed";
    public const string SqlDisabled = "sql_disabled";
    public const string BulkDisabled = "bulk_disabled";
    public const string BackupDisabled = "backup_disabled";
    public const string OwnerRequired = "owner_required";
    public const string InvalidAggregate = "invalid_aggregate";
    public const string NotRelationalTier = "not_relational";
    public const string NoRelationalTier = "no_relational_tier";
    public const string RelationalUnavailable = "relational_unavailable";

    /// <summary>
    /// Client-synthesized, never sent by a server: a Manual-dispatch client's frame queue hit
    /// its configured limit without a tick, and the client aborted its own socket.
    /// </summary>
    public const string DispatchOverflow = "dispatch_overflow";
}

/// <summary>One frame on the wire. Every frame carries its channel tag.</summary>
public abstract record Frame
{
    /// <summary>The channel this frame rides; ordering is guaranteed only within a channel.</summary>
    public int Channel { get; init; }

    public abstract FrameType Type { get; }
}

/// <summary>
/// The client's first frame: the protocol versions it speaks and a bearer JWT. The token is
/// validated at the handshake unless the connection already authenticated by header or connect
/// ticket, in which case it is ignored; an unauthenticated Hello with no valid token is rejected —
/// the IdP is the gate.
/// </summary>
public sealed record HelloFrame(int MinVersion, int MaxVersion, string? Token) : Frame
{
    public override FrameType Type => FrameType.Hello;
}

/// <summary>
/// The server's handshake reply. <see cref="HttpProtocol"/> reports what was actually negotiated
/// (for example <c>HTTP/2</c>), because a client must never assume it got the transport it asked
/// for. <see cref="EpochId"/> names the commit log incarnation resume cursors count against.
/// <see cref="Identity"/> is the identity the connection authenticated as, told to the client by
/// the party that computes it — a client must never re-derive it from its own token, because the
/// derivation is the one piece of the contract that must never disagree.
/// </summary>
public sealed record WelcomeFrame(int Version, Guid ConnectionId, Guid EpochId, ulong HeadLsn, string HttpProtocol, Identity Identity) : Frame
{
    public override FrameType Type => FrameType.Welcome;
}

/// <summary>Invokes a reducer. <see cref="TraceParent"/> is a W3C traceparent linking client and server traces.</summary>
public sealed record CallReducerFrame(uint RequestId, string Reducer, byte[] Arguments, string? TraceParent) : Frame
{
    public override FrameType Type => FrameType.CallReducer;
}

/// <summary>The outcome of one reducer call. <see cref="Lsn"/> is the commit LSN, or 0 for a read-only commit or failure.</summary>
public sealed record ReducerResultFrame(uint RequestId, bool Ok, ulong Lsn, string? ErrorCode, string? Message) : Frame
{
    public override FrameType Type => FrameType.ReducerResult;
}

/// <summary>
/// Registers (or, with an already-used id, re-scopes) a subscription. Parameter values bind the
/// query's <c>:name</c> placeholders.
/// </summary>
public sealed record SubscribeFrame(uint SubscriptionId, string Query, IReadOnlyDictionary<string, object?>? Parameters) : Frame
{
    public override FrameType Type => FrameType.Subscribe;
}

public sealed record UnsubscribeFrame(uint SubscriptionId) : Frame
{
    public override FrameType Type => FrameType.Unsubscribe;
}

public sealed record UnsubscribedFrame(uint SubscriptionId) : Frame
{
    public override FrameType Type => FrameType.Unsubscribed;
}

/// <summary>
/// A row in an initial set: the encoded primary key and the row's values as schema-ordered v1 row
/// bytes, shaped by the subscription's <see cref="WireDescriptor"/>.
/// <see cref="ColumnMask"/> is empty unless a column policy masked this row.
/// </summary>
public readonly record struct WireRow(byte[] Key, ReadOnlyMemory<byte> Row, ReadOnlyMemory<byte> ColumnMask);

/// <summary>
/// One chunk of a subscription's initial result set, consistent at <see cref="AnchorLsn"/>. The
/// delta stream carries only LSNs greater than the anchor, which is what makes the boundary
/// gap-free and duplicate-free.
/// <para>
/// <see cref="Descriptor"/> rides on chunk 0 and is null on every chunk after it: it describes the
/// subscription, not the chunk, and the client holds it until the subscription is re-established.
/// </para>
/// </summary>
public sealed record SubscriptionAppliedFrame(
    uint SubscriptionId,
    ulong AnchorLsn,
    uint ChunkIndex,
    bool IsLast,
    IReadOnlyList<WireRow> Rows,
    WireDescriptor? Descriptor = null) : Frame
{
    public override FrameType Type => FrameType.SubscriptionApplied;
}

/// <summary>
/// One row delta. <see cref="Row"/> is empty for a delete, and shaped by the subscription's
/// descriptor otherwise; <see cref="ColumnMask"/> is empty unless a column policy masked this row.
/// </summary>
public readonly record struct WireRowOp(RowOpKind Kind, byte[] Key, ReadOnlyMemory<byte> Row, ReadOnlyMemory<byte> ColumnMask);

/// <summary>The deltas one subscription receives from one committed transaction.</summary>
public sealed record SubscriptionUpdate(uint SubscriptionId, IReadOnlyList<WireRowOp> Ops);

/// <summary>
/// One committed transaction's deltas for every matching subscription on this connection. Frames
/// arrive in LSN order on the data channel; a client acks the LSN once the whole frame is applied.
/// </summary>
public sealed record TransactionUpdateFrame(ulong Lsn, IReadOnlyList<SubscriptionUpdate> Updates) : Frame
{
    public override FrameType Type => FrameType.TransactionUpdate;
}

/// <summary>An error scoped by <see cref="RequestId"/> or <see cref="SubscriptionId"/> (0 = connection-scoped).</summary>
public sealed record ErrorFrame(string Code, string Message, uint RequestId = 0, uint SubscriptionId = 0) : Frame
{
    public override FrameType Type => FrameType.Error;
}

public sealed record PingFrame(uint Id) : Frame
{
    public override FrameType Type => FrameType.Ping;
}

public sealed record PongFrame(uint Id) : Frame
{
    public override FrameType Type => FrameType.Pong;
}

/// <summary>A subscription named inside <see cref="ResumeFrame"/> for re-establishment without an initial set.</summary>
public sealed record ResumeSubscription(uint SubscriptionId, string Query, IReadOnlyDictionary<string, object?>? Parameters);

/// <summary>
/// Resumes an attachment: the log epoch the cursor counts against, the last LSN the client fully
/// applied, and the subscriptions to re-establish. The server either serves the gap from the log
/// or answers full resync — the server decides, never the client.
/// </summary>
public sealed record ResumeFrame(Guid EpochId, ulong LastAckedLsn, IReadOnlyList<ResumeSubscription> Subscriptions) : Frame
{
    public override FrameType Type => FrameType.Resume;
}

/// <summary>Rejected means the client must re-establish every subscription from a fresh initial set.</summary>
public sealed record ResumeResultFrame(bool Accepted, string? Reason) : Frame
{
    public override FrameType Type => FrameType.ResumeResult;
}

/// <summary>
/// Presents a fresh token mid-session, before the current one's expiry plus the server's grace
/// window (<c>Auth:ReauthGraceSeconds</c>) runs out. A re-auth may refresh a token but never
/// change the connection's identity: a token resolving to a different identity closes the
/// connection, because every delta already sent was filtered under the current identity's
/// policies.
/// </summary>
public sealed record ReauthenticateFrame(string Token) : Frame
{
    public override FrameType Type => FrameType.Reauthenticate;
}

public sealed record ReauthenticateResultFrame(bool Accepted, string? Message) : Frame
{
    public override FrameType Type => FrameType.ReauthenticateResult;
}
