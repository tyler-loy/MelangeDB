using System.Net;
using System.Net.Http.Headers;
using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>Options for one <see cref="MelangeClient"/>.</summary>
public sealed class MelangeClientOptions
{
    /// <summary>The websocket endpoint, e.g. <c>ws://host:port/melange</c>.</summary>
    public required Uri Uri { get; set; }

    /// <summary>
    /// The HTTP version to request for the handshake. HTTP/2 uses RFC 8441 extended CONNECT and
    /// multiplexes several sockets onto one TCP connection. Whatever is requested, the server
    /// reports what was actually negotiated in <see cref="MelangeClient.NegotiatedHttpProtocol"/> —
    /// never assume you got the transport you asked for.
    /// </summary>
    public Version HttpVersion { get; set; } = System.Net.HttpVersion.Version11;

    /// <summary>
    /// The bearer JWT presented at the handshake. When null, the client loads one from
    /// <see cref="TokenStore"/> instead; connecting with neither fails — every connection presents
    /// a valid token, and the server's IdP is the gate.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Where the client persists its token across runs. Defaults to <see cref="InMemoryTokenStore"/>;
    /// use <see cref="FileTokenStore"/> (or platform secure storage) in anything real — for a
    /// guest, the token is the character.
    /// </summary>
    public ITokenStore TokenStore { get; set; } = new InMemoryTokenStore();

    /// <summary>
    /// Authenticate via the connect-ticket flow instead of the Hello token: POST the JWT to
    /// <see cref="TicketUri"/> (derived from <see cref="Uri"/> by default), then open the socket
    /// with the single-use ticket on the URL. The path that works where headers cannot be set.
    /// </summary>
    public bool UseTicket { get; set; }

    /// <summary>Overrides the ticket endpoint; defaults to <c>{Uri as http(s)}/ticket</c>.</summary>
    public Uri? TicketUri { get; set; }

    /// <summary>Requests <c>permessage-deflate</c> on the socket.</summary>
    public bool CompressionEnabled { get; set; }

    /// <summary>
    /// A diagnostics hook observing every received frame and its wire size, on the receive loop.
    /// Useful for asserting wire-level behaviour; keep it fast.
    /// </summary>
    public Action<Frame, int>? FrameInspector { get; set; }

    /// <summary>
    /// How data-channel frames — initial sets, deltas — and the connection lifecycle events
    /// around them are dispatched. <see cref="DispatchMode.Immediate"/> (the default) applies
    /// frames and raises events on the receive loop as they arrive.
    /// <see cref="DispatchMode.Manual"/> queues whole frames and applies them only inside
    /// <see cref="MelangeClient.FrameTick"/>, on the caller's thread — the game-loop mode, for
    /// hosts (Godot, Unity) whose handlers may only run on the host's own thread.
    /// </summary>
    public DispatchMode Dispatch { get; set; } = DispatchMode.Immediate;

    /// <summary>
    /// Manual dispatch only: the ceiling on entries queued between ticks. On overflow the client
    /// synthesizes a <see cref="MelangeErrorCodes.DispatchOverflow"/> error at the head of the
    /// queue and aborts the socket — never dropping a delta silently (the cache would diverge),
    /// never blocking the receive loop (a blocked loop stops answering pings and the server
    /// convicts the client illegibly). The default is over a minute of not ticking at a
    /// sustained 1,000 commits per second — far longer than any loading screen — while keeping
    /// worst-case memory bounded. Recovery is the ordinary
    /// <see cref="MelangeClient.ReconnectAsync"/> resume path once the app ticks again.
    /// </summary>
    public int DispatchQueueLimit { get; set; } = 65536;

    /// <summary>
    /// Automatic reconnect. Null — the default — leaves reconnecting to the application, through
    /// <see cref="MelangeClient.ReconnectAsync"/>. Set, a dropped connection is re-dialed with
    /// backoff and resumed through that same path: the client's caches, subscriptions, and handlers
    /// stay usable across the drop, and the identity must not change. See <see cref="ReconnectPolicy"/>.
    /// </summary>
    public ReconnectPolicy? Reconnect { get; set; }

    /// <summary>
    /// Supplies the token for every dial — the first connect and every reconnect — taking precedence
    /// over <see cref="Token"/> and <see cref="TokenStore"/>. For expiring credentials: return a fresh
    /// token each time, and a dial after expiry presents it rather than the one that expired. The
    /// phase 04 rule holds: a token for a different identity fails a reconnect rather than switching
    /// the client to another player mid-session.
    /// </summary>
    public Func<CancellationToken, ValueTask<string>>? TokenProvider { get; set; }
}

/// <summary>
/// How <see cref="MelangeClient"/> reconnects on its own (road-to-0.4 phase 28): exponential backoff
/// with jitter, capped, optionally bounded. Every attempt runs the ordinary
/// <see cref="MelangeClient.ReconnectAsync"/> path — resume from the last acknowledged LSN, or a full
/// re-establishment when the server cannot serve the gap — so a reconnect reconciles the caches
/// exactly as a hand-written one would.
/// </summary>
public sealed class ReconnectPolicy
{
    /// <summary>The wait before the first attempt. Short, because most drops are blips.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>The ceiling on the wait between attempts.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How much each wait grows over the last, before jitter.</summary>
    public double Multiplier { get; set; } = 2.0;

    /// <summary>
    /// The fraction of each wait taken off at random, 0 to 1, so a server restart is not met by
    /// every client at the same instant.
    /// </summary>
    public double Jitter { get; set; } = 0.2;

    /// <summary>
    /// How many attempts before giving up and raising <see cref="MelangeClient.OnReconnectFailed"/>;
    /// null, the default, keeps trying for as long as the client lives.
    /// </summary>
    public int? MaxAttempts { get; set; }

    /// <summary>How long one attempt — dial, handshake, resume — may take before it counts as failed.</summary>
    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>The wait before attempt <paramref name="attempt"/> (1-based), jitter applied.</summary>
    public TimeSpan DelayFor(int attempt, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);
        var exponent = Math.Clamp(attempt - 1, 0, 64);
        var raw = InitialDelay.TotalMilliseconds * Math.Pow(Math.Max(Multiplier, 1.0), exponent);
        var capped = Math.Min(raw, MaxDelay.TotalMilliseconds);
        var jittered = capped * (1 - (Math.Clamp(Jitter, 0, 1) * random.NextDouble()));
        return TimeSpan.FromMilliseconds(Math.Max(jittered, 0));
    }
}

/// <summary>One automatic reconnect attempt about to start, after <see cref="Delay"/>.</summary>
/// <param name="Attempt">1 for the first attempt after a drop.</param>
/// <param name="Delay">The backoff wait before this attempt.</param>
/// <param name="LastError">Why the previous attempt failed; null before the first.</param>
public sealed record ReconnectAttempt(int Attempt, TimeSpan Delay, Exception? LastError);

/// <summary>How a <see cref="MelangeClient"/> dispatches data frames and connection events.</summary>
public enum DispatchMode
{
    /// <summary>Frames apply and events fire on the receive loop as they arrive. The default.</summary>
    Immediate,

    /// <summary>
    /// Whole frames queue in arrival order; <see cref="MelangeClient.FrameTick"/> applies them
    /// and fires their events on the calling thread. A Manual client that is never ticked
    /// applies nothing — subscriptions and reconnects await their frames, so tick from the
    /// host's own loop.
    /// </summary>
    Manual,
}

/// <summary>
/// Thrown when a reducer call fails; <see cref="Code"/> is a <see cref="MelangeErrorCodes"/> value.
/// When the failure came from an HTTP step rather than a socket frame — today that is the connect
/// ticket in <see cref="MelangeClient.ConnectAsync"/> — <see cref="Status"/>,
/// <see cref="ResponseHeaders"/>, and <see cref="Reference"/> carry the server's side of it, so a
/// client can show the player something that finds the request in the server's logs instead of
/// leaving them with "it didn't work".
/// </summary>
public sealed class MelangeCallException : Exception
{
    public MelangeCallException(string code, string message)
        : base(message)
        => Code = code;

    public string Code { get; }

    /// <summary>
    /// The retry contract, named: the call was refused for a condition the server designed and
    /// expects to clear (a handoff freeze window, a border copy just after the shard map flips,
    /// a fenced node) — retry it unchanged on the next tick. Nothing else about the call needs
    /// to change, and nothing went wrong on the server.
    /// </summary>
    public bool IsTransient => Code == MelangeErrorCodes.Transient;

    /// <summary>
    /// The HTTP status of the failed response, or null when the failure arrived as a socket frame
    /// (every path but the connect ticket).
    /// </summary>
    public HttpStatusCode? Status { get; init; }

    /// <summary>
    /// The failed response's headers, or null for a socket-frame failure. Held rather than copied,
    /// so a caller that wants a header this type does not name — a proxy's own correlation id, a
    /// <c>Retry-After</c> — can read it.
    /// </summary>
    public HttpResponseHeaders? ResponseHeaders { get; init; }

    /// <summary>
    /// The id to quote when reporting this failure: the server's <c>traceId</c> where it is
    /// tracing, otherwise its request id. Null when the server sent neither — an older build, or a
    /// failure produced by something in front of it that stamps nothing. It also appears in
    /// <see cref="Exception.Message"/>, so a client that only logs the message still carries it.
    /// </summary>
    public string? Reference { get; init; }
}

/// <summary>Thrown when the server rejects a subscription; <see cref="Code"/> is a <see cref="MelangeErrorCodes"/> value.</summary>
public sealed class MelangeSubscriptionException : Exception
{
    public MelangeSubscriptionException(string code, string message)
        : base(message)
        => Code = code;

    public string Code { get; }
}

/// <summary>
/// A producer's answer as it arrived: the encoded result (read it with
/// <see cref="MelangeDB.Protocol.ProducerResultReader"/>, or let the generated bindings) and the
/// commit LSN the server's read answered for.
/// </summary>
public sealed record ProducerReply(ReadOnlyMemory<byte> Payload, ulong Lsn);
