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
}

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

/// <summary>Thrown when a reducer call fails; <see cref="Code"/> is a <see cref="MelangeErrorCodes"/> value.</summary>
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
}

/// <summary>Thrown when the server rejects a subscription; <see cref="Code"/> is a <see cref="MelangeErrorCodes"/> value.</summary>
public sealed class MelangeSubscriptionException : Exception
{
    public MelangeSubscriptionException(string code, string message)
        : base(message)
        => Code = code;

    public string Code { get; }
}
