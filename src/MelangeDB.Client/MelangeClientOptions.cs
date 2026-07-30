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
}

/// <summary>Thrown when a reducer call fails; <see cref="Code"/> is a <see cref="MelangeErrorCodes"/> value.</summary>
public sealed class MelangeCallException : Exception
{
    public MelangeCallException(string code, string message)
        : base(message)
        => Code = code;

    public string Code { get; }
}

/// <summary>Thrown when the server rejects a subscription; <see cref="Code"/> is a <see cref="MelangeErrorCodes"/> value.</summary>
public sealed class MelangeSubscriptionException : Exception
{
    public MelangeSubscriptionException(string code, string message)
        : base(message)
        => Code = code;

    public string Code { get; }
}
