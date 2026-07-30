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

    /// <summary>The bearer token presented in the handshake. Validation semantics land in phase 04.</summary>
    public string? Token { get; set; }

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
