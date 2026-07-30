using System.Collections.Concurrent;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Server;

/// <summary>
/// The transport's composition root, built once by <c>MapMelangeSocket</c>: the serializer, the
/// subscription engine wired into the engine as a commit observer, the live connection set, and
/// the ticket store. Everything here reads options through the monitor, so live keys apply with
/// no restart.
/// </summary>
internal sealed class MelangeTransport : ICommitObserver
{
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly ConcurrentDictionary<ConnectionId, MelangeSocketConnection> _connections = new();

    public MelangeTransport(
        MelangeEngine engine,
        MelangeReducerHost reducers,
        IOptionsMonitor<MelangeDbOptions> options,
        TimeProvider? time,
        ILoggerFactory loggerFactory,
        CancellationToken stopping = default)
    {
        Stopping = stopping;
        Engine = engine;
        Reducers = reducers;
        _options = options;
        Time = time ?? TimeProvider.System;
        Logger = loggerFactory.CreateLogger("MelangeDB.Server");
        Serializer = options.CurrentValue.Transport.Serializer switch
        {
            WireSerializer.MessagePack => new MessagePackFrameSerializer(),
            var other => throw new InvalidOperationException($"Unknown Transport:Serializer value {other}."),
        };
        Telemetry = options.CurrentValue.Telemetry.Enabled
            ? new ServerTelemetry(
                () => _options.CurrentValue.Telemetry,
                () => _connections.Count,
                () => Subscriptions!.ActiveByTable)
            : null;
        Subscriptions = new SubscriptionEngine(engine, Telemetry);
        Tickets = new TicketStore(Time);
        engine.AddCommitObserver(this);
    }

    /// <summary>Fires on host shutdown, so live sockets close instead of pinning graceful stop.</summary>
    public CancellationToken Stopping { get; }

    public MelangeEngine Engine { get; }

    public MelangeReducerHost Reducers { get; }

    public TimeProvider Time { get; }

    public ILogger Logger { get; }

    public IMelangeSerializer Serializer { get; }

    public SubscriptionEngine Subscriptions { get; }

    public ServerTelemetry? Telemetry { get; }

    public TicketStore Tickets { get; }

    public MelangeDbOptions Options => _options.CurrentValue;

    public int ActiveConnections => _connections.Count;

    public void OnCommit(CommitRecord record) => Subscriptions.Fanout(record);

    public void OnConnectionOpened(MelangeSocketConnection connection) =>
        _connections.TryAdd(connection.ConnectionId, connection);

    public void OnConnectionClosed(MelangeSocketConnection connection) =>
        _connections.TryRemove(connection.ConnectionId, out _);
}

/// <summary>
/// Mints and redeems connect tickets. Phase 03 ships the endpoint and the store; phase 04 wires
/// redemption into the socket handshake and backs the ticket with a validated JWT. Tickets are
/// single-use and short-lived so a leaked one is near-worthless.
/// </summary>
internal sealed class TicketStore(TimeProvider time)
{
    private const int TicketTtlSeconds = 30;

    private readonly ConcurrentDictionary<string, DateTimeOffset> _tickets = new(StringComparer.Ordinal);

    /// <summary>Mints a single-use ticket. Returns the ticket and its lifetime in seconds.</summary>
    public (string Ticket, int ExpiresInSeconds) Mint()
    {
        Prune();
        var ticket = Guid.NewGuid().ToString("N");
        _tickets[ticket] = time.GetUtcNow().AddSeconds(TicketTtlSeconds);
        return (ticket, TicketTtlSeconds);
    }

    /// <summary>Redeems a ticket; each ticket redeems at most once, and never after expiry.</summary>
    public bool TryRedeem(string ticket) =>
        _tickets.TryRemove(ticket, out var expires) && expires >= time.GetUtcNow();

    private void Prune()
    {
        var now = time.GetUtcNow();
        foreach (var (ticket, expires) in _tickets)
        {
            if (expires < now)
                _tickets.TryRemove(ticket, out _);
        }
    }
}
