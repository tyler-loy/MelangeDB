using System.Collections.Concurrent;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Server;

/// <summary>
/// The transport's composition root, built once by <c>MapMelangeSocket</c>: the serializer, the
/// subscription engine wired into the engine as a commit observer, the authenticator over the
/// host's JWT bearer scheme, the ticket store, the DI-resolved policy set, the per-identity
/// connection caps, and the live connection set. Everything here reads options through the
/// monitor, so live keys apply with no restart.
/// </summary>
internal sealed class MelangeTransport : ICommitObserver, IDisposable
{
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly ConcurrentDictionary<ConnectionId, MelangeSocketConnection> _connections = new();
    private readonly ConcurrentDictionary<Identity, int> _connectionsPerIdentity = new();
    private readonly IDisposable _sessionsAttachment;

    public MelangeTransport(
        MelangeEngine engine,
        MelangeReducerHost reducers,
        IOptionsMonitor<MelangeDbOptions> options,
        TimeProvider? time,
        ILoggerFactory loggerFactory,
        MelangeAuthenticator authenticator,
        MelangeSessions sessions,
        PolicySet policies,
        CancellationToken stopping = default)
    {
        Stopping = stopping;
        Engine = engine;
        Reducers = reducers;
        _options = options;
        Time = time ?? TimeProvider.System;
        Logger = loggerFactory.CreateLogger("MelangeDB.Server");
        Authenticator = authenticator;
        Sessions = sessions;
        Policies = policies;
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
        Tickets = new TicketStore(Time, () => _options.CurrentValue.Auth.TicketTtlSeconds);
        _sessionsAttachment = sessions.Attach(TerminateSessions);
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

    public MelangeAuthenticator Authenticator { get; }

    public MelangeSessions Sessions { get; }

    public PolicySet Policies { get; }

    public MelangeDbOptions Options => _options.CurrentValue;

    public int ActiveConnections => _connections.Count;

    public void OnCommit(CommitRecord record) => Subscriptions.Fanout(record);

    public void OnConnectionOpened(MelangeSocketConnection connection) =>
        _connections.TryAdd(connection.ConnectionId, connection);

    public void OnConnectionClosed(MelangeSocketConnection connection) =>
        _connections.TryRemove(connection.ConnectionId, out _);

    /// <summary>
    /// Counts a new connection against <c>Auth:MaxConnectionsPerIdentity</c>; false refuses it.
    /// The caller must pair a true with <see cref="ReleaseConnectionSlot"/> on close.
    /// </summary>
    public bool TryReserveConnectionSlot(Identity identity)
    {
        var cap = _options.CurrentValue.Auth.MaxConnectionsPerIdentity;
        while (true)
        {
            var current = _connectionsPerIdentity.GetValueOrDefault(identity);
            if (current >= cap)
                return false;
            if (current == 0
                ? _connectionsPerIdentity.TryAdd(identity, 1)
                : _connectionsPerIdentity.TryUpdate(identity, current + 1, current))
            {
                return true;
            }
        }
    }

    public void ReleaseConnectionSlot(Identity identity)
    {
        while (_connectionsPerIdentity.TryGetValue(identity, out var current))
        {
            if (current <= 1
                ? _connectionsPerIdentity.TryRemove(new KeyValuePair<Identity, int>(identity, current))
                : _connectionsPerIdentity.TryUpdate(identity, current - 1, current))
            {
                return;
            }
        }
    }

    /// <summary>Closes every live connection held by an identity — the moderation path.</summary>
    public int TerminateSessions(Identity identity)
    {
        var closed = 0;
        foreach (var connection in _connections.Values)
        {
            if (connection.Caller == identity && connection.Terminate())
                closed++;
        }

        return closed;
    }

    public void Dispose() => _sessionsAttachment.Dispose();
}
