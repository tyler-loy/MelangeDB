using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The distributed <see cref="IEventTransport"/>. Handler code does not change; where handlers
/// run does, per the settled phase 09 decision: <b>events are dispatched to handlers on the
/// hub</b>. On the hub (and on a non-clustered node) this transport behaves exactly like the
/// in-process one — local delivery into the durable bus. On a shard node, local delivery is
/// suppressed: shard-published events reach the hub through each engine's log-driven
/// <see cref="EventForwarder"/>, at-least-once, and are handled there. Shard-side handler
/// execution (interest-scoped delivery) is phase 10 territory.
/// </summary>
public sealed class ClusterEventTransport : IEventTransport
{
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private IEventInbox? _inbox;

    public ClusterEventTransport(IOptionsMonitor<MelangeDbOptions> options) => _options = options;

    public void Connect(IEventInbox inbox) => _inbox = inbox;

    public void Publish(IReadOnlyList<EventEnvelope> batch)
    {
        if (_options.CurrentValue.Cluster.Role != ClusterRole.Shard)
            _inbox?.Receive(batch);
    }
}

/// <summary>
/// Hub-side dispatch of events that arrived over a node link. Foreign envelopes cannot ride the
/// local bus — its checkpoints count the hub's own log — so they dispatch here: per subscriber,
/// per binding, with the same retry policy as the bus, then a loud log instead of a dead-letter
/// file. At-least-once end to end: the forwarding node advances its cursor only on ack, and the
/// ack is sent only after this dispatch completes.
/// </summary>
internal sealed partial class ForeignEventDispatcher
{
    private readonly EventHandlerRegistry _registry;
    private readonly IServiceScopeFactory _scopes;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly ILogger _logger;

    public ForeignEventDispatcher(
        EventHandlerRegistry registry,
        IServiceScopeFactory scopes,
        IOptionsMonitor<MelangeDbOptions> options,
        ILogger logger)
    {
        _registry = registry;
        _scopes = scopes;
        _options = options;
        _logger = logger;
    }

    public async Task DispatchAsync(string sourceNode, ulong sourceShard, EventsForward batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Events.Length; i++)
        {
            var wire = batch.Events[i];
            foreach (var registration in _registry.Handlers)
            {
                if (!registration.TryGetBinding(wire.EventType, out var binding))
                    continue;
                object @event;
                try
                {
                    @event = EventCodec.Deserialize(binding.EventType, wire.Payload);
                }
                catch (Exception exception)
                {
                    LogForeignEventFailed(_logger, wire.EventType, registration.Name, sourceNode, sourceShard, exception);
                    continue;
                }

                await DeliverAsync(registration, binding, @event, wire, sourceNode, sourceShard, ct).ConfigureAwait(false);
            }
        }
    }

    private async Task DeliverAsync(
        EventHandlerRegistration registration,
        EventHandlerRegistration.EventBinding binding,
        object @event,
        WireEvent wire,
        string sourceNode,
        ulong sourceShard,
        CancellationToken ct)
    {
        var options = _options.CurrentValue.Events;
        var attempts = 1 + Math.Max(0, options.HandlerRetries);
        for (var attempt = 1; attempt <= attempts; attempt++)
        {
            try
            {
                using var scope = _scopes.CreateScope();
                var handler = scope.ServiceProvider.GetRequiredService(registration.HandlerType);
                using var depth = EventDispatchContext.Enter(wire.Depth + 1);
                await binding.Invoke(handler, @event, ct).ConfigureAwait(false);
                return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                if (attempt >= attempts)
                {
                    LogForeignEventFailed(_logger, wire.EventType, registration.Name, sourceNode, sourceShard, exception);
                    return;
                }

                await Task.Delay(Math.Max(1, options.RetryBackoffMs), ct).ConfigureAwait(false);
            }
        }
    }

    [LoggerMessage(EventId = 1704, EventName = "ForeignEventHandlerFailed", Level = LogLevel.Error,
        Message = "Handler '{Subscriber}' exhausted its retries for foreign event '{EventType}' from node '{SourceNode}' (shard {SourceShard}); delivery moves on. Foreign events have no dead-letter file — the source log still holds the event.")]
    private static partial void LogForeignEventFailed(
        ILogger logger, string eventType, string subscriber, string sourceNode, ulong sourceShard, Exception exception);
}

/// <summary>
/// The shard-side half of cross-node events: a log-driven pump that forwards this engine's
/// committed events to the hub in LSN order. The cursor advances only on the hub's ack and
/// persists beside the shard's log, so a crash re-forwards from the last acknowledged record —
/// at-least-once, with the log as the buffer, exactly like every other projection consumer.
/// </summary>
internal sealed class EventForwarder : ICommitObserver, IDisposable
{
    private const int MaxRecordsPerBatch = 256;

    private readonly MelangeEngine _engine;
    private readonly ulong _shardValue;
    private readonly string _cursorPath;
    private readonly Func<NodeLink?> _link;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _signal = new(0, 1);
    private readonly CancellationTokenSource _stopped = new();
    private Task? _loop;

    public EventForwarder(MelangeEngine engine, ulong shardValue, string cursorPath, Func<NodeLink?> link, ILogger logger)
    {
        _engine = engine;
        _shardValue = shardValue;
        _cursorPath = cursorPath;
        _link = link;
        _logger = logger;
    }

    public void Start()
    {
        _engine.AddCommitObserver(this);

        // Records not yet forwarded to the hub must survive log truncation — the log is this
        // forwarder's buffer, exactly as it is for the local bus's subscribers, and the cursor
        // (everything at or below it is delivered) is the highest removable LSN.
        _engine.AddTruncationFloor(() => ReadCursor());
        _loop = Task.Run(LoopAsync);
    }

    public void OnCommit(CommitRecord record)
    {
        if (record.Events.Count > 0)
            Kick();
    }

    /// <summary>Wakes the pump — a new event committed, or the hub link came back.</summary>
    public void Kick()
    {
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private async Task LoopAsync()
    {
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var link = _link();
                if (link is null || !await ForwardOnceAsync(link, ct).ConfigureAwait(false))
                {
                    // Nothing to send, or no link: wait for a kick, re-probing periodically so a
                    // silently restored link does not strand buffered events.
                    await _signal.WaitAsync(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception)
            {
                // Link failure mid-forward: the cursor did not advance; retry after a beat.
                try
                {
                    await Task.Delay(200, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>Forwards one batch; returns false when the cursor is at the head.</summary>
    private async Task<bool> ForwardOnceAsync(NodeLink link, CancellationToken ct)
    {
        var cursor = ReadCursor();

        // Durable, not head: ReadFrom serves nothing beyond the durability watermark, so judging
        // availability by the head would report more-to-do through a gap the scan cannot reach yet
        // and drain-until-done would spin through it.
        var head = _engine.Log.DurableLsn;
        if (cursor >= head)
            return false;

        var events = new List<WireEvent>();
        var timestamps = new List<long>();
        var lsns = new List<ulong>();
        ulong upTo = cursor;
        var records = 0;
        foreach (var record in _engine.Log.ReadFrom(cursor + 1))
        {
            upTo = record.Lsn;
            foreach (var @event in record.Events)
            {
                events.Add(WireEvent.From(@event));
                timestamps.Add(record.Timestamp.UnixTimeMicroseconds);
                lsns.Add(record.Lsn);
            }

            if (++records >= MaxRecordsPerBatch)
                break;
        }

        if (events.Count > 0)
        {
            await link.RequestAsync(
                "events-forward",
                new EventsForward(upTo, _shardValue, [.. events], [.. timestamps], [.. lsns]),
                ct).ConfigureAwait(false);
        }

        WriteCursor(upTo);
        return upTo < head;
    }

    private ulong ReadCursor()
    {
        try
        {
            if (File.Exists(_cursorPath))
            {
                var parts = File.ReadAllText(_cursorPath).Split('|');
                if (parts.Length == 2 && Guid.TryParse(parts[0], out var epoch) && epoch == _engine.Log.EpochId
                    && ulong.TryParse(parts[1], out var lsn))
                {
                    return lsn;
                }
            }
        }
        catch (IOException)
        {
        }

        return _engine.Log.BaseLsn;
    }

    private void WriteCursor(ulong lsn) =>
        File.WriteAllText(_cursorPath, $"{_engine.Log.EpochId}|{lsn}");

    public void Dispose()
    {
        _stopped.Cancel();
        Kick();
        try
        {
            _loop?.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException)
        {
        }
    }
}
