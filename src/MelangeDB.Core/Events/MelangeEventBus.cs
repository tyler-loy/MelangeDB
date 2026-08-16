using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// Delivers domain events to <see cref="IEventHandler{TEvent}"/> subscribers: a projection
/// consumer over the commit log's event records, exactly the shape of a storage applier. Each
/// handler type is one logical subscriber with its own durable LSN checkpoint, so a subscriber
/// that was down catches up from the log instead of losing events — delivery is at-least-once and
/// replayable. Handlers run on per-subscriber dispatch loops, <em>outside</em> the emitting
/// transaction and never under a lock the commit path takes; a failing handler is retried with
/// backoff and then dead-lettered, and can neither wedge the applier pipeline nor block later
/// transactions. The in-memory delivery window is bounded (<c>Events:MaxQueueDepth</c>); overflow
/// evicts the oldest entries and a lagging subscriber replays from the log, because the log
/// <em>is</em> the buffer and the checkpoint already models the lag. Checkpoints idle past
/// <c>Events:SubscriberExpirySeconds</c> are evicted loudly so an abandoned subscriber cannot pin
/// log truncation forever.
/// </summary>
public sealed class MelangeEventBus : ICommitObserver, IEventInbox, IDisposable
{
    private readonly MelangeEngine _engine;
    private readonly EventHandlerRegistry _registry;
    private readonly IEventTransport _transport;
    private readonly IServiceScopeFactory _scopes;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly Lock _windowLock = new();
    private readonly Lock _checkpointLock = new();
    private readonly List<WindowEntry> _window = [];
    private readonly List<Subscriber> _subscribers = [];
    private readonly List<Task> _loops = [];
    private EventCheckpointStore? _store;
    private DeadLetterStore? _deadLetters;
    private ITimer? _sweepTimer;
    private int _windowEventCount;
    private ulong _windowStartLsn = 1;
    private ulong _publishedHead;
    private volatile bool _started;
    private volatile bool _stopped;

    public MelangeEventBus(
        MelangeEngine engine,
        EventHandlerRegistry registry,
        IEventTransport transport,
        IServiceScopeFactory scopes,
        IOptionsMonitor<MelangeDbOptions> options,
        ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _registry = registry;
        _transport = transport;
        _scopes = scopes;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<MelangeEventBus>();
    }

    /// <summary>
    /// The lowest checkpoint LSN of any live (non-evicted) subscriber — orphaned checkpoints
    /// included until expiry evicts them. This is the floor phase 07's log truncation must respect:
    /// truncating past it would strand a subscriber that is merely behind. Null when no
    /// checkpoints exist, meaning events pin nothing.
    /// </summary>
    public ulong? MinimumLiveCheckpointLsn
    {
        get
        {
            lock (_checkpointLock)
            {
                if (_store is null)
                    return null;
                ulong? min = null;
                foreach (var entry in _store.Entries.Values)
                {
                    if (!entry.Evicted && (min is null || entry.Lsn < min))
                        min = entry.Lsn;
                }

                return min;
            }
        }
    }

    /// <summary>Each registered subscriber's current state, for diagnostics and tests.</summary>
    public IReadOnlyList<EventSubscriberStatus> Subscribers
    {
        get
        {
            lock (_checkpointLock)
            {
                return _subscribers
                    .Select(s => new EventSubscriberStatus(s.Name, s.Cursor, s.LostPlace))
                    .ToArray();
            }
        }
    }

    /// <summary>
    /// Loads the durable checkpoints, anchors every subscriber, registers as a commit observer,
    /// and starts the dispatch loops and the expiry sweep. A subscriber with a live checkpoint
    /// resumes from it; one whose checkpoint was evicted starts from the current head and is told
    /// so (EventId 1404); a brand-new subscriber starts from the current head — event history
    /// before a handler existed is not replayed into it.
    /// </summary>
    internal void Start()
    {
        var options = _options.CurrentValue.Events;
        _store = new EventCheckpointStore(_engine.Options.CommitLog.Path);
        _deadLetters = new DeadLetterStore(options.DeadLetterPath);
        _engine.Telemetry?.SetEventQueueDepthProvider(() =>
        {
            lock (_windowLock)
            {
                return _windowEventCount;
            }
        });

        // Anchor and observer registration happen under one write-lock hold, so no commit can
        // slip between the anchor and the stream of observed records — the scheduler's pattern.
        _engine.ReadConsistent(head =>
        {
            var now = _time.GetUtcNow().ToUnixTimeMilliseconds();
            lock (_checkpointLock)
            {
                foreach (var registration in _registry.Handlers)
                {
                    var subscriber = new Subscriber(registration);
                    if (_store.Entries.TryGetValue(registration.Name, out var entry) && !entry.Evicted)
                    {
                        subscriber.Cursor = entry.Lsn;
                        entry.LastActiveUnixMs = now;
                    }
                    else
                    {
                        if (entry is { Evicted: true })
                        {
                            subscriber.LostPlace = true;
                            LogMessages.SubscriberLostPlace(_logger, registration.Name, entry.Lsn, head);
                        }

                        subscriber.Cursor = head;
                        _store.Entries[registration.Name] = new EventCheckpointStore.Entry
                        {
                            Lsn = head,
                            LastActiveUnixMs = now,
                        };
                    }

                    _subscribers.Add(subscriber);
                }

                _store.Save();
            }

            lock (_windowLock)
            {
                _publishedHead = head;
                _windowStartLsn = head + 1;
            }

            if (_subscribers.Count > 0)
            {
                _transport.Connect(this);
                _engine.AddCommitObserver(this);
            }
        });

        _started = true;
        foreach (var subscriber in _subscribers)
        {
            _loops.Add(Task.Run(() => RunSubscriberAsync(subscriber), CancellationToken.None));
            subscriber.Signal.Release();
        }

        var sweepPeriod = SweepPeriod(options.SubscriberExpirySeconds);
        _sweepTimer = _time.CreateTimer(static state => ((MelangeEventBus)state!).Sweep(), this, sweepPeriod, sweepPeriod);
    }

    /// <summary>
    /// Stops delivery: cancels the loops, waits briefly for in-flight handlers, and persists the
    /// checkpoints. An event mid-delivery at shutdown is not checkpointed and redelivers on the
    /// next start — at-least-once, honestly.
    /// </summary>
    internal void Stop()
    {
        if (_stopped)
            return;
        _stopped = true;
        _cts.Cancel();
        _sweepTimer?.Dispose();
        try
        {
            Task.WaitAll([.. _loops], TimeSpan.FromSeconds(10));
        }
        catch (AggregateException)
        {
            // A loop observed cancellation mid-await; its checkpoint simply stays put.
        }

        lock (_checkpointLock)
        {
            _store?.Save();
        }

        _cts.Dispose();
    }

    public void Dispose() => Stop();

    /// <summary>
    /// The commit observer: runs under the engine's write lock, so it only hands the record's
    /// events to the transport (which enqueues) and wakes the dispatch loops. No user code runs
    /// here, ever — that is the lock-ordering discipline that keeps a handler from wedging the
    /// commit path.
    /// </summary>
    public void OnCommit(CommitRecord record)
    {
        if (!_started || _stopped)
            return;
        if (record.Events.Count > 0)
        {
            var emitter = Activity.Current?.Context ?? default;
            var batch = new EventEnvelope[record.Events.Count];
            for (var i = 0; i < record.Events.Count; i++)
            {
                batch[i] = new EventEnvelope
                {
                    Lsn = record.Lsn,
                    Timestamp = record.Timestamp,
                    Event = record.Events[i],
                    EmitterContext = emitter,
                };
            }

            _transport.Publish(batch);
        }

        lock (_windowLock)
        {
            _publishedHead = record.Lsn;
        }

        foreach (var subscriber in _subscribers)
            subscriber.Signal.Release();
    }

    /// <summary>
    /// The transport's local inbox: appends one committed record's events to the bounded delivery
    /// window. Overflow evicts the oldest entries — the log still holds them, so a subscriber that
    /// needed the evicted range replays from the log instead. Nothing is lost; the checkpoint lag
    /// says how far behind the subscriber is.
    /// </summary>
    public void Receive(IReadOnlyList<EventEnvelope> batch)
    {
        if (batch.Count == 0)
            return;
        var maxDepth = _options.CurrentValue.Events.MaxQueueDepth;
        lock (_windowLock)
        {
            _window.Add(new WindowEntry(batch[0].Lsn, batch));
            _windowEventCount += batch.Count;
            while (_windowEventCount > maxDepth && _window.Count > 1)
            {
                var evicted = _window[0];
                _window.RemoveAt(0);
                _windowEventCount -= evicted.Events.Count;
                _windowStartLsn = evicted.Lsn + 1;
            }
        }
    }

    private async Task RunSubscriberAsync(Subscriber subscriber)
    {
        var ct = _cts.Token;
        try
        {
            while (true)
            {
                // The timeout is the durability backstop, not the delivery path: the commit-time
                // kick can arrive before the record's fsync completes, and the drain's capped log
                // read then stops short — on a quiet engine nothing would ever re-kick.
                await subscriber.Signal.WaitAsync(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
                await DrainAsync(subscriber, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
    }

    private async Task DrainAsync(Subscriber subscriber, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            ulong target;
            var batch = new List<WindowEntry>();
            var readLogUpTo = 0UL;
            lock (_windowLock)
            {
                target = _publishedHead;
                if (subscriber.Cursor >= target)
                    return;
                if (subscriber.Cursor + 1 >= _windowStartLsn)
                {
                    foreach (var entry in _window)
                    {
                        if (entry.Lsn > subscriber.Cursor && entry.Lsn <= target)
                            batch.Add(entry);
                    }
                }
                else
                {
                    // The window no longer covers this subscriber's gap; the log is the buffer.
                    readLogUpTo = Math.Min(_windowStartLsn - 1, target);
                }
            }

            if (readLogUpTo > 0)
            {
                foreach (var record in _engine.Log.ReadFrom(subscriber.Cursor + 1))
                {
                    if (record.Lsn > readLogUpTo)
                        break;
                    if (record.Events.Count > 0)
                        batch.Add(new WindowEntry(record.Lsn, ToEnvelopes(record)));
                }

                target = readLogUpTo;
            }

            foreach (var entry in batch)
            {
                ct.ThrowIfCancellationRequested();
                var delivered = false;
                foreach (var envelope in entry.Events)
                {
                    if (!subscriber.Registration.TryGetBinding(envelope.Event.EventType, out var binding))
                        continue;
                    await DeliverAsync(subscriber, envelope, binding, ct).ConfigureAwait(false);
                    delivered = true;
                }

                AdvanceCheckpoint(subscriber, entry.Lsn, persist: delivered);
            }

            AdvanceCheckpoint(subscriber, target, persist: false);
        }
    }

    private async Task DeliverAsync(
        Subscriber subscriber,
        EventEnvelope envelope,
        EventHandlerRegistration.EventBinding binding,
        CancellationToken ct)
    {
        object @event;
        try
        {
            @event = EventCodec.Deserialize(binding.EventType, envelope.Event.Payload);
        }
        catch (Exception exception)
        {
            DeadLetter(subscriber, envelope, attempts: 0, exception);
            return;
        }

        var options = _options.CurrentValue.Events;
        var attempts = 1 + Math.Max(0, options.HandlerRetries);
        for (var attempt = 1; attempt <= attempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await InvokeHandlerAsync(subscriber, envelope, binding, @event, ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                if (attempt >= attempts)
                {
                    DeadLetter(subscriber, envelope, attempt, exception);
                    return;
                }

                var backoff = BackoffDelay(options.RetryBackoffMs, attempt);
                LogMessages.HandlerRetry(
                    _logger, subscriber.Name, envelope.Event.EventType, envelope.Lsn, attempt, backoff.TotalMilliseconds, exception);
                await Task.Delay(backoff, _time, ct).ConfigureAwait(false);
            }
        }
    }

    private async Task InvokeHandlerAsync(
        Subscriber subscriber,
        EventEnvelope envelope,
        EventHandlerRegistration.EventBinding binding,
        object @event,
        CancellationToken ct)
    {
        using var scope = _scopes.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService(subscriber.Registration.HandlerType);
        using var activity = _engine.Telemetry?.StartEventHandle(envelope.Event.EventType, subscriber.Name, envelope.EmitterContext);
        using var depth = EventDispatchContext.Enter(envelope.Event.Depth + 1);
        try
        {
            await binding.Invoke(handler, @event, ct).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            throw;
        }
    }

    private void DeadLetter(Subscriber subscriber, EventEnvelope envelope, int attempts, Exception failure)
    {
        var cause = failure is System.Reflection.TargetInvocationException { InnerException: { } inner } ? inner : failure;
        JsonElement payload;
        try
        {
            payload = JsonSerializer.Deserialize<JsonElement>(envelope.Event.Payload.Span);
        }
        catch (JsonException)
        {
            payload = JsonSerializer.SerializeToElement(Convert.ToBase64String(envelope.Event.Payload.Span));
        }

        _deadLetters?.Append(new DeadLetterRecord(
            _time.GetUtcNow().ToString("O"),
            subscriber.Name,
            envelope.Event.EventType,
            envelope.Lsn,
            envelope.Event.Depth,
            attempts,
            cause.Message,
            cause.GetType().FullName ?? cause.GetType().Name,
            payload));
        _engine.Telemetry?.RecordDeadLettered(envelope.Event.EventType);
        LogMessages.DeadLettered(_logger, subscriber.Name, envelope.Event.EventType, envelope.Lsn, attempts, cause);
    }

    private void AdvanceCheckpoint(Subscriber subscriber, ulong lsn, bool persist)
    {
        lock (_checkpointLock)
        {
            if (lsn <= subscriber.Cursor && !persist)
                return;
            if (lsn > subscriber.Cursor)
                subscriber.Cursor = lsn;
            if (_store is null)
                return;
            if (_store.Entries.TryGetValue(subscriber.Name, out var entry))
            {
                entry.Lsn = subscriber.Cursor;
                entry.LastActiveUnixMs = _time.GetUtcNow().ToUnixTimeMilliseconds();
                entry.Evicted = false;
            }

            if (persist)
                _store.Save();
        }
    }

    /// <summary>
    /// The expiry sweep. Registered subscribers are alive by definition and get their
    /// last-active refreshed; an unregistered checkpoint idle past
    /// <c>Events:SubscriberExpirySeconds</c> is evicted with a loud log and stops pinning
    /// retention. Deliberate, bounded data loss over unbounded disk growth.
    /// </summary>
    private void Sweep()
    {
        if (!_started || _stopped)
            return;
        var options = _options.CurrentValue.Events;
        var now = _time.GetUtcNow();
        var nowMs = now.ToUnixTimeMilliseconds();
        var expiryMs = (long)options.SubscriberExpirySeconds * 1000;
        lock (_checkpointLock)
        {
            if (_store is null)
                return;
            var registered = _subscribers.Select(s => s.Name).ToHashSet(StringComparer.Ordinal);
            var dirty = false;
            foreach (var (name, entry) in _store.Entries)
            {
                if (registered.Contains(name))
                {
                    entry.LastActiveUnixMs = nowMs;
                    dirty = true;
                    continue;
                }

                if (!entry.Evicted && nowMs - entry.LastActiveUnixMs > expiryMs)
                {
                    entry.Evicted = true;
                    entry.EvictedAtUnixMs = nowMs;
                    dirty = true;
                    LogMessages.CheckpointEvicted(
                        _logger, name, entry.Lsn, TimeSpan.FromMilliseconds(nowMs - entry.LastActiveUnixMs), options.SubscriberExpirySeconds);
                }
            }

            if (dirty)
                _store.Save();
        }

        _sweepTimer?.Change(SweepPeriod(options.SubscriberExpirySeconds), SweepPeriod(options.SubscriberExpirySeconds));
    }

    private static TimeSpan SweepPeriod(int expirySeconds) =>
        TimeSpan.FromSeconds(Math.Clamp(expirySeconds / 10.0, 1, 3600));

    private static TimeSpan BackoffDelay(int baseMs, int attempt)
    {
        var ms = Math.Min((long)Math.Max(1, baseMs) << Math.Min(attempt - 1, 16), 30_000);
        return TimeSpan.FromMilliseconds(ms);
    }

    private static EventEnvelope[] ToEnvelopes(CommitRecord record)
    {
        var envelopes = new EventEnvelope[record.Events.Count];
        for (var i = 0; i < record.Events.Count; i++)
        {
            envelopes[i] = new EventEnvelope
            {
                Lsn = record.Lsn,
                Timestamp = record.Timestamp,
                Event = record.Events[i],
            };
        }

        return envelopes;
    }

    private readonly record struct WindowEntry(ulong Lsn, IReadOnlyList<EventEnvelope> Events);

    private sealed class Subscriber(EventHandlerRegistration registration)
    {
        public EventHandlerRegistration Registration { get; } = registration;

        public string Name => Registration.Name;

        public SemaphoreSlim Signal { get; } = new(0);

        public ulong Cursor { get; set; }

        public bool LostPlace { get; set; }
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, string, ulong, int, double, Exception?> HandlerRetryMessage =
            LoggerMessage.Define<string, string, ulong, int, double>(
                LogLevel.Warning,
                new EventId(1401, "EventHandlerRetry"),
                "Subscriber '{Subscriber}' failed handling '{EventType}' at LSN {Lsn} (attempt {Attempt}); retrying in {BackoffMs:F0}ms.");

        public static void HandlerRetry(ILogger logger, string subscriber, string eventType, ulong lsn, int attempt, double backoffMs, Exception failure) =>
            HandlerRetryMessage(logger, subscriber, eventType, lsn, attempt, backoffMs, failure);

        private static readonly Action<ILogger, string, string, ulong, int, Exception?> DeadLetteredMessage =
            LoggerMessage.Define<string, string, ulong, int>(
                LogLevel.Error,
                new EventId(1402, "EventDeadLettered"),
                "Subscriber '{Subscriber}' dead-lettered '{EventType}' at LSN {Lsn} after {Attempts} attempt(s); the event is recorded under Events:DeadLetterPath and delivery moves on.");

        public static void DeadLettered(ILogger logger, string subscriber, string eventType, ulong lsn, int attempts, Exception failure) =>
            DeadLetteredMessage(logger, subscriber, eventType, lsn, attempts, failure);

        private static readonly Action<ILogger, string, ulong, double, int, Exception?> CheckpointEvictedMessage =
            LoggerMessage.Define<string, ulong, double, int>(
                LogLevel.Warning,
                new EventId(1403, "SubscriberCheckpointEvicted"),
                "Event subscriber '{Subscriber}' has been idle at LSN {Lsn} for {IdleHours:F1}h, past Events:SubscriberExpirySeconds ({ExpirySeconds}s); its checkpoint is evicted and no longer pins log retention. If it returns it will start from current state.");

        public static void CheckpointEvicted(ILogger logger, string subscriber, ulong lsn, TimeSpan idle, int expirySeconds) =>
            CheckpointEvictedMessage(logger, subscriber, lsn, idle.TotalHours, expirySeconds, null);

        private static readonly Action<ILogger, string, ulong, ulong, Exception?> SubscriberLostPlaceMessage =
            LoggerMessage.Define<string, ulong, ulong>(
                LogLevel.Warning,
                new EventId(1404, "SubscriberLostPlace"),
                "Event subscriber '{Subscriber}' returned after its checkpoint (LSN {EvictedLsn}) was evicted; it has lost its place and starts from current state at LSN {Head}. Events between the two were not delivered to it.");

        public static void SubscriberLostPlace(ILogger logger, string subscriber, ulong evictedLsn, ulong head) =>
            SubscriberLostPlaceMessage(logger, subscriber, evictedLsn, head, null);
    }
}

/// <summary>One subscriber's live state: its name, checkpoint, and whether eviction cost it its place.</summary>
public readonly record struct EventSubscriberStatus(string Name, ulong CheckpointLsn, bool LostPlace);
