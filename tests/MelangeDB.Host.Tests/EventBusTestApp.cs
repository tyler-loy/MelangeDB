using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Host.Tests;

/// <summary>A note was published — the workhorse event of the bus tests.</summary>
public sealed record NotePublished(string Text, Identity Author);

/// <summary>Carries no row writes with it: proves a publish-only transaction still commits.</summary>
public sealed record GateEvent(string Tag);

/// <summary>One hop of the event → reducer → event cycle the depth guard must bound.</summary>
public sealed record ChainEvent(int Step);

/// <summary>Singleton hooks the event-bus tests observe deliveries and steer failures through.</summary>
public sealed class EventProbe
{
    /// <summary>Everything successfully handled, tagged "handler:payload".</summary>
    public ConcurrentQueue<string> Received { get; } = [];

    /// <summary>Released once per successful handling, any handler.</summary>
    public SemaphoreSlim Delivered { get; } = new(0);

    /// <summary>Released once per <see cref="FailingNoteHandler"/> attempt, success or not.</summary>
    public SemaphoreSlim Attempted { get; } = new(0);

    /// <summary>How many more times <see cref="FailingNoteHandler"/> throws. int.MaxValue = always.</summary>
    public int FailuresRemaining;

    /// <summary>What <see cref="GateHandler"/> awaits before completing; complete it to unblock.</summary>
    public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Released when <see cref="GateHandler"/> starts an attempt, before it blocks.</summary>
    public SemaphoreSlim GateEntered { get; } = new(0);

    /// <summary>The chain steps <see cref="ChainHandler"/> observed, in order.</summary>
    public ConcurrentQueue<int> ChainObserved { get; } = [];

    /// <summary>Waits for a condition driven by background delivery; fails the test after 10s.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string reason)
    {
        for (var i = 0; i < 1000; i++)
        {
            if (condition())
                return;
            await Task.Delay(10);
        }

        Xunit.Assert.Fail($"Timed out waiting: {reason}");
    }
}

public sealed class EventReducers
{
    [Reducer]
    public void PublishNote(ReducerContext ctx, string text)
    {
        ctx.Db.Note.Insert(new Note { Author = ctx.Caller, Text = text, Score = 0 });
        ctx.Publish(new NotePublished(text, ctx.Caller));
    }

    [Reducer]
    public void PublishNoteAndThrow(ReducerContext ctx, string text)
    {
        ctx.Db.Note.Insert(new Note { Author = ctx.Caller, Text = text, Score = 0 });
        ctx.Publish(new NotePublished(text, ctx.Caller));
        throw new RejectedException("rolled back: the event above must reach nobody");
    }

    [Reducer]
    public void PublishGate(ReducerContext ctx, string tag) => ctx.Publish(new GateEvent(tag));

    [Reducer]
    public void StartChain(ReducerContext ctx) => ctx.Publish(new ChainEvent(0));

    [Reducer]
    public void ContinueChain(ReducerContext ctx, int step) => ctx.Publish(new ChainEvent(step));
}

/// <summary>The well-behaved subscriber; constructor injection proves handlers are DI-resolved.</summary>
public sealed class AuditNoteHandler(EventProbe probe, ILogger<AuditNoteHandler> logger) : IEventHandler<NotePublished>
{
    public Task HandleAsync(NotePublished @event, CancellationToken cancellationToken)
    {
        logger.LogDebug("Handling {Text}", @event.Text);
        probe.Received.Enqueue($"audit:{@event.Text}");
        probe.Delivered.Release();
        return Task.CompletedTask;
    }
}

/// <summary>A second subscriber to the same event, whose failures the probe controls.</summary>
public sealed class FailingNoteHandler(EventProbe probe) : IEventHandler<NotePublished>
{
    public Task HandleAsync(NotePublished @event, CancellationToken cancellationToken)
    {
        probe.Attempted.Release();
        if (Interlocked.Decrement(ref probe.FailuresRemaining) >= 0)
            throw new InvalidOperationException($"deliberate failure handling '{@event.Text}'");
        probe.Received.Enqueue($"failing:{@event.Text}");
        probe.Delivered.Release();
        return Task.CompletedTask;
    }
}

/// <summary>Blocks on the probe's gate — how a test wedges a subscriber to make it miss events.</summary>
public sealed class GateHandler(EventProbe probe) : IEventHandler<GateEvent>
{
    public async Task HandleAsync(GateEvent @event, CancellationToken cancellationToken)
    {
        probe.GateEntered.Release();
        await probe.Gate.Task.WaitAsync(cancellationToken);
        probe.Received.Enqueue($"gate:{@event.Tag}");
        probe.Delivered.Release();
    }
}

/// <summary>
/// Calls a reducer from inside a handler — legal, a new transaction — which publishes again: the
/// cycle the depth guard exists for. The publish at the depth limit throws, aborting the reducer
/// and failing this handler, which ends the chain in the dead-letter path.
/// </summary>
public sealed class ChainHandler(EventProbe probe, MelangeReducerHost reducers) : IEventHandler<ChainEvent>
{
    public Task HandleAsync(ChainEvent @event, CancellationToken cancellationToken)
    {
        probe.ChainObserved.Enqueue(@event.Step);
        reducers.Call("ContinueChain", TestApp.Caller, @event.Step + 1);
        return Task.CompletedTask;
    }
}

/// <summary>Captures structured log entries so tests can assert on stable EventIds.</summary>
public sealed class LogCollector : ILoggerProvider
{
    public ConcurrentQueue<(int EventId, string EventName, LogLevel Level, string Message)> Entries { get; } = [];

    public bool Has(int eventId) => Entries.Any(e => e.EventId == eventId);

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(LogCollector owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            owner.Entries.Enqueue((eventId.Id, eventId.Name ?? string.Empty, logLevel, formatter(state, exception)));
    }
}
