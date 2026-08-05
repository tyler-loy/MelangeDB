using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The event bus contract: transactional publication (an event never escapes a rolled-back
/// transaction), DI-resolved handlers outside the transaction, per-subscriber durable checkpoints
/// with restart catch-up, retry-then-dead-letter that never stalls the pipeline, the publish-depth
/// cycle guard, checkpoint expiry, and the transport seam.
/// </summary>
public class EventBusTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-events-").FullName;
    private readonly ManualTimeProvider _time = new();
    private readonly LogCollector _logs = new();

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private IHost BuildHost(IDictionary<string, string?>? settings = null, Action<MelangeDbBuilder>? events = null) =>
        TestApp.Build(
            _root,
            settings,
            builder =>
            {
                builder.Services.AddSingleton<TimeProvider>(_time);
                builder.Logging.AddProvider(_logs);
            },
            events);

    private async Task<IHost> StartHostAsync(IDictionary<string, string?>? settings = null, Action<MelangeDbBuilder>? events = null)
    {
        var host = BuildHost(settings, events);
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static EventProbe Probe(IHost host) => host.Services.GetRequiredService<EventProbe>();

    private static async Task WaitDeliveredAsync(EventProbe probe, int count)
    {
        for (var i = 0; i < count; i++)
            Assert.True(await probe.Delivered.WaitAsync(TimeSpan.FromSeconds(10)), $"delivery {i + 1} of {count} never arrived");
    }

    private string DeadLetterFile => Path.Combine(_root, "deadletter", "melange.deadletter.ndjson");

    /// <summary>
    /// Only fully flushed dead-letter lines. The store creates the file before its first flush,
    /// so an existence check can observe an empty file, and a plain <c>File.ReadLines</c> can
    /// both surface a partial trailing line and collide with the writer's open handle mid-append.
    /// </summary>
    private string[] CompleteDeadLetterLines()
    {
        using var stream = new FileStream(DeadLetterFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream);
        var text = reader.ReadToEnd();
        var end = text.LastIndexOf('\n');
        return end < 0
            ? []
            : text[..end].Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(static l => l.TrimEnd('\r')).ToArray();
    }

    [Fact]
    public async Task A_committed_event_reaches_both_DI_resolved_handlers_outside_the_transaction()
    {
        var handled = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "MelangeDB",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "melange.event.handle")
                {
                    lock (handled)
                    {
                        handled.Add(activity);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        using var host = await StartHostAsync();
        var lsn = host.Reducers().Call("PublishNote", TestApp.Caller, "hello");
        await WaitDeliveredAsync(Probe(host), 2);

        Assert.Contains("audit:hello", Probe(host).Received);
        Assert.Contains("failing:hello", Probe(host).Received);

        // The record carries the event as payload, alongside the row writes.
        var record = host.Engine().Log.ReadFrom(lsn).Single();
        var evt = Assert.Single(record.Events);
        Assert.Equal(typeof(NotePublished).FullName, evt.EventType);
        Assert.Equal(0, evt.Depth);

        // Checkpoints converge to the head, and the phase-07 truncation floor is exposed.
        await EventProbe.WaitUntilAsync(
            () => host.Bus().Subscribers.All(s => s.CheckpointLsn >= lsn),
            "checkpoints never reached the publishing LSN");
        Assert.NotNull(host.Bus().MinimumLiveCheckpointLsn);
        Assert.True(host.Bus().MinimumLiveCheckpointLsn <= host.Engine().Log.HeadLsn);

        // The handler span is a new trace linked to the emitter, never parented under it.
        await EventProbe.WaitUntilAsync(
            () =>
            {
                lock (handled)
                {
                    return handled.Any(a => a.Links.Any());
                }
            },
            "no linked melange.event.handle span was recorded");
        lock (handled)
        {
            var span = handled.First(a => a.Links.Any());
            Assert.Null(span.Parent);
            Assert.Equal(typeof(NotePublished).FullName, span.GetTagItem("melange.event.type"));
        }

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task An_event_published_by_a_reducer_that_throws_reaches_nobody()
    {
        // THE property: the transactional outbox exists so a notification can never escape a
        // state change that didn't happen.
        using var host = await StartHostAsync();
        var headBefore = host.Engine().Log.HeadLsn;
        Assert.Throws<RejectedException>(() =>
            host.Reducers().Call("PublishNoteAndThrow", TestApp.Caller, "ghost"));
        Assert.Equal(headBefore, host.Engine().Log.HeadLsn);

        // A later committed event flows normally; per-subscriber order means if "ghost" were ever
        // going to arrive it would arrive before "real".
        host.Reducers().Call("PublishNote", TestApp.Caller, "real");
        await WaitDeliveredAsync(Probe(host), 2);

        Assert.DoesNotContain(Probe(host).Received, r => r.Contains("ghost"));
        Assert.Contains("audit:real", Probe(host).Received);
        Assert.DoesNotContain(
            host.Engine().Log.ReadFrom(1).SelectMany(r => r.Events),
            e => System.Text.Encoding.UTF8.GetString(e.Payload.Span).Contains("ghost"));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task One_failing_handler_does_not_prevent_the_other_from_receiving()
    {
        using var host = await StartHostAsync();
        Probe(host).FailuresRemaining = int.MaxValue;

        host.Reducers().Call("PublishNote", TestApp.Caller, "shared");
        await WaitDeliveredAsync(Probe(host), 1);
        Assert.Contains("audit:shared", Probe(host).Received);
        Assert.DoesNotContain("failing:shared", Probe(host).Received);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_failing_handler_retries_on_backoff_then_dead_letters_without_stalling_anything()
    {
        long deadLettered = 0;
        using var meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name == "melange.events.deadlettered")
                    l.EnableMeasurementEvents(instrument);
            },
        };
        meterListener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref deadLettered, value));
        meterListener.Start();

        using var host = await StartHostAsync();
        var probe = Probe(host);
        probe.FailuresRemaining = int.MaxValue;

        host.Reducers().Call("PublishNote", TestApp.Caller, "poison");
        Assert.True(await probe.Attempted.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), "first attempt never happened");

        // The retry waits on the clock: no second attempt until backoff elapses.
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, probe.Attempted.CurrentCount);

        // While one subscriber is in backoff, the pipeline is open: transactions commit and the
        // healthy subscriber keeps receiving.
        host.Reducers().Call("AddNote", TestApp.Caller, "unblocked", 1.0);
        host.Reducers().Call("PublishNote", TestApp.Caller, "after");
        await EventProbe.WaitUntilAsync(() => probe.Received.Contains("audit:after"), "healthy subscriber stalled");
        Assert.Contains("audit:poison", probe.Received);

        _time.Advance(TimeSpan.FromMilliseconds(600));
        Assert.True(await probe.Attempted.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), "backoff elapsed but no retry");

        // Exhaust the remaining retries; the event dead-letters and delivery moves on.
        for (var i = 0; i < 20 && !File.Exists(DeadLetterFile); i++)
        {
            _time.Advance(TimeSpan.FromSeconds(3));
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        await EventProbe.WaitUntilAsync(() => File.Exists(DeadLetterFile), "no dead-letter file appeared");
        await EventProbe.WaitUntilAsync(
            () =>
            {
                using var stream = new FileStream(DeadLetterFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var reader = new StreamReader(stream);
                return reader.ReadToEnd().Contains("poison");
            },
            "the poisoned event never dead-lettered");

        var line = CompleteDeadLetterLines().First(l => l.Contains("poison"));
        using var record = JsonDocument.Parse(line);
        Assert.Equal(typeof(FailingNoteHandler).FullName, record.RootElement.GetProperty("Subscriber").GetString());
        Assert.Equal(typeof(NotePublished).FullName, record.RootElement.GetProperty("EventType").GetString());
        Assert.Equal(4, record.RootElement.GetProperty("Attempts").GetInt32());
        Assert.Equal("poison", record.RootElement.GetProperty("Payload").GetProperty("Text").GetString());

        Assert.True(Interlocked.Read(ref deadLettered) >= 1);
        Assert.True(_logs.Has(1401), "no EventHandlerRetry (1401) log entry");
        Assert.True(_logs.Has(1402), "no EventDeadLettered (1402) log entry");
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_subscriber_down_for_N_transactions_catches_up_from_its_checkpoint_on_restart()
    {
        using (var first = await StartHostAsync())
        {
            // The gate handler wedges on the first event; two more commit behind it. Its
            // checkpoint never advances, which is the whole point.
            first.Reducers().Call("PublishGate", TestApp.Caller, "g1");
            first.Reducers().Call("PublishGate", TestApp.Caller, "g2");
            first.Reducers().Call("PublishGate", TestApp.Caller, "g3");
            Assert.True(await Probe(first).GateEntered.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), "gate handler never started");
            await first.StopAsync(TestContext.Current.CancellationToken);
        }

        // A publish-only transaction wrote no rows yet committed a record — asserted by catching
        // up against those records now.
        using var second = BuildHost();
        Probe(second).Gate.SetResult();
        await second.StartAsync(TestContext.Current.CancellationToken);
        await WaitDeliveredAsync(Probe(second), 3);

        Assert.Equal(
            ["gate:g1", "gate:g2", "gate:g3"],
            Probe(second).Received.Where(r => r.StartsWith("gate:")).Order().ToArray());
        await second.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Publishing_through_an_event_reducer_event_cycle_is_depth_limited()
    {
        using var host = await StartHostAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Events:HandlerRetries"] = "0",
        });

        host.Reducers().Call("StartChain", TestApp.Caller);
        await EventProbe.WaitUntilAsync(
            () => File.Exists(DeadLetterFile) && CompleteDeadLetterLines().Length >= 1,
            "the chain never hit the depth limit");
        await EventProbe.WaitUntilAsync(() => Probe(host).ChainObserved.Count >= 4, "chain deliveries missing");

        // Depths 0..3 published; the publish at ambient depth 4 (= Events:MaxPublishDepth) threw,
        // aborting the reducer, failing the handler, and dead-lettering the chain's last event.
        Assert.Equal([0, 1, 2, 3], Probe(host).ChainObserved.Order().ToArray());
        Assert.Equal(4, host.Engine().Log.ReadFrom(1).SelectMany(r => r.Events)
            .Count(e => e.EventType == typeof(ChainEvent).FullName));

        var line = CompleteDeadLetterLines().Single();
        using var record = JsonDocument.Parse(line);
        Assert.Equal(typeof(ChainEvent).FullName, record.RootElement.GetProperty("EventType").GetString());
        Assert.Equal(3, record.RootElement.GetProperty("Depth").GetInt32());
        Assert.Contains("MaxPublishDepth", record.RootElement.GetProperty("Error").GetString());
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task An_idle_checkpoint_is_evicted_loudly_and_a_returning_subscriber_starts_from_current_state()
    {
        var settings = new Dictionary<string, string?> { ["MelangeDb:Events:SubscriberExpirySeconds"] = "60" };
        ulong headAfterFirst;
        using (var first = await StartHostAsync(settings))
        {
            first.Reducers().Call("PublishNote", TestApp.Caller, "first");
            await WaitDeliveredAsync(Probe(first), 2);
            headAfterFirst = first.Engine().Log.HeadLsn;

            // A handler records its receipt from inside DeliverAsync, and the checkpoint it
            // advances is written after that call returns — so "delivered" happens strictly
            // before "checkpointed". Stopping on the delivery signal can land in that gap, and
            // a loop cancelled there leaves its checkpoint put and redelivers next start, which
            // is at-least-once working as designed. What this test needs before it stops the
            // host is the checkpoint, so wait for the checkpoint.
            await EventProbe.WaitUntilAsync(
                () => first.Bus().MinimumLiveCheckpointLsn == headAfterFirst,
                "the subscribers never checkpointed at head");

            await first.StopAsync(TestContext.Current.CancellationToken);
        }

        // A deployment without the handlers: every checkpoint is now an orphan pinning retention.
        using (var second = await StartHostAsync(settings, events: _ => { }))
        {
            Assert.Equal((ulong?)headAfterFirst, second.Bus().MinimumLiveCheckpointLsn);

            _time.Advance(TimeSpan.FromSeconds(30));
            Assert.NotNull(second.Bus().MinimumLiveCheckpointLsn); // idle 30s < 60s: still pinned

            _time.Advance(TimeSpan.FromSeconds(45));
            await EventProbe.WaitUntilAsync(
                () => second.Bus().MinimumLiveCheckpointLsn is null,
                "the idle checkpoints were never evicted");

            // The sweep flips the checkpoint state first and logs one statement later, on its
            // own timer thread — polling the state can win that race by a preemption, so the log
            // entry gets the same wait the state did.
            await EventProbe.WaitUntilAsync(
                () => _logs.Has(1403), "no SubscriberCheckpointEvicted (1403) log entry");

            // Committed while nobody subscribed — and after eviction, gone for good.
            second.Reducers().Call("PublishNote", TestApp.Caller, "missed");
            await second.StopAsync(TestContext.Current.CancellationToken);
        }

        // The subscriber returns: told it lost its place, starting from current state.
        using var third = await StartHostAsync(settings);
        Assert.True(_logs.Has(1404), "no SubscriberLostPlace (1404) log entry");
        Assert.True(
            third.Bus().Subscribers.Single(s => s.Name == typeof(AuditNoteHandler).FullName).LostPlace,
            "the returning subscriber was not told it lost its place");

        third.Reducers().Call("PublishNote", TestApp.Caller, "fresh");
        await WaitDeliveredAsync(Probe(third), 2);
        Assert.Contains("audit:fresh", Probe(third).Received);
        Assert.DoesNotContain("audit:missed", Probe(third).Received);
        await third.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_replacement_transport_carries_events_with_no_handler_changes()
    {
        var transport = new RecordingTransport();
        var host = TestApp.Build(
            _root,
            null,
            builder =>
            {
                builder.Services.AddSingleton<TimeProvider>(_time);
                builder.Services.AddSingleton<IEventTransport>(transport);
            });
        await host.StartAsync(TestContext.Current.CancellationToken);
        using var _ = host;

        host.Reducers().Call("PublishNote", TestApp.Caller, "via-custom-transport");
        await WaitDeliveredAsync(Probe(host), 2);

        Assert.Contains("audit:via-custom-transport", Probe(host).Received);
        Assert.True(transport.PublishedBatches >= 1, "the custom transport never saw the event");
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Stands in for phase 09's distributed transport: same seam, extra bookkeeping.</summary>
    private sealed class RecordingTransport : IEventTransport
    {
        private IEventInbox? _inbox;
        private int _published;

        public int PublishedBatches => Volatile.Read(ref _published);

        public void Connect(IEventInbox inbox) => _inbox = inbox;

        public void Publish(IReadOnlyList<EventEnvelope> batch)
        {
            Interlocked.Increment(ref _published);
            _inbox?.Receive(batch);
        }
    }
}
