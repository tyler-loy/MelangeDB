using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Host.Tests;

[Table(Public = true)]
public partial struct Note
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public Identity Author;

    public string Text;

    public double Score;
}

[Table]
public partial struct Audit
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public string Entry;
}

/// <summary>Repeating timer rows: the world tick. Implicitly private, implicitly Local.</summary>
[Table(Scheduled = nameof(TickReducers.WorldTick))]
public partial struct WorldTickTimer
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;

    public int Payload;
}

/// <summary>One-shot timer rows; each fire deletes its row transactionally with its work.</summary>
[Table(Scheduled = nameof(TickReducers.RunOnce))]
public partial struct OneShotTimer
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;

    public string Tag;
}

/// <summary>What scheduled fires commit, so tick work is observable as ordinary rows.</summary>
[Table]
public partial struct TickLog
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public string Entry;
}

/// <summary>Singleton hooks the scheduler tests observe and steer fires through.</summary>
public sealed class SchedulerProbe
{
    private int _worldTicks;
    private int _oneShots;

    public int WorldTicks => Volatile.Read(ref _worldTicks);

    public int OneShots => Volatile.Read(ref _oneShots);

    /// <summary>Whether tick reducers write a TickLog row — off makes a fire write nothing.</summary>
    public bool WriteRows { get; set; } = true;

    public bool ThrowOnOneShot { get; set; }

    /// <summary>Runs inside the WorldTick body — how the overrun tests make a tick slow.</summary>
    public Action<ReducerContext>? OnWorldTick { get; set; }

    public void CountWorldTick() => Interlocked.Increment(ref _worldTicks);

    public void CountOneShot() => Interlocked.Increment(ref _oneShots);
}

public sealed class TickReducers(SchedulerProbe probe)
{
    [Reducer]
    public void WorldTick(ReducerContext ctx, WorldTickTimer timer)
    {
        probe.CountWorldTick();
        probe.OnWorldTick?.Invoke(ctx);
        if (probe.WriteRows)
            ctx.Db.TickLog.Insert(new TickLog { Entry = $"tick:{timer.Id}:{timer.Payload}" });
    }

    [Reducer]
    public void RunOnce(ReducerContext ctx, OneShotTimer timer)
    {
        if (probe.ThrowOnOneShot)
            throw new RejectedException("one-shot rejected");
        probe.CountOneShot();
        if (probe.WriteRows)
            ctx.Db.TickLog.Insert(new TickLog { Entry = $"once:{timer.Tag}" });
    }

    [Reducer]
    public void ScheduleTick(ReducerContext ctx, long intervalMs, int payload) =>
        ctx.Db.WorldTickTimer.Insert(new WorldTickTimer
        {
            ScheduledAt = ScheduleAt.Interval(TimeSpan.FromMilliseconds(intervalMs)),
            Payload = payload,
        });

    [Reducer]
    public void ScheduleOnce(ReducerContext ctx, Timestamp at, string tag) =>
        ctx.Db.OneShotTimer.Insert(new OneShotTimer { ScheduledAt = ScheduleAt.Instant(at), Tag = tag });

    [Reducer]
    public void ScheduleOnceAndThrow(ReducerContext ctx, Timestamp at)
    {
        ctx.Db.OneShotTimer.Insert(new OneShotTimer { ScheduledAt = ScheduleAt.Instant(at), Tag = "doomed" });
        throw new RejectedException("rolled back: the timer above must not survive");
    }

    [Reducer]
    public void RescheduleTick(ReducerContext ctx, ulong id, long intervalMs)
    {
        var timer = ctx.Db.WorldTickTimer.Id.Find(id) ?? throw new RejectedException("no such timer");
        ctx.Db.WorldTickTimer.Update(timer with { ScheduledAt = ScheduleAt.Interval(TimeSpan.FromMilliseconds(intervalMs)) });
    }

    [Reducer]
    public void CancelWorldTicks(ReducerContext ctx)
    {
        foreach (var timer in ctx.Db.WorldTickTimer.Iter().ToList())
            ctx.Db.WorldTickTimer.Id.Delete(timer.Id);
    }
}

/// <summary>The test's feature flag, bound from <c>Feature:</c>.</summary>
public sealed class FeatureOptions
{
    public bool Enabled { get; set; } = true;
}

/// <summary>Scoped per reducer call; records its lifetime so scope-per-call is observable.</summary>
public sealed class ScopedProbe : IDisposable
{
    public Guid Id { get; } = Guid.NewGuid();

    public bool Disposed { get; private set; }

    public void Dispose() => Disposed = true;
}

/// <summary>Singleton across calls; collects what each call's scope looked like.</summary>
public sealed class SingletonProbe
{
    public ConcurrentQueue<ScopedProbe> Scopes { get; } = [];
}

public sealed class NoteReducers(
    IOptionsMonitor<FeatureOptions> features,
    ScopedProbe probe,
    SingletonProbe singleton,
    ILogger<NoteReducers> logger)
{
    [Reducer]
    public void AddNote(ReducerContext ctx, string text, double score)
    {
        if (!features.CurrentValue.Enabled)
            throw new RejectedException("Notes are disabled.");
        singleton.Scopes.Enqueue(probe);
        ctx.Db.Note.Insert(new Note { Author = ctx.Caller, Text = text, Score = score });
        ctx.Db.Audit.Insert(new Audit { Entry = $"note:{text}" });
        logger.LogInformation("Added note {Text}", text);
    }

    [Reducer]
    public void AddMany(ReducerContext ctx, string[] texts, byte[] blob)
    {
        foreach (var text in texts)
            ctx.Db.Note.Insert(new Note { Author = ctx.Caller, Text = text, Score = 0 });
    }

    [Reducer]
    public void Clamp(ReducerContext ctx, int value, float x)
    {
        ctx.Db.Note.Insert(new Note { Author = ctx.Caller, Text = value.ToString(), Score = x });
    }
}

internal static class TestApp
{
    public static Identity Caller { get; } = Identity.Hash("host-tests");

    public static IHost Build(
        string root,
        IDictionary<string, string?>? settings = null,
        Action<HostApplicationBuilder>? configure = null,
        Action<MelangeDbBuilder>? events = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
        {
            DisableDefaults = true,
        });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(root, "hot"),
            ["MelangeDb:Events:DeadLetterPath"] = Path.Combine(root, "deadletter"),
        });
        if (settings is not null)
            builder.Configuration.AddInMemoryCollection(settings);

        builder.Services.Configure<FeatureOptions>(builder.Configuration.GetSection("Feature"));
        builder.Services.AddScoped<ScopedProbe>();
        builder.Services.AddSingleton<SingletonProbe>();
        builder.Services.AddSingleton<SchedulerProbe>();
        builder.Services.AddSingleton<EventProbe>();

        builder.Services.AddMelangeDb(melange =>
        {
            melange
                .AddTablesFrom(typeof(Note).Assembly)
                .AddReducersFrom(typeof(NoteReducers).Assembly);
            if (events is null)
                melange.AddEventHandlersFrom(typeof(Note).Assembly);
            else
                events(melange);
        });
        builder.Services.AddHealthChecks();

        // Runs after AddMelangeDb, so a test's registrations win by running last — including
        // replacing IEventTransport, since the last registration resolves.
        configure?.Invoke(builder);
        return builder.Build();
    }

    /// <summary>Mutates a configuration value and raises the change tokens — a live reload, no restart.</summary>
    public static void ReloadWith(this IHost host, string key, string value)
    {
        var root = (IConfigurationRoot)host.Services.GetRequiredService<IConfiguration>();
        root[key] = value;
        root.Reload();
    }

    public static MelangeEngine Engine(this IHost host) => host.Services.GetRequiredService<MelangeEngine>();

    public static MelangeReducerHost Reducers(this IHost host) => host.Services.GetRequiredService<MelangeReducerHost>();

    public static MelangeEventBus Bus(this IHost host) => host.Services.GetRequiredService<MelangeEventBus>();
}
