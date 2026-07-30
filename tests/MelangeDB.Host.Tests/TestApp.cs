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

    public static IHost Build(string root, IDictionary<string, string?>? settings = null, Action<HostApplicationBuilder>? configure = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
        {
            DisableDefaults = true,
        });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(root, "hot"),
        });
        if (settings is not null)
            builder.Configuration.AddInMemoryCollection(settings);

        builder.Services.Configure<FeatureOptions>(builder.Configuration.GetSection("Feature"));
        builder.Services.AddScoped<ScopedProbe>();
        builder.Services.AddSingleton<SingletonProbe>();
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Note).Assembly)
            .AddReducersFrom(typeof(NoteReducers).Assembly));
        builder.Services.AddHealthChecks();
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
}
