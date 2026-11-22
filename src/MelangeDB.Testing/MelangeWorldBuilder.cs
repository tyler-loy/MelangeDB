using System.Reflection;
using MelangeDB.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MelangeDB.Testing;

/// <summary>Describes a <see cref="MelangeWorld"/>; <see cref="Build"/> starts it.</summary>
public sealed class MelangeWorldBuilder
{
    private readonly Assembly[] _gameAssemblies;
    private readonly List<Action<IServiceCollection>> _services = [];
    private readonly List<Action<MelangeDbBuilder>> _melange = [];
    private readonly Dictionary<string, string?> _settings = new(StringComparer.OrdinalIgnoreCase);
    private bool _durableCommits;

    internal MelangeWorldBuilder(Assembly[] gameAssemblies)
    {
        ArgumentNullException.ThrowIfNull(gameAssemblies);
        if (gameAssemblies.Length == 0)
            throw new ArgumentException("Name at least one assembly carrying the game's generated tables and reducers.", nameof(gameAssemblies));
        _gameAssemblies = [.. gameAssemblies.Distinct()];
    }

    internal ManualClock ClockInstance { get; private set; } = new();

    /// <summary>
    /// Registers or substitutes the game's dependencies, after MelangeDB's own registrations, so
    /// a registration here wins: <c>.WithServices(s =&gt; s.AddSingleton&lt;ILootTable, FixedLoot&gt;())</c>.
    /// </summary>
    public MelangeWorldBuilder WithServices(Action<IServiceCollection> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _services.Add(configure);
        return this;
    }

    /// <summary>
    /// Adds to the MelangeDB registration beyond the game's tables, reducers and producers, which
    /// are always registered: event handlers, a storage engine, anything the host's builder takes.
    /// </summary>
    public MelangeWorldBuilder ConfigureMelange(Action<MelangeDbBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _melange.Add(configure);
        return this;
    }

    /// <summary>Sets a configuration key, as appsettings would: <c>.WithSetting("MelangeDb:RateLimit:Enabled", "false")</c>.</summary>
    public MelangeWorldBuilder WithSetting(string key, string? value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _settings[key] = value;
        return this;
    }

    /// <summary>Starts the world's clock at <paramref name="start"/> instead of <see cref="ManualClock.DefaultStart"/>.</summary>
    public MelangeWorldBuilder StartingAt(DateTimeOffset start)
    {
        ClockInstance = new ManualClock(start);
        return this;
    }

    /// <summary>
    /// Fsyncs every commit before it returns (<c>CommitLog:FsyncPolicy = OnCommit</c>), as a
    /// production deployment that cannot lose an acknowledged write runs. The default is an
    /// interval flush, which is much faster across a large suite and loses nothing a
    /// <see cref="MelangeWorld.Restart"/> can see, because a restart closes the log cleanly. A
    /// test about what a <em>crash</em> leaves behind wants this; a test about game logic does not.
    /// </summary>
    public MelangeWorldBuilder WithDurableCommits()
    {
        _durableCommits = true;
        return this;
    }

    /// <summary>Creates the data directory and starts the world.</summary>
    public MelangeWorld Build()
    {
        var root = Directory.CreateTempSubdirectory("melange-world-").FullName;
        try
        {
            return new MelangeWorld(this, root);
        }
        catch
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
            }

            throw;
        }
    }

    internal IHost BuildHost(string root)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(root, "hot"),
            ["MelangeDb:Events:DeadLetterPath"] = Path.Combine(root, "deadletter"),
            ["MelangeDb:CommitLog:FsyncPolicy"] = _durableCommits ? "OnCommit" : "Interval",

            // A test reports its own residency if it cares; a line per world is noise.
            ["MelangeDb:Residency:ReportOnStartup"] = "false",
        });
        builder.Configuration.AddInMemoryCollection(_settings);

        builder.Services.AddSingleton<TimeProvider>(ClockInstance);
        builder.Services.AddMelangeDb(melange =>
        {
            foreach (var assembly in _gameAssemblies)
                melange.AddTablesFrom(assembly).AddReducersFrom(assembly).AddProducersFrom(assembly);
            foreach (var configure in _melange)
                configure(melange);
        });
        builder.Services.AddHealthChecks();
        foreach (var configure in _services)
            configure(builder.Services);
        return builder.Build();
    }
}
