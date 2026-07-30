using MelangeDB.Core;

namespace MelangeDB.Storage.Faster.Tests;

public enum StoreKind
{
    InMemory,
    Faster,
}

/// <summary>
/// One engine over a throwaway data directory, parameterized by hot-store engine — the fixture
/// that runs the same suite against <see cref="InMemoryHotStore"/> and
/// <see cref="FasterHotStore"/>. <see cref="Restart"/> simulates a process kill and restart: the
/// old engine is abandoned and a fresh one rebuilds from the snapshot and log alone, which for the
/// FASTER engine is its entire recovery story.
/// </summary>
internal sealed class StoreHarness : IDisposable
{
    private static readonly Type[] DefaultTables =
    [
        typeof(Creature),
        typeof(ItemDefinition),
        typeof(TerrainBlob),
        typeof(AutoSized),
        typeof(NamedThing),
    ];

    private readonly Type[] _tables;
    private readonly TimeProvider? _timeProvider;

    public StoreHarness(
        StoreKind kind,
        Action<MelangeDbOptions>? configure = null,
        Type[]? tables = null,
        TimeProvider? timeProvider = null)
    {
        Kind = kind;
        _tables = tables ?? DefaultTables;
        _timeProvider = timeProvider;
        Root = Directory.CreateTempSubdirectory("melange-faster-test-").FullName;
        Options = new MelangeDbOptions
        {
            HotStore =
            {
                Path = Path.Combine(Root, "hot"),
                Engine = kind == StoreKind.Faster ? HotStoreEngine.Faster : HotStoreEngine.InMemory,
                MemoryBudgetBytes = 8 * 1024 * 1024,
            },
            CommitLog = { Path = Path.Combine(Root, "log") },
        };
        configure?.Invoke(Options);
        Engine = CreateEngine();
    }

    public StoreKind Kind { get; }

    public string Root { get; }

    public MelangeDbOptions Options { get; }

    public MelangeEngine Engine { get; private set; }

    public static Identity Caller { get; } = Identity.Hash("faster-test-caller");

    public void Invoke(string reducerName, Action<ReducerContext> body) =>
        Engine.Invoke(reducerName, Caller, body);

    /// <summary>Restarts the engine, rebuilding from the snapshot and log alone.</summary>
    public void Restart()
    {
        Engine.Dispose();
        Engine = CreateEngine();
    }

    /// <summary>A deterministic, byte-faithful dump of every table: (table, key hex, row hex).</summary>
    public List<string> Dump()
    {
        var dump = new List<string>();
        foreach (var table in Engine.Schema.Tables)
        {
            foreach (var pair in Engine.HotStore.Scan(table.Id))
                dump.Add($"{table.Name}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}");
        }

        return dump;
    }

    public void Dispose()
    {
        Engine.Dispose();
        try
        {
            Directory.Delete(Root, recursive: true);
        }
        catch (IOException)
        {
            // A concurrently open read handle on Windows; the temp dir reaper gets it later.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private MelangeEngine CreateEngine() =>
        new(
            Options,
            new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables().Where(t => _tables.Contains(t.RowType))),
            loggerFactory: null,
            _timeProvider,
            Kind == StoreKind.Faster ? new FasterHotStoreProvider() : null);
}
