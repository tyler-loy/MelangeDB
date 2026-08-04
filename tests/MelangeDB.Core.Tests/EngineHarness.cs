using Microsoft.Extensions.Logging;

namespace MelangeDB.Core.Tests;

/// <summary>
/// One engine over a throwaway data directory. <see cref="Restart"/> simulates a process kill and
/// restart: the old engine is abandoned (only the log's own durability guarantees carry over) and
/// a fresh engine rebuilds from the log alone.
/// </summary>
internal sealed class EngineHarness : IDisposable
{
    private static readonly Type[] DefaultTables =
    [
        typeof(Player),
        typeof(InventoryItem),
        typeof(Registration),
        typeof(TerrainChunk),
    ];

    private readonly Type[] _tables;
    private readonly bool _useReflectionSchema;
    private readonly ILoggerFactory? _loggerFactory;

    public EngineHarness(
        FsyncPolicy fsyncPolicy = FsyncPolicy.OnCommit,
        bool telemetryEnabled = true,
        Type[]? tables = null,
        bool useReflectionSchema = false,
        ILoggerFactory? loggerFactory = null)
    {
        _tables = tables ?? DefaultTables;
        _useReflectionSchema = useReflectionSchema;
        _loggerFactory = loggerFactory;
        Root = Directory.CreateTempSubdirectory("melange-test-").FullName;
        Options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(Root, "hot") },
            CommitLog = { Path = Path.Combine(Root, "log"), FsyncPolicy = fsyncPolicy },
            Telemetry = { Enabled = telemetryEnabled },
        };
        Engine = CreateEngine();
    }

    public string Root { get; }

    public MelangeDbOptions Options { get; }

    public MelangeEngine Engine { get; private set; }

    public string LogFilePath => Path.Combine(Options.CommitLog.Path, "melange.log");

    public static Identity Caller { get; } = Identity.Hash("test-caller");

    public void Invoke(string reducerName, Action<ReducerContext> body) =>
        Engine.Invoke(reducerName, Caller, body);

    /// <summary>
    /// Restarts the engine, rebuilding from the log alone. Windows file sharing requires closing
    /// the old handle first, but under <see cref="FsyncPolicy.OnCommit"/> every committed record
    /// was already durable before Dispose ran, so this is equivalent to a mid-run kill for every
    /// committed transaction; the kill-mid-append case is covered by the torn-record tests.
    /// </summary>
    public void Restart()
    {
        Engine.Dispose();
        Engine = CreateEngine();
    }

    /// <summary>A deterministic, byte-faithful dump of every table: (table, key hex, row hex).</summary>
    public List<string> Dump() => Dump(Engine.HotStore);

    public List<string> Dump(IHotStore store)
    {
        var dump = new List<string>();
        foreach (var table in Engine.Schema.Tables)
        {
            foreach (var pair in store.Scan(table.Id))
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
    }

    /// <summary>
    /// The default path uses the generated registration — codecs attached, no reflection — which is
    /// how phase 01's suite doubles as proof that generated schemas behave identically. The
    /// reflection path stays available for the format-compatibility tests.
    /// </summary>
    public SchemaRegistry CreateRegistry() =>
        _useReflectionSchema
            ? SchemaRegistry.FromTypes(_tables)
            : GeneratedRegistry(_tables);

    public static SchemaRegistry GeneratedRegistry(params Type[] tables) =>
        new(new MelangeDB.Generated.MelangeModel().Tables().Where(t => tables.Contains(t.RowType)));

    private MelangeEngine CreateEngine() =>
        new(Options, CreateRegistry(), _loggerFactory);
}
