using Microsoft.Extensions.Logging;

namespace MelangeDB.Core;

/// <summary>
/// The registration seam <c>HotStore:Engine</c> selects by: a storage package registers a provider
/// (FASTER's <c>UseFasterHotStore()</c>), and <c>Auto</c> picks it when present — selection by
/// registration, not by path. The in-memory store needs no provider; it is the built-in fallback.
/// </summary>
public interface IHotStoreProvider
{
    /// <summary>The engine this provider supplies, matched against <c>HotStore:Engine</c>.</summary>
    HotStoreEngine Engine { get; }

    /// <summary>Creates the store. Called once, before recovery replays the log into it.</summary>
    IHotStore Create(HotStoreContext context);
}

/// <summary>Everything a hot store needs at construction.</summary>
public sealed class HotStoreContext
{
    /// <summary>The registered schema.</summary>
    public required SchemaRegistry Schema { get; init; }

    /// <summary>The engine's options; the store reads <c>HotStore:*</c> and <c>Residency:*</c>.</summary>
    public required MelangeDbOptions Options { get; init; }

    /// <summary>Each table's resolved residency — config override over attribute; see <see cref="ResidencyResolver"/>.</summary>
    public required IReadOnlyDictionary<TableId, Residency> Residency { get; init; }

    /// <summary>The host's logger factory, or a null factory outside a host.</summary>
    public required ILoggerFactory LoggerFactory { get; init; }
}

/// <summary>
/// Resolves each table's effective residency: the per-table configuration override wins over the
/// attribute, and a table whose attribute leaves residency unspecified takes
/// <c>Residency:Default</c>. Because <see cref="MelangeDB.Residency.Paged"/> is the attribute's
/// default value, an attribute explicitly declaring Paged is indistinguishable from silence — under
/// a non-Paged configured default, the per-table override is how a table is pinned back down.
/// <see cref="MelangeDB.Residency.Auto"/> survives resolution; the store itself resolves it against
/// <c>Residency:AutoThresholdBytes</c> as the table grows.
/// </summary>
public static class ResidencyResolver
{
    public static IReadOnlyDictionary<TableId, Residency> Resolve(SchemaRegistry schema, ResidencyOptions options)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(options);
        var resolved = new Dictionary<TableId, Residency>(schema.Tables.Count);
        foreach (var table in schema.Tables)
        {
            if (options.PerTable.TryGetValue(table.Name, out var configured))
                resolved[table.Id] = configured;
            else if (table.Residency != Residency.Paged)
                resolved[table.Id] = table.Residency;
            else
                resolved[table.Id] = options.Default;
        }

        return resolved;
    }
}
