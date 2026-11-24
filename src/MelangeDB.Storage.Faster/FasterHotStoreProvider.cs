using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// The provider <c>UseFasterHotStore()</c> registers; <c>HotStore:Engine</c> Auto selects it by its
/// presence, and <c>HotStore:PagedEngine</c> picks the engine under it.
/// </summary>
public sealed class FasterHotStoreProvider : IHotStoreProvider
{
    private readonly IPagedKvFactory? _engine;
    private readonly PagedEngine? _pagedEngine;

    /// <summary>The paged store over the engine <c>HotStore:PagedEngine</c> names.</summary>
    public FasterHotStoreProvider()
    {
    }

    /// <summary>The paged store over a given engine, whatever the options say: the tests' and the bake-off's override.</summary>
    internal FasterHotStoreProvider(IPagedKvFactory engine) => _engine = engine;

    /// <summary>The paged store over a given engine kind, configured from the options as the default provider does.</summary>
    internal FasterHotStoreProvider(PagedEngine engine) => _pagedEngine = engine;

    public HotStoreEngine Engine => HotStoreEngine.Faster;

    public IHotStore Create(HotStoreContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new FasterHotStore(
            context,
            _engine ?? PagedEngines.For(_pagedEngine ?? context.Options.HotStore.PagedEngine, context.Options.HotStore, context.LoggerFactory));
    }
}

/// <summary>Registers the FASTER storage engine on the MelangeDB builder.</summary>
public static class FasterMelangeDbBuilderExtensions
{
    /// <summary>
    /// Registers the FASTER-backed hot store. With <c>HotStore:Engine</c> at its default of
    /// <c>Auto</c>, this registration <em>is</em> the selection — selection by registration, not
    /// by path. The in-memory store remains available by setting <c>HotStore:Engine</c> to
    /// <c>InMemory</c> without removing this call.
    /// </summary>
    public static MelangeDbBuilder UseFasterHotStore(this MelangeDbBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<IHotStoreProvider, FasterHotStoreProvider>();
        return builder;
    }
}
