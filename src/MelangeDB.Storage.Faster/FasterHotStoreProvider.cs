using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MelangeDB.Storage.Faster;

/// <summary>The provider <c>UseFasterHotStore()</c> registers; <c>HotStore:Engine</c> Auto selects it by its presence.</summary>
public sealed class FasterHotStoreProvider : IHotStoreProvider
{
    public HotStoreEngine Engine => HotStoreEngine.Faster;

    public IHotStore Create(HotStoreContext context) => new FasterHotStore(context);
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
