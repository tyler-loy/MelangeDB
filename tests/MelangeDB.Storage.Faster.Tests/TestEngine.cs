using MelangeDB.Core;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// The paged store's engine for this suite. <c>MELANGE_TEST_PAGED_ENGINE</c> names a
/// <see cref="PagedEngine"/> (faster, melangelog) and runs every test against it; CI runs the suite
/// once per engine. Unset is the provider's own choice, the <c>HotStore:PagedEngine</c> default.
/// </summary>
internal static class TestEngine
{
    public static IHotStoreProvider Provider() =>
        Environment.GetEnvironmentVariable("MELANGE_TEST_PAGED_ENGINE") is { Length: > 0 } name
            ? new FasterHotStoreProvider(Enum.Parse<PagedEngine>(name, ignoreCase: true))
            : new FasterHotStoreProvider();
}
