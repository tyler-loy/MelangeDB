using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// A producer reads one pinned view for its whole call and has no serialized fallback, so a store
/// that cannot pin refuses to start once any producer is registered — and only then.
/// </summary>
public class ProducerStartupTests
{
    private static readonly ProducerDescriptor Descriptor = new(
        "Answer",
        typeof(object),
        static (ref ReducerArgsReader reader) => reader.End(),
        static (_, _, _, _) => new ValueTask<object?>(42),
        static (writer, value) => writer.WriteInt64((int)value!));

    [Fact]
    public void A_store_without_pinned_reads_refuses_to_start_with_producers_registered()
    {
        using var harness = new EngineHarness(storeProvider: ReadViewlessStore.Provider);

        var failure = Assert.Throws<InvalidOperationException>(() =>
            MelangeDbHostedService.RequireReadViewsForProducers(harness.Engine, Host(harness, Descriptor)));
        Assert.Contains("IReadViewSource", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_same_store_starts_with_no_producers_and_a_pinning_store_starts_with_them()
    {
        using var viewless = new EngineHarness(storeProvider: ReadViewlessStore.Provider);
        MelangeDbHostedService.RequireReadViewsForProducers(viewless.Engine, Host(viewless));

        using var pinning = new EngineHarness();
        MelangeDbHostedService.RequireReadViewsForProducers(pinning.Engine, Host(pinning, Descriptor));
    }

    private static MelangeProducerHost Host(EngineHarness harness, params ProducerDescriptor[] producers) =>
        new(
            harness.Engine,
            new ProducerRegistry(producers),
            new NoScopes(),
            new FixedOptions(harness.Options));

    private sealed class NoScopes : IServiceScopeFactory
    {
        public IServiceScope CreateScope() => throw new NotSupportedException("Startup checks resolve nothing.");
    }

    private sealed class FixedOptions(MelangeDbOptions value) : IOptionsMonitor<MelangeDbOptions>
    {
        public MelangeDbOptions CurrentValue => value;

        public MelangeDbOptions Get(string? name) => value;

        public IDisposable? OnChange(Action<MelangeDbOptions, string?> listener) => null;
    }
}
