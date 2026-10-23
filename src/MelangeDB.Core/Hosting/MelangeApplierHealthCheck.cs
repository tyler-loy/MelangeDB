using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// The <c>melange-applier</c> health check: unhealthy when any applier's checkpoint lags the log
/// head by more than <c>HealthChecks:ApplierLagThreshold</c> transactions, and degraded when an
/// applier reports a condition lag cannot show (<see cref="IApplierCondition"/>) — archive rows
/// dead-lettered while the checkpoint kept advancing. The whole two-tier
/// design rests on appliers being <em>allowed</em> to lag — so the dangerous failure is a silent
/// stall, writes succeeding while a projection falls hours behind. This check is that alarm's
/// health-endpoint form; <c>melange.applier.lag</c> is its metric form.
/// </summary>
internal sealed class MelangeApplierHealthCheck : IHealthCheck
{
    private readonly IServiceProvider _provider;
    private readonly MelangeDbRuntimeState _state;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    public MelangeApplierHealthCheck(IServiceProvider provider, MelangeDbRuntimeState state, IOptionsMonitor<MelangeDbOptions> options)
    {
        _provider = provider;
        _state = state;
        _options = options;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_state.Started)
            return Task.FromResult(HealthCheckResult.Unhealthy("MelangeDB has not started; no applier is running."));

        var threshold = _options.CurrentValue.HealthChecks.ApplierLagThreshold;
        var engine = (MelangeEngine)_provider.GetService(typeof(MelangeEngine))!;
        var lagging = engine.Appliers.Lags()
            .Where(l => l.Lag > threshold)
            .Select(l => $"{l.Applier} is {l.Lag} transaction(s) behind")
            .ToList();
        if (lagging.Count > 0)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                $"Applier lag exceeds HealthChecks:ApplierLagThreshold ({threshold}): {string.Join("; ", lagging)}."));
        }

        var degraded = engine.Appliers.Appliers
            .OfType<IApplierCondition>()
            .Select(static applier => applier.Degraded)
            .OfType<string>()
            .ToList();
        return Task.FromResult(degraded.Count > 0
            ? HealthCheckResult.Degraded(string.Join(" ", degraded))
            : HealthCheckResult.Healthy("Every applier is within its lag threshold."));
    }
}
