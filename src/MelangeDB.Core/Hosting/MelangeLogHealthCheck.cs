using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MelangeDB.Core;

/// <summary>
/// The <c>melange-log</c> health check: unhealthy when the commit log cannot accept appends —
/// before startup completes, or after an append failure (disk full being the realistic case)
/// poisoned the log. A poisoned log rejects every append until restart, so surfacing it through
/// the host's health endpoint is what turns a silent write outage into a rotated pod.
/// </summary>
internal sealed class MelangeLogHealthCheck : IHealthCheck
{
    private readonly IServiceProvider _provider;
    private readonly MelangeDbRuntimeState _state;

    public MelangeLogHealthCheck(IServiceProvider provider, MelangeDbRuntimeState state)
    {
        _provider = provider;
        _state = state;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_state.Started)
            return Task.FromResult(HealthCheckResult.Unhealthy("MelangeDB has not started; the commit log is not open."));

        var engine = (MelangeEngine)_provider.GetService(typeof(MelangeEngine))!;
        if (engine.LogFailure is { } failure)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "The commit log is poisoned and rejects appends until restart (unwritable or out of disk).",
                failure));
        }

        return Task.FromResult(HealthCheckResult.Healthy($"Commit log writable at LSN {engine.Log.HeadLsn}."));
    }
}
