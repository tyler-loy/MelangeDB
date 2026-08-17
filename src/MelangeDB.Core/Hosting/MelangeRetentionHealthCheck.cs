using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// The <c>melange-retention</c> health check: unhealthy when more records are pinned above the
/// truncation floor than <c>HealthChecks:RetentionPinnedThreshold</c> allows, naming the floor
/// that is holding them. The applier check already covers one holder; this one exists because an
/// applier is only one of seven, and the other six — live event subscribers, backup pins, the two
/// cluster handoff markers, the cluster events cursor, the Resume window — had nothing at all. The
/// symptom they share is a disk filling with a log that will not compact.
/// <para>
/// Healthy trivially when nothing is configured to truncate, and while no truncation decision has
/// been made yet: the floors are a reading taken at truncation time (see
/// <see cref="TruncationFloorReport"/>), and there is nothing honest to say before the first one.
/// </para>
/// </summary>
internal sealed class MelangeRetentionHealthCheck : IHealthCheck
{
    private readonly IServiceProvider _provider;
    private readonly MelangeDbRuntimeState _state;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    public MelangeRetentionHealthCheck(IServiceProvider provider, MelangeDbRuntimeState state, IOptionsMonitor<MelangeDbOptions> options)
    {
        _provider = provider;
        _state = state;
        _options = options;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_state.Started)
            return Task.FromResult(HealthCheckResult.Unhealthy("MelangeDB has not started; the commit log is not open."));

        var options = _options.CurrentValue;
        if (!options.Snapshots.Enabled || !options.Snapshots.TruncateLog)
        {
            return Task.FromResult(HealthCheckResult.Healthy(
                "Log truncation is not configured; the log grows without bound by design."));
        }

        var engine = (MelangeEngine)_provider.GetService(typeof(MelangeEngine))!;
        if (engine.TruncationFloors is not { } report)
        {
            return Task.FromResult(HealthCheckResult.Healthy(
                "No truncation has been decided yet; the first snapshot evaluates the floors."));
        }

        // The head is live, the floors are the last reading: the distance grows while a stuck
        // holder stands still, which is the whole point of measuring it this way.
        var head = engine.Log.HeadLsn;
        var pinned = head > report.EffectiveFloor ? head - report.EffectiveFloor : 0;
        var threshold = options.HealthChecks.RetentionPinnedThreshold;
        if (pinned <= (ulong)Math.Max(0, threshold))
        {
            return Task.FromResult(HealthCheckResult.Healthy(
                $"{pinned} record(s) pinned above the truncation floor; '{report.Governing.Name}' governs at LSN {report.Governing.Lsn}."));
        }

        return Task.FromResult(HealthCheckResult.Unhealthy(
            $"{pinned} record(s) are pinned above the truncation floor, over " +
            $"HealthChecks:RetentionPinnedThreshold ({threshold}). The floor is held by " +
            $"'{report.Governing.Name}' at LSN {report.Governing.Lsn}, decided behind the snapshot at " +
            $"LSN {report.SnapshotLsn} with the head now at LSN {head}. Everything older than that " +
            "floor stays on disk until that holder checkpoints past it."));
    }
}
