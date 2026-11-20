namespace MelangeDB.LoadTest;

/// <summary>
/// What a subcommand fixes about the cluster it serves, beyond the shared flags: the heartbeat
/// and failure-timeout defaults (overridable by flag) and any extra MelangeDB settings.
/// </summary>
internal sealed record ServeTuning(int HeartbeatMs, int FailureTimeoutMs, IReadOnlyDictionary<string, string?> Settings)
{
    /// <summary>serve and all: the timing the capacity ladder was recorded with.</summary>
    public static ServeTuning Serve { get; } = new(250, 15_000, new Dictionary<string, string?>());
}
