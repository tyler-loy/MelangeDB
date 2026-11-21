using System.Diagnostics;

namespace MelangeDB.LoadTest;

/// <summary>
/// Sums the write-lock split of one reducer across every shard in the serve process: how long
/// each transaction held the lock, and how much of that was post-commit — where subscription
/// fan-out runs, as a commit observer under the lock. Read from the <c>melange.slow_reducer</c>
/// event on the <c>melange.reducer</c> span, which carries the split; serve sets
/// Telemetry:SlowReducerMs to 0 so every transaction carries it. The ratio of the two sums is
/// read-side fan-out's reopening metric (road-to-0.3 phase 20).
/// </summary>
internal sealed class LockSplitListener : IDisposable
{
    private readonly string _reducer;
    private readonly ActivityListener _listener;
    private readonly Lock _gate = new();
    private long _transactions;
    private double _lockedMs;
    private double _postCommitMs;

    public LockSplitListener(string reducer)
    {
        _reducer = reducer;
        _listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == "MelangeDB",
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = OnStopped,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public long Transactions
    {
        get
        {
            lock (_gate)
                return _transactions;
        }
    }

    public double LockedMs
    {
        get
        {
            lock (_gate)
                return _lockedMs;
        }
    }

    public double PostCommitMs
    {
        get
        {
            lock (_gate)
                return _postCommitMs;
        }
    }

    private void OnStopped(Activity activity)
    {
        if (activity.OperationName != "melange.reducer"
            || activity.GetTagItem("melange.reducer.name") as string != _reducer)
            return;
        foreach (var evt in activity.Events)
        {
            if (evt.Name != "melange.slow_reducer")
                continue;
            double? locked = null, postCommit = null;
            foreach (var (key, value) in evt.Tags)
            {
                if (key == "melange.locked_ms" && value is double l)
                    locked = l;
                else if (key == "melange.post_commit_ms" && value is double p)
                    postCommit = p;
            }

            // An abort carries no post-commit; only committed transactions enter the ratio.
            if (locked is { } lockedMs && postCommit is { } postCommitMs)
            {
                lock (_gate)
                {
                    _transactions++;
                    _lockedMs += lockedMs;
                    _postCommitMs += postCommitMs;
                }
            }

            return;
        }
    }

    public void Dispose() => _listener.Dispose();
}
