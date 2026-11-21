using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Phase 30 step 2e: promotion. A follower hands its projection to the engine that takes over the
/// log, which replays only the records after the follower's cursor; a projection that does not
/// belong to the log is refused and the engine recovers cold, as it always has.
/// </summary>
public class WarmStartTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly SchemaRegistry _registry = EngineHarness.GeneratedRegistry(typeof(Player), typeof(InventoryItem));

    public void Dispose()
    {
        foreach (var root in _roots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
                // A handle Windows has not released yet; the temp reaper gets it.
            }
        }
    }

    private string TempDir()
    {
        var dir = Directory.CreateTempSubdirectory("melange-warm-").FullName;
        _roots.Add(dir);
        return dir;
    }

    private MelangeDbOptions Options(string logDirectory, long? term, int retentionSeconds = 3600) => new()
    {
        CommitLog = { Path = logDirectory, Term = term },
        HotStore = { Path = Path.Combine(TempDir(), "hot") },
        Resume = { RetentionWindowSeconds = retentionSeconds },
    };

    private MelangeEngine Owner(string logDirectory, long? term, WarmStart? warm = null, int retentionSeconds = 3600) =>
        new(Options(logDirectory, term, retentionSeconds), _registry, null, null, null, 0, null, warm is null ? null : () => warm);

    private LogFollower Follower(string logDirectory) =>
        new(new MelangeDbOptions { CommitLog = { Path = logDirectory }, HotStore = { Path = Path.Combine(TempDir(), "follower-hot") } }, _registry, originator: 0);

    private static void Spawn(MelangeEngine engine, string name) =>
        engine.Invoke("Spawn", EngineHarness.Caller, ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash(name), Name = name, RoomId = 1 });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash(name), ItemName = "torch", Quantity = 1 });
        });

    private static void CatchUp(LogFollower follower, ulong ceiling)
    {
        follower.Ceiling = ceiling;
        while (follower.Poll() > 0)
        {
        }
    }

    private static List<string> Dump(SchemaRegistry schema, IHotStore store) =>
        [.. schema.Tables.SelectMany(table => store.Scan(table.Id).Select(pair => $"{table.Name}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}")).Order()];

    [Fact]
    public void A_new_owner_starts_from_the_followers_projection_and_replays_only_the_tail()
    {
        var dir = TempDir();
        using var follower = Follower(dir);
        List<string> expected;
        ulong lastItem;
        using (var first = Owner(dir, term: 1))
        {
            for (var i = 0; i < 3; i++)
                Spawn(first, $"p{i}");

            // Two records the follower will not have seen, as a ceiling a heartbeat old would leave
            // it. They mint no item ids, so only the follower's sequences know the next one.
            for (var i = 3; i < 5; i++)
            {
                var name = $"p{i}";
                first.Invoke("Join", EngineHarness.Caller, ctx => ctx.Db.Insert(new Player { Id = Identity.Hash(name), Name = name, RoomId = 2 }));
            }

            CatchUp(follower, 3);
            expected = Dump(_registry, first.HotStore);
            lastItem = first.CommittedView.Scan<InventoryItem>().Max(static item => item.Id);
        }

        var warm = follower.Release();
        Assert.NotNull(warm);
        Assert.Equal(3UL, warm.AppliedLsn);

        using (var promoted = Owner(dir, term: 2, warm))
        {
            Assert.Equal(3UL, promoted.WarmStartedFrom);
            Assert.Same(warm.Store, promoted.HotStore);
            Assert.Equal(expected, Dump(_registry, promoted.HotStore));

            // The sequences continue past every id the previous owner made durable.
            Spawn(promoted, "after");
            var minted = promoted.CommittedView.Scan<InventoryItem>().Single(static item => item.Owner == Identity.Hash("after"));
            Assert.Equal(lastItem + 1, minted.Id);
            expected = Dump(_registry, promoted.HotStore);
        }

        // And the log it now owns recovers cold to the same state.
        using var reopened = Owner(dir, term: 2);
        Assert.Null(reopened.WarmStartedFrom);
        Assert.Equal(expected, Dump(_registry, reopened.HotStore));
    }

    [Fact]
    public void A_projection_of_another_log_is_refused_and_the_engine_recovers_cold()
    {
        var elsewhere = TempDir();
        using var follower = Follower(elsewhere);
        using (var other = Owner(elsewhere, term: 1))
        {
            Spawn(other, "other");
            CatchUp(follower, other.Log.DurableLsn);
        }

        var dir = TempDir();
        List<string> expected;
        using (var first = Owner(dir, term: 1))
        {
            Spawn(first, "mine");
            Spawn(first, "also-mine");
            expected = Dump(_registry, first.HotStore);
        }

        var warm = follower.Release()!;
        using var engine = Owner(dir, term: 2, warm);
        Assert.Null(engine.WarmStartedFrom);
        Assert.NotSame(warm.Store, engine.HotStore);
        Assert.Equal(expected, Dump(_registry, engine.HotStore));
    }

    [Fact]
    public void A_follower_with_nothing_trustworthy_hands_over_nothing()
    {
        // Never bootstrapped: the owner had not booted.
        using (var early = Follower(TempDir()))
            Assert.Null(early.Release());

        // Its place in the log was truncated away since its last poll.
        var dir = TempDir();
        using var follower = Follower(dir);
        using var owner = Owner(dir, term: 1, retentionSeconds: 0);
        Spawn(owner, "a");
        CatchUp(follower, owner.Log.DurableLsn);
        for (var i = 0; i < 4; i++)
            Spawn(owner, $"b{i}");
        Assert.NotNull(owner.TakeSnapshot());
        Assert.True(owner.Log.BaseLsn > follower.AppliedLsn);
        Assert.Null(follower.Release());
        Assert.Throws<ObjectDisposedException>(() => follower.Poll());
    }

    [Fact]
    public void An_open_that_cannot_take_the_log_leaves_the_replica_for_the_retry()
    {
        var dir = TempDir();
        using var follower = Follower(dir);
        var asked = 0;
        WarmStart? Take()
        {
            asked++;
            return follower.Release();
        }

        using (var first = Owner(dir, term: 1))
        {
            Spawn(first, "a");
            CatchUp(follower, first.Log.DurableLsn);

            // The previous owner still holds the log, as a dying process does for a moment.
            Assert.ThrowsAny<Exception>(() => new MelangeEngine(Options(dir, term: 2), _registry, null, null, null, 0, null, Take));
            Assert.Equal(0, asked);
            Assert.Equal(1UL, follower.AppliedLsn); // Still following, not disposed.
        }

        using var promoted = new MelangeEngine(Options(dir, term: 2), _registry, null, null, null, 0, null, Take);
        Assert.Equal(1, asked);
        Assert.Equal(1UL, promoted.WarmStartedFrom);
    }
}
