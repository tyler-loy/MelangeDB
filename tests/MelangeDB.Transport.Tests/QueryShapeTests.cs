using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The four supported query shapes, projection semantics on the wire, the moving-range pattern,
/// and every subscription cost limit — each rejected before execution with an actionable error.
/// </summary>
public class QueryShapeTests
{
    [Fact]
    public async Task Whole_table_subscription_streams_inserts_updates_and_deletes()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, subscription.Count);

        var events = new List<string>();
        subscription.OnInsert += row => events.Add($"+{row.Columns["Id"]}");
        subscription.OnUpdate += (_, row) => events.Add($"~{row.Columns["Id"]}");
        subscription.OnDelete += row => events.Add($"-{row.Columns["Id"]}");

        host.Call("SetChunk", 2L, 1L, new byte[] { 2 });
        host.Call("SetChunk", 2L, 1L, new byte[] { 3 });
        var head = host.Call("DeleteChunk", 1L);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");

        Assert.Equal(["+2", "~2", "-1"], events);
        Assert.Equal(1, subscription.Count);
    }

    [Fact]
    public async Task Equality_subscription_on_an_indexed_column_tracks_rows_entering_and_leaving_the_predicate()
    {
        await using var host = await TransportTestHost.StartAsync();
        var alice = Identity.Hash("alice");
        var bob = Identity.Hash("bob");
        host.Reducers.Call("Spawn", alice, "Alice", 1);
        host.Reducers.Call("Spawn", bob, "Bob", 2);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var room1 = await client.SubscribeAsync(
            "SELECT * FROM PlayerState WHERE RoomId = :room",
            new Dictionary<string, object?> { ["room"] = 1 },
            TestContext.Current.CancellationToken);
        Assert.Equal(1, room1.Count);
        Assert.Equal("Alice", room1.Rows.Single().Columns["Name"]);

        var events = new List<string>();
        room1.OnInsert += row => events.Add($"+{row.Columns["Name"]}");
        room1.OnDelete += row => events.Add($"-{row.Columns["Name"]}");

        // Bob moves into room 1; Alice leaves for room 3. Updates that cross the predicate must
        // surface as insert and delete, not as updates.
        host.Reducers.Call("Spawn", bob, "Bob", 1);
        var head = host.Reducers.Call("Spawn", alice, "Alice", 3);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");

        Assert.Equal(["+Bob", "-Alice"], events);
        Assert.Equal("Bob", room1.Rows.Single().Columns["Name"]);
    }

    [Fact]
    public async Task Equality_subscription_on_the_primary_key_works()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 5L, 0L, new byte[] { 5 });
        host.Call("SetChunk", 6L, 0L, new byte[] { 6 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE Id = :id",
            new Dictionary<string, object?> { ["id"] = 5L },
            TestContext.Current.CancellationToken);
        Assert.Equal(1, subscription.Count);

        var head = host.Call("SetChunk", 5L, 0L, new byte[] { 55 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");
        Assert.Equal(new byte[] { 55 }, (byte[])subscription.Rows.Single().Columns["Data"]!);
    }

    [Fact]
    public async Task Projected_subscription_carries_partial_rows_and_stays_silent_for_non_projected_changes()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("AddSkill", 7L, "mining", 10L, 1);

        var updates = new List<TransactionUpdateFrame>();
        await using var client = host.CreateClient(o => o.FrameInspector = (frame, _) =>
        {
            if (frame is TransactionUpdateFrame update)
                updates.Add(update);
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var subscription = await client.SubscribeAsync(
            "SELECT PlayerNum, Name, TotalXp FROM Skill WHERE PlayerNum = :p",
            new Dictionary<string, object?> { ["p"] = 7L },
            TestContext.Current.CancellationToken);
        var initial = subscription.Rows.Single();
        Assert.Equal(["Name", "PlayerNum", "TotalXp"], initial.Columns.Keys.Order().ToList());

        // A non-projected column changes: the subscription must not emit — that is wasted
        // bandwidth on the hottest path. A projected change must still arrive as a partial row.
        host.Call("SetSkillLevel", 1UL, 9);
        var head = host.Call("SetSkillXp", 1UL, 999L);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");

        var mine = updates.Where(u => u.Updates.Any(g => g.SubscriptionId == subscription.Id)).ToList();
        var ops = mine.SelectMany(u => u.Updates.Where(g => g.SubscriptionId == subscription.Id)).SelectMany(g => g.Ops).ToList();
        var op = Assert.Single(ops);
        var columns = WireRowValues.ToColumns(subscription.Descriptor!, op.Row.Span, op.ColumnMask.Span);
        Assert.Equal(["Name", "PlayerNum", "TotalXp"], columns.Keys.Order().ToList());
        Assert.Equal(999L, columns["TotalXp"]);
        Assert.Equal(999L, subscription.Rows.Single().Columns["TotalXp"]);
    }

    [Fact]
    public async Task Moving_range_rescope_emits_inserts_for_newly_visible_and_deletes_for_newly_invisible_rows()
    {
        await using var host = await TransportTestHost.StartAsync();
        for (var i = 0L; i < 20; i++)
            host.Call("SetChunk", i, i, new byte[] { (byte)i });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 0L, ["hi"] = 7L },
            TestContext.Current.CancellationToken);
        Assert.Equal(8, subscription.Count);

        var inserted = new List<long>();
        var deleted = new List<long>();
        subscription.OnInsert += row => inserted.Add((long)row.Columns["Id"]!);
        subscription.OnDelete += row => deleted.Add((long)row.Columns["Id"]!);

        // The player "moves": same subscription, new window. The server diffs the scopes.
        await client.RescopeAsync(subscription, new Dictionary<string, object?> { ["lo"] = 5L, ["hi"] = 12L }, TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => subscription.Count == 8 && inserted.Count == 5 && deleted.Count == 5, "the rescope diff");

        Assert.Equal([8, 9, 10, 11, 12], inserted.Order().ToList());
        Assert.Equal([0, 1, 2, 3, 4], deleted.Order().ToList());

        // And the delta stream now follows the new window.
        var head = host.Call("SetChunk", 30L, 12L, new byte[] { 30 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");
        Assert.Contains(subscription.Rows, row => (long)row.Columns["Id"]! == 30);
    }

    [Fact]
    public async Task Private_and_unknown_tables_are_rejected_with_the_same_non_leaking_error()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("AddSecret", 1UL, "the-map-seed");

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var privateError = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("SELECT * FROM SecretTable", cancellationToken: TestContext.Current.CancellationToken));
        var unknownError = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("SELECT * FROM NoSuchTable", cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(MelangeErrorCodes.UnknownTable, privateError.Code);
        Assert.Equal(MelangeErrorCodes.UnknownTable, unknownError.Code);
        Assert.Equal(
            privateError.Message.Replace("SecretTable", "*"),
            unknownError.Message.Replace("NoSuchTable", "*"));
    }

    [Fact]
    public async Task Unbounded_subscription_to_a_mandatory_predicate_table_is_rejected_before_any_rows_are_read()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:RequirePredicateOn:0"] = "Chunk.X",
        });
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var unbounded = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.PredicateRequired, unbounded.Code);
        Assert.Contains("RequirePredicateOn", unbounded.Message);

        // A predicate on the wrong column does not satisfy the requirement either.
        var wrongColumn = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM Chunk WHERE Id = :id",
                new Dictionary<string, object?> { ["id"] = 1L },
                TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.PredicateRequired, wrongColumn.Code);

        var bounded = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 0L, ["hi"] = 10L },
            TestContext.Current.CancellationToken);
        Assert.Equal(1, bounded.Count);
    }

    [Fact]
    public async Task Range_subscription_exceeding_the_maximum_span_is_rejected_naming_the_limit()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var tooWide = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
                new Dictionary<string, object?> { ["lo"] = 0L, ["hi"] = 5000L },
                TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.RangeTooWide, tooWide.Code);
        Assert.Contains("1024", tooWide.Message);
        Assert.Contains("MaxRangeSpan", tooWide.Message);
    }

    [Fact]
    public async Task Row_and_byte_ceilings_reject_before_streaming()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:MaxRowsPerSubscription"] = "10",
            ["MelangeDb:Subscriptions:MaxBytesPerSubscription"] = "4096",
        });
        for (var i = 0L; i < 20; i++)
            host.Call("SetChunk", i, i, new byte[] { (byte)i });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var tooManyRows = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.TooManyRows, tooManyRows.Code);

        host.Call("SetChunk", 100L, 100L, new byte[3000]);
        host.Call("SetChunk", 101L, 101L, new byte[3000]);
        var tooManyBytes = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
                new Dictionary<string, object?> { ["lo"] = 100L, ["hi"] = 101L },
                TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.TooManyBytes, tooManyBytes.Code);
    }

    [Fact]
    public async Task Subscription_count_per_connection_is_capped()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:MaxPerConnection"] = "2",
        });
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        await client.SubscribeAsync("SELECT * FROM PlayerState", cancellationToken: TestContext.Current.CancellationToken);
        var third = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("SELECT * FROM Skill", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.TooManySubscriptions, third.Code);
    }

    [Fact]
    public async Task Unsubscribe_stops_the_delta_stream_without_touching_other_subscriptions()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var chunks = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        var players = await client.SubscribeAsync("SELECT * FROM PlayerState", cancellationToken: TestContext.Current.CancellationToken);
        await client.UnsubscribeAsync(chunks, TestContext.Current.CancellationToken);

        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        var head = host.Reducers.Call("Spawn", Identity.Hash("carol"), "Carol", 1);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");

        Assert.Equal(0, chunks.Count);
        Assert.Equal(1, players.Count);
    }

    [Fact]
    public async Task Predicates_on_unindexed_columns_and_unknown_columns_are_rejected()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var unindexed = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM PlayerState WHERE Name = :n",
                new Dictionary<string, object?> { ["n"] = "Alice" },
                TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnindexedColumn, unindexed.Code);

        var unknownColumn = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("SELECT Nope FROM PlayerState", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnknownColumn, unknownColumn.Code);

        var garbage = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync("DELETE FROM PlayerState", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.ParseError, garbage.Code);
    }

    /// <summary>
    /// A primary-key range must read only the rows inside it — never the rows it passes to get
    /// there.
    ///
    /// <para>The moving-range tests above all range over <c>X</c>, which is <c>[Index]</c>ed, so
    /// they take <c>ScanIndexRange</c> and the primary-key range path went uncovered on cost. It
    /// filtered a full <c>Scan</c>, so a window near the end of a table materialized every row
    /// below it and discarded them: cost proportional to the window's distance from row zero, on
    /// exactly the subscription shape terrain streaming uses. Rows below the low bound cannot
    /// match an ordered key walk, so reading them is never work — this asserts the walk stays a
    /// walk.</para>
    /// </summary>
    [Fact]
    public async Task Primary_key_range_reads_only_the_rows_inside_the_range()
    {
        await using var host = await TransportTestHost.StartAsync();
        for (var i = 0L; i < 500; i++)
            host.Call("SetChunk", i, i, new byte[64]);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var chunk = host.Engine.Schema.Get(typeof(Chunk));
        long RowsScanned() => host.Engine.HotStore.Statistics().Tables.Single(t => t.Table == chunk.Id).RowsScanned;

        // The window sits at the far end deliberately: under the old shape this cost 490 row
        // reads to deliver 10, and a window at the near end would have hidden that.
        var before = RowsScanned();
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE Id BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 490L, ["hi"] = 499L },
            TestContext.Current.CancellationToken);
        Assert.Equal(10, subscription.Count);
        Assert.Equal(0, RowsScanned() - before);

        // And the re-scope path, which walks the table twice — once for the old window, once for
        // the new — so it is the one a player crossing chunk boundaries pays over and over.
        before = RowsScanned();
        await client.RescopeAsync(
            subscription,
            new Dictionary<string, object?> { ["lo"] = 480L, ["hi"] = 489L },
            TestContext.Current.CancellationToken);
        // Not Count == 10: the window is the same size before and after, so a count check passes
        // instantly against the *old* window and asserts nothing. Wait for the new bound.
        await TransportTestHost.WaitUntilAsync(
            () => subscription.Count == 10 && subscription.Rows.Min(r => (long)r.Columns["Id"]!) == 480L,
            "the rescope diff");
        Assert.Equal(0, RowsScanned() - before);
    }
}
