using System.Net;
using System.Text;
using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// <c>col &lt;&gt; &lt;default&gt;</c> — the sparse-subset predicate (issue #122), raised by the
/// reference workload after a 160,000-row chunk table blew the row ceiling and the only expressible
/// filter turned out to be a boolean somebody had to add to the schema.
///
/// <para>The tests are built around that report's own argument: the workaround column selects the
/// identical row set, so it was never buying safety — only parser satisfaction. What has to be
/// asserted is therefore not "the predicate works" but three narrower things. That it agrees with
/// the workaround exactly. That it costs what an index scan costs, not what a filtered table scan
/// costs, because a predicate slower than the column it replaces would never be adopted. And that
/// the refusals stay narrow enough that "not the default" cannot quietly become "any
/// inequality".</para>
/// </summary>
public class NotDefaultPredicateTests
{
    /// <summary>
    /// The reporter's counterexample, executed: <c>EditCount &lt;&gt; 0</c> and the denormalised
    /// <c>IsEdited = true</c> select the same rows. Equality on an indexed boolean was always
    /// permitted and always had unbounded cardinality, so the two queries have the same ceiling and
    /// the same cost profile — the only thing that ever separated them was a column the schema did
    /// not need.
    /// </summary>
    [Fact]
    public async Task The_predicate_and_the_workaround_column_select_the_identical_row_set()
    {
        await using var host = await TransportTestHost.StartAsync();
        for (var i = 0L; i < 60; i++)
            host.Call("EditChunk", i, i % 5 == 0 ? 3u : 0u, 0);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var predicate = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE EditCount <> 0",
            cancellationToken: TestContext.Current.CancellationToken);
        var workaround = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE IsEdited = true",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(12, predicate.Count);
        Assert.Equal(
            workaround.Rows.Select(r => (long)r.Columns["Id"]!).Order().ToList(),
            predicate.Rows.Select(r => (long)r.Columns["Id"]!).Order().ToList());
    }

    /// <summary>
    /// The whole point of serving this as an index range rather than a filtered <c>Scan</c>. The
    /// cheap implementation — walk every row, keep the ones that are not the default — would have
    /// read all 600 rows to deliver 6, which is *worse* than the boolean column it replaces, and a
    /// predicate slower than the workaround is a predicate nobody adopts.
    ///
    /// <para>Sparsity is the case that matters and 1% is the shape the reporter had: 13,404 edited
    /// chunks out of 160,000. <c>RowsScanned</c> counts materialization, so an index walk registers
    /// zero — the same assertion the primary-key range test makes, for the same reason.</para>
    /// </summary>
    [Fact]
    public async Task Serving_it_is_an_index_walk_and_not_a_filtered_table_scan()
    {
        await using var host = await TransportTestHost.StartAsync();
        for (var i = 0L; i < 600; i++)
            host.Call("EditChunk", i, i % 100 == 0 ? 1u : 0u, 0);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var table = host.Engine.Schema.Get(typeof(EditedChunk));
        long RowsScanned() => host.Engine.HotStore.Statistics().Tables.Single(t => t.Table == table.Id).RowsScanned;

        var before = RowsScanned();
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE EditCount <> 0",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(6, subscription.Count);
        Assert.Equal(0, RowsScanned() - before);
    }

    /// <summary>
    /// Rows crossing the predicate in either direction surface as insert and delete, not as
    /// updates — the same contract equality and range already keep. The delta path evaluates the
    /// compiled bounds against the row's encoded column, so this is really asserting that the
    /// bounds the compiler built and the comparison the fan-out runs agree.
    /// </summary>
    [Fact]
    public async Task Rows_entering_and_leaving_the_set_arrive_as_inserts_and_deletes()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("EditChunk", 1L, 2u, 0);
        host.Call("EditChunk", 2L, 0u, 0);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE EditCount <> 0",
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, subscription.Count);

        var events = new List<string>();
        subscription.OnInsert += row => events.Add($"+{row.Columns["Id"]}");
        subscription.OnUpdate += (_, row) => events.Add($"~{row.Columns["Id"]}");
        subscription.OnDelete += row => events.Add($"-{row.Columns["Id"]}");

        host.Call("EditChunk", 2L, 1u, 0);     // 0 -> 1: enters the set.
        host.Call("EditChunk", 1L, 5u, 0);     // 2 -> 5: stays, so a plain update.
        var head = host.Call("EditChunk", 1L, 0u, 0);   // 5 -> 0: leaves the set.
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "deltas to drain");

        Assert.Equal(["+2", "~1", "-1"], events);
        Assert.Equal(2L, subscription.Rows.Single().Columns["Id"]);
    }

    /// <summary>
    /// A <c>bool</c> column's "not the default" is the true rows, and it compiles to a range whose
    /// bounds are both <c>true</c>. Worth asserting because it is the degenerate case of the
    /// bound-construction: one value wide, and off-by-one in either direction gives the empty set
    /// or the whole table rather than an error.
    /// </summary>
    [Fact]
    public async Task Not_default_on_a_bool_column_is_the_true_rows()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("EditChunk", 1L, 1u, 0);
        host.Call("EditChunk", 2L, 0u, 0);
        host.Call("EditChunk", 3L, 9u, 0);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE IsEdited <> false",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal([1L, 3L], subscription.Rows.Select(r => (long)r.Columns["Id"]!).Order().ToList());
    }

    /// <summary>
    /// The shape exists because a counter has no span, so it must not be span-checked. This is the
    /// exact query the reporter could not write: <c>BETWEEN 1 AND :hi</c> needs a clamp that is a
    /// lie the client tells the server, and refusing the clamp while accepting <c>&lt;&gt;</c> is
    /// the whole behavioural difference.
    /// </summary>
    [Fact]
    public async Task Not_default_is_not_span_checked_where_the_equivalent_range_would_be()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:MaxRangeSpan"] = "16",
        });
        host.Call("EditChunk", 1L, 4000u, 0);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var tooWide = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE EditCount BETWEEN 1 AND 4294967295",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.RangeTooWide, tooWide.Code);

        var accepted = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE EditCount <> 0",
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, accepted.Count);
    }

    /// <summary>
    /// The row ceiling is what bounds this shape, and it still does. "No span check" must not read
    /// as "no limit" — the argument for skipping the span was that the row ceiling already carries
    /// the weight, so it had better carry it.
    /// </summary>
    [Fact]
    public async Task The_row_ceiling_still_bounds_it()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:MaxRowsPerSubscription"] = "4",
        });
        for (var i = 0L; i < 10; i++)
            host.Call("EditChunk", i, 1u, 0);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var rejected = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE EditCount <> 0",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.TooManyRows, rejected.Code);
    }

    /// <summary>
    /// The two refusals that keep the shape small. An operand that is not the default would be an
    /// arbitrary inequality — no index affinity, a table scan wearing a predicate. A signed column
    /// has zero in the middle of its range, so "not zero" is two ranges rather than one, and this
    /// first cut says so by name instead of quietly serving half of it.
    /// </summary>
    [Fact]
    public async Task Only_the_default_and_only_the_kinds_whose_default_is_their_minimum()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var notTheDefault = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE EditCount <> 7",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnsupportedPredicate, notTheDefault.Code);
        Assert.Contains("default", notTheDefault.Message, StringComparison.Ordinal);

        var signed = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE Elevation <> 0",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnsupportedPredicate, signed.Code);
        Assert.Contains("Int32", signed.Message, StringComparison.Ordinal);

        // The indexed-column rule is not relaxed for this shape: an index is what makes it cheap,
        // so a column without one has no business carrying it.
        var unindexed = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM Skill WHERE Level <> 0",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnindexedColumn, unindexed.Code);
    }

    /// <summary>
    /// UInt64 is the kind that most needs its own case, and it had none. A SQL integer literal is
    /// parsed as <c>long</c>, so <c>&lt;&gt; 0</c> is the only literal form that can name the
    /// default of a column whose range runs past <c>long.MaxValue</c> — and the compiled range's
    /// upper bound is <c>ulong.MaxValue</c>, which no other kind exercises. A row sitting exactly
    /// on that bound is the one an off-by-one would silently drop.
    /// </summary>
    [Fact]
    public async Task Not_default_on_a_uint64_column_reaches_the_top_of_its_range()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("EditChunkVisited", 1L, 0u, 0, 0UL);
        host.Call("EditChunkVisited", 2L, 0u, 0, 1UL);
        host.Call("EditChunkVisited", 3L, 0u, 0, ulong.MaxValue);
        host.Call("EditChunkVisited", 4L, 0u, 0, (ulong)long.MaxValue + 1);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var set = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE Visits <> 0",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(
            [2L, 3L, 4L],
            set.Rows.Select(r => (long)r.Columns["Id"]!).Order().ToList());
    }

    /// <summary>
    /// The operand has to <em>be</em> the default, and "coerces to it" is a weaker question that
    /// would have widened the shape by accident. The write path's coercion rounds and reads a
    /// missing value as the default, so borrowing it here made <c>&lt;&gt; 0.5</c> and an unbound
    /// parameter both compile as not-default — naming a predicate no caller wrote, and hiding a
    /// mistake in the one shape that happens to want zero.
    /// </summary>
    [Fact]
    public async Task An_operand_that_merely_coerces_to_the_default_is_refused()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // 0.5 truncates to 0 on the write path; it names no value of an unsigned column here.
        var fractional = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE EditCount <> 0.5",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnsupportedPredicate, fractional.Code);

        // A null parameter is the mistake this refusal exists to surface, not a spelling of zero.
        var unbound = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE EditCount <> :p",
                new Dictionary<string, object?> { ["p"] = null },
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnsupportedPredicate, unbound.Code);

        // Exactly zero still compiles, however it is spelled — the rule is about the value.
        var bound = await client.SubscribeAsync(
            "SELECT * FROM EditedChunk WHERE EditCount <> :p",
            new Dictionary<string, object?> { ["p"] = 0 },
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(0, bound.Count);

        // And a bool column is named by its own default, never by 0 — the shape's headline
        // spelling is `<> 0` only where the default is 0.
        var boolByZero = await Assert.ThrowsAsync<MelangeSubscriptionException>(() =>
            client.SubscribeAsync(
                "SELECT * FROM EditedChunk WHERE IsEdited <> 0",
                cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnsupportedPredicate, boolByZero.Code);
    }

    /// <summary>
    /// Ad-hoc row queries take the same compiled path subscriptions do, so the shape works there.
    /// Aggregates do not: they run on the relational tier through a predicate discriminated by
    /// which operands it carries, and this first cut refuses them by name rather than letting one
    /// fall into the range branch and report a null operand nobody wrote.
    /// </summary>
    [Fact]
    public async Task Adhoc_rows_serve_it_and_aggregates_refuse_it_by_name()
    {
        // Owner mode, because an aggregate is owner-only: the refusal under test sits downstream
        // of that gate, and a policy-enforced host would answer 'owner_required' before reaching it.
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Sql:AdHocEnabled"] = "true",
            ["MelangeDb:Sql:AdHocMode"] = "Owner",
        });
        host.Call("EditChunk", 1L, 1u, 0);
        host.Call("EditChunk", 2L, 0u, 0);
        using var http = host.CreateHttp(TestTokens.For("alice", role: "melange-owner"));

        var rows = await http.PostAsync(
            "/melange/sql",
            Json("""{"query": "SELECT * FROM EditedChunk WHERE EditCount <> 0"}"""),
            TestContext.Current.CancellationToken);
        Assert.Equal(1, (await ReadJsonAsync(rows)).GetProperty("rows").GetArrayLength());

        var aggregate = await http.PostAsync(
            "/melange/sql",
            Json("""{"query": "SELECT COUNT(*) FROM WorldStat WHERE Value <> 0"}"""),
            TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.BadRequest, aggregate.StatusCode);
        Assert.Equal(
            MelangeErrorCodes.UnsupportedPredicate,
            (await ReadJsonAsync(aggregate)).GetProperty("error").GetString());
    }

    private static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");

    private static async Task<System.Text.Json.JsonElement> ReadJsonAsync(HttpResponseMessage response) =>
        System.Text.Json.JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement.Clone();
}
