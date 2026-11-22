using System.Globalization;
using System.Text.Json.Nodes;

namespace MelangeDB.Conformance.Harness;

/// <summary>
/// The scenarios that need time, failure, or a second server: the anchor boundary under concurrent
/// writes, resume against the same and a different log, backpressure, reauthentication, and the
/// heartbeat.
/// </summary>
public static partial class ConformanceSuite
{
    /// <summary>Small enough that one large delta trips it, which is how the suite forces overflow_resync.</summary>
    private const int BackpressureBufferBytes = 16 * 1024;

    /// <summary>
    /// A many-chunk initial set while the suite commits into the same group as fast as it can, so
    /// deltas overtake the set's chunks. Every event must be consistent with the one before it, and
    /// the cache must end exactly where the server is: no commit lost under the set, none twice.
    /// </summary>
    private static async Task AnchorBoundaryAsync(SuiteContext context)
    {
        const int group = 500;
        const long first = 50_000;
        const int seeded = 600;
        var truth = new Dictionary<long, JsonObject>();
        for (var id = first; id < first + seeded; id++)
        {
            context.Server.Call("PutSample", id, (int)(id % 3), group);
            truth[id] = Truth.Sample(id, (int)(id % 3), group);
        }

        await context.ConnectAsync().ConfigureAwait(false);
        var subscribing = context.Adapter.MustAsync("subscribe", new JsonObject
        {
            ["table"] = "Sample",
            ["query"] = "SELECT * FROM Sample WHERE Group = :g",
            ["parameters"] = Parameters(("g", Tagged.Int64(group))),
        });

        ulong last = 0;
        var inserted = first + seeded;
        var changed = first;
        var deleted = first + seeded - 1;
        for (var write = 0; (!subscribing.IsCompleted || write < 30) && write < 300; write++)
        {
            switch (write % 3)
            {
                case 0:
                    last = context.Server.Call("PutSample", inserted, 1, group);
                    truth[inserted] = Truth.Sample(inserted, 1, group);
                    inserted++;
                    break;
                case 1:
                    last = context.Server.Call("ChangeSample", changed, 2);
                    truth[changed] = Truth.Sample(changed, 2, group);
                    changed++;
                    break;
                default:
                    last = context.Server.Call("DeleteSample", deleted);
                    truth.Remove(deleted);
                    deleted--;
                    break;
            }
        }

        await subscribing.ConfigureAwait(false);
        await context.Adapter.MustAsync("sync", new JsonObject { ["lsn"] = CorpusJson.U64(last) }).ConfigureAwait(false);
        var replayed = ReplayEvents(context.Adapter.TakeEvents(), "Sample");
        CheckRows(truth.Values, replayed.Values, "The rows the client's events describe");
        await CheckCacheAsync(context, "Sample", truth.Values, static _ => true).ConfigureAwait(false);
    }

    /// <summary>The socket drops; commits land meanwhile; the reconnect resumes and replays exactly them.</summary>
    private static async Task ResumeAcceptedAsync(SuiteContext context)
    {
        const int group = 600;
        context.Server.Call("PutSample", 60_001L, 0, group);
        context.Server.Call("PutSample", 60_002L, 0, group);
        await context.ConnectAsync().ConfigureAwait(false);
        await context.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group = :g", Parameters(("g", Tagged.Int64(group)))).ConfigureAwait(false);
        context.Adapter.TakeEvents();

        await DropAsync(context).ConfigureAwait(false);
        context.Server.Call("PutSample", 60_003L, 1, group);
        context.Server.Call("ChangeSample", 60_001L, 2);
        var last = context.Server.Call("DeleteSample", 60_002L);

        var reconnect = await context.Adapter.MustAsync("reconnect").ConfigureAwait(false);
        Check(reconnect["resumed"]?.GetValue<bool>() == true, "The epoch is unchanged and the gap is inside the retention window, so the reconnect must resume, not resync.");
        await context.Adapter.MustAsync("sync", new JsonObject { ["lsn"] = CorpusJson.U64(last) }).ConfigureAwait(false);
        CheckEvents(
            context.Adapter.TakeEvents(),
            [
                Insert("Sample", Truth.Sample(60_003, 1, group)),
                Update("Sample", Truth.Sample(60_001, 0, group), Truth.Sample(60_001, 2, group)),
                Delete("Sample", Truth.Sample(60_002, 0, group)),
            ],
            ordered: true,
            "A resume must replay exactly the commits the client missed, in order");
        await CheckCacheAsync(context, "Sample", [Truth.Sample(60_001, 2, group), Truth.Sample(60_003, 1, group)], static row => (int)row["Group"]! == group).ConfigureAwait(false);
    }

    private static async Task ReauthenticateAsync(SuiteContext context)
    {
        await context.ConnectAsync().ConfigureAwait(false);
        await context.Adapter.MustAsync("reauthenticate", new JsonObject { ["token"] = context.Server.Token }).ConfigureAwait(false);

        var switched = await context.Adapter.RequestAsync("reauthenticate", new JsonObject { ["token"] = context.Server.TokenFor("someone-else") }).ConfigureAwait(false);
        Check(switched["ok"]?.GetValue<bool>() == false, "Reauthenticating as a different identity must fail.");
        var told = await context.Adapter.WaitForEventsAsync(
            static events => events.Any(static e => (string?)e["event"] == "error" && (string?)e["code"] == "identity_changed"),
            EventWait).ConfigureAwait(false);
        Check(told, "The server's identity_changed error must reach the application.");
    }

    /// <summary>
    /// The server restarts over a fresh log: a new epoch, so the resume is rejected. The client must
    /// re-establish and reconcile by diff — a row that survived the restart unchanged in identity
    /// is an update, not a delete and an insert.
    /// </summary>
    private static async Task ResumeNewEpochAsync(SuiteContext context)
    {
        const int group = 700;
        await using var server = await ConformanceServer.StartAsync().ConfigureAwait(false);
        var own = context with { Server = server };
        server.Call("PutSample", 70_001L, 0, group);
        server.Call("PutSample", 70_002L, 0, group);
        await own.ConnectAsync().ConfigureAwait(false);
        await own.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group = :g", Parameters(("g", Tagged.Int64(group)))).ConfigureAwait(false);
        context.Adapter.TakeEvents();

        await DropAsync(own).ConfigureAwait(false);
        await server.RestartAsync(freshLog: true).ConfigureAwait(false);
        server.Call("PutSample", 70_002L, 2, group);
        var last = server.Call("PutSample", 70_003L, 1, group);

        var reconnect = await context.Adapter.MustAsync("reconnect").ConfigureAwait(false);
        Check(reconnect["resumed"]?.GetValue<bool>() == false, "The log is a new incarnation, so the reconnect must resync, not resume.");
        await context.Adapter.MustAsync("sync", new JsonObject { ["lsn"] = CorpusJson.U64(last) }).ConfigureAwait(false);
        CheckEvents(
            context.Adapter.TakeEvents(),
            [
                Delete("Sample", Truth.Sample(70_001, 0, group)),
                Update("Sample", Truth.Sample(70_002, 0, group), Truth.Sample(70_002, 2, group)),
                Insert("Sample", Truth.Sample(70_003, 1, group)),
            ],
            ordered: false,
            "A full resync must reconcile the cache by diff, not flush and reload it");
        await CheckCacheAsync(own, "Sample", [Truth.Sample(70_002, 2, group), Truth.Sample(70_003, 1, group)], static _ => true).ConfigureAwait(false);
    }

    /// <summary>
    /// One delta larger than <c>Subscriptions:MaxBufferedBytes</c> makes the server drop the
    /// connection's subscriptions and send overflow_resync. The client must re-establish them on its
    /// own: the cache converges and the subscription is live again.
    /// </summary>
    private static async Task OverflowResyncAsync(SuiteContext context)
    {
        const int group = 800;
        await using var server = await ConformanceServer.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:MaxBufferedBytes"] = BackpressureBufferBytes.ToString(CultureInfo.InvariantCulture),
        }).ConfigureAwait(false);
        var own = context with { Server = server };
        await own.ConnectAsync().ConfigureAwait(false);
        await own.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group = :g", Parameters(("g", Tagged.Int64(group)))).ConfigureAwait(false);
        await own.WriteAndSyncAsync("PutSample", 80_001L, 0, group).ConfigureAwait(false);
        context.Adapter.TakeEvents();

        var blob = SampleRows.Build(80_002, 0, group);
        blob.Blob = [.. Enumerable.Range(0, BackpressureBufferBytes * 4).Select(static i => (byte)(i * 31))];
        var big = server.Call("PutBlob", 80_002L, group, BackpressureBufferBytes * 4);
        var synced = await context.Adapter.RequestAsync("sync", new JsonObject { ["lsn"] = CorpusJson.U64(big) }).ConfigureAwait(false);
        Check(
            synced["ok"]?.GetValue<bool>() == true,
            "After overflow_resync the server holds none of the connection's subscriptions; the client must re-subscribe on its own, or its cache stops changing. "
            + $"The client's cursor never reached the oversized commit: {synced["error"]?.ToJsonString()}");

        var events = context.Adapter.TakeEvents();
        Check(events.Any(static e => (string?)e["event"] == "error" && (string?)e["code"] == "overflow_resync"), "The overflow_resync error must reach the application.");
        CheckEvents([.. events.Where(static e => (string?)e["event"] != "error")], [Insert("Sample", Truth.Sample(blob))], ordered: true, "Re-establishing must reconcile by diff: the unchanged row raises nothing");

        await own.WriteAndSyncAsync("PutSample", 80_003L, 1, group).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [Insert("Sample", Truth.Sample(80_003, 1, group))], ordered: true, "The re-established subscription must be live");
        await CheckCacheAsync(own, "Sample", [Truth.Sample(80_001, 0, group), Truth.Sample(blob), Truth.Sample(80_003, 1, group)], static _ => true).ConfigureAwait(false);
    }

    /// <summary>
    /// The server's clock moves through more than its heartbeat timeout in interval steps. A
    /// client that answers each Ping survives; one that does not is aborted by the third.
    /// </summary>
    private static async Task HeartbeatAsync(SuiteContext context)
    {
        await context.ConnectAsync().ConfigureAwait(false);
        for (var tick = 0; tick < 5; tick++)
        {
            context.Server.Clock.Advance(TimeSpan.FromMilliseconds(ConformanceServer.HeartbeatIntervalMs));

            // The Pong travels in real time; give it a moment before the clock moves again.
            await Task.Delay(400).ConfigureAwait(false);
        }

        var disconnected = context.Adapter.TakeEvents().Any(static e => (string?)e["event"] == "disconnected");
        Check(!disconnected, "The server disconnected a client through five heartbeat intervals: it is not answering Ping with Pong.");
        await context.Adapter.MustAsync("call", new JsonObject
        {
            ["reducer"] = "PutSample",
            ["arguments"] = new JsonArray(Tagged.Int64(90_001), Tagged.Int64(0), Tagged.Int64(900)),
        }).ConfigureAwait(false);
    }

    private static async Task DropAsync(SuiteContext context)
    {
        await context.Adapter.MustAsync("drop").ConfigureAwait(false);
        var dropped = await context.Adapter.WaitForEventsAsync(static events => events.Any(static e => (string?)e["event"] == "disconnected"), EventWait).ConfigureAwait(false);
        Check(dropped, "After 'drop' the client must report the disconnect.");
        context.Adapter.TakeEvents();
    }

    /// <summary>
    /// Applies a table's events in order to an empty map, failing on any event that contradicts
    /// the state before it: an insert of a present row, an update or delete of an absent one, or an
    /// update or delete whose old row is not the row held.
    /// </summary>
    private static Dictionary<string, JsonObject> ReplayEvents(List<JsonObject> events, string table)
    {
        var rows = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        foreach (var e in events.Where(e => (string?)e["table"] == table))
        {
            var kind = (string)e["event"]!;
            var row = (e["row"] ?? e["new"])!.AsObject();
            var key = (string)row["Id"]!;
            switch (kind)
            {
                case "insert":
                    Check(!rows.ContainsKey(key), $"Insert for row {key}, which the client already presented: a commit applied twice across the anchor boundary.");
                    rows[key] = row;
                    break;
                case "update":
                    Check(rows.TryGetValue(key, out var held) && Canonical(held) == Canonical(e["old"]), $"Update for row {key} whose old value is not the row the client presented.");
                    rows[key] = row;
                    break;
                case "delete":
                    Check(rows.TryGetValue(key, out var gone) && Canonical(gone) == Canonical(row), $"Delete for row {key}, which the client never presented as that value.");
                    rows.Remove(key);
                    break;
            }
        }

        return rows;
    }

    private static void CheckRows(IEnumerable<JsonObject> expected, IEnumerable<JsonObject> actual, string what)
    {
        var wanted = expected.Select(Canonical).Order(StringComparer.Ordinal).ToList();
        var held = actual.Select(Canonical).Order(StringComparer.Ordinal).ToList();
        if (wanted.SequenceEqual(held))
            return;
        var missing = wanted.Except(held).Take(3).ToList();
        var extra = held.Except(wanted).Take(3).ToList();
        throw new ConformanceFailure(
            $"{what} differ from the server's: {wanted.Count} expected, {held.Count} held.\n  missing (first 3): [{string.Join(", ", missing)}]\n  extra (first 3):   [{string.Join(", ", extra)}]");
    }
}
