using System.Text.Json.Nodes;

namespace MelangeDB.Conformance.Harness;

/// <summary>One scenario's outcome. <see cref="Obligations"/> names the CLIENT-CONFORMANCE.md rules it checks.</summary>
public sealed record ScenarioResult(string Name, string Obligations, bool Passed, string? Failure, TimeSpan Elapsed);

/// <summary>
/// The live conformance suite: every scenario drives the client under test through its adapter
/// against a real <see cref="ConformanceServer"/>, while the suite writes server-side and knows
/// exactly what the client must end up holding and which events it must have raised.
/// </summary>
public static partial class ConformanceSuite
{
    private static readonly TimeSpan EventWait = TimeSpan.FromSeconds(10);

    /// <summary>A scenario: its name, the obligations it checks, and its body.</summary>
    private sealed record Scenario(string Name, string Obligations, Func<SuiteContext, Task> Run);

    private static readonly Scenario[] Scenarios =
    [
        new("corpus-decode", "C2.1, C2.2, C4.1-C4.3, C7.1, C9.4", CorpusDecodeAsync),
        new("encoding-vectors", "C4.2, C5.2, C9.2", EncodingVectorsAsync),
        new("handshake", "C3.1, C3.3, C16.1", HandshakeAsync),
        new("initial-set", "C4.2, C6.2, C8.1", InitialSetAsync),
        new("calls", "C9.1-C9.4", CallsAsync),
        new("merged-cache", "C8.1, C6.5", MergedCacheAsync),
        new("rescope", "C7.2, C8.1", RescopeAsync),
        new("column-masks", "C4.3, C4.4", ColumnMasksAsync),
        new("anchor-boundary", "C6.2, C6.3, C7.3", AnchorBoundaryAsync),
        new("resume-accepted", "C10.1-C10.3", ResumeAcceptedAsync),
        new("reauthenticate", "C13.1, C13.2", ReauthenticateAsync),
        new("resume-new-epoch", "C10.2, C10.4, C8.2", ResumeNewEpochAsync),
        new("overflow-resync", "C11.1, C8.2", OverflowResyncAsync),

        // Last: it moves the shared server's clock, which every later resume window would feel.
        new("heartbeat", "C12.1", HeartbeatAsync),
    ];

    /// <summary>Runs every scenario whose name passes <paramref name="filter"/> and reports each.</summary>
    public static async Task<IReadOnlyList<ScenarioResult>> RunAsync(
        AdapterChannel adapter,
        JsonObject corpus,
        Func<string, bool>? filter = null,
        CancellationToken cancellationToken = default)
    {
        await adapter.MustAsync("hello").ConfigureAwait(false);
        await using var server = await ConformanceServer.StartAsync(null, cancellationToken).ConfigureAwait(false);
        var results = new List<ScenarioResult>();
        foreach (var scenario in Scenarios)
        {
            if (filter is not null && !filter(scenario.Name))
                continue;
            var started = DateTime.UtcNow;
            try
            {
                await adapter.MustAsync("close").ConfigureAwait(false);
                adapter.TakeEvents();
                await scenario.Run(new SuiteContext(adapter, server, corpus)).ConfigureAwait(false);
                results.Add(new ScenarioResult(scenario.Name, scenario.Obligations, true, null, DateTime.UtcNow - started));
            }
            catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                var failure = exception is ConformanceFailure ? exception.Message : $"{exception.GetType().Name}: {exception.Message}{Environment.NewLine}{exception.StackTrace}";
                results.Add(new ScenarioResult(scenario.Name, scenario.Obligations, false, failure, DateTime.UtcNow - started));
            }
        }

        await adapter.RequestAsync("close").ConfigureAwait(false);
        return results;
    }

    private static async Task CorpusDecodeAsync(SuiteContext context)
    {
        var sessions = context.Corpus["sessions"]!.AsArray().ToDictionary(static s => (string)s!["name"]!, static s => s!.AsObject());
        foreach (var session in sessions.Values)
        {
            var frames = new List<JsonObject>();
            if (session["descriptorsFrom"] is { } from)
                frames.AddRange(Frames(sessions[(string)from!]));
            var inherited = frames.Count;
            frames.AddRange(Frames(session));

            var response = await context.Adapter.MustAsync("decodeFrames", new JsonObject
            {
                ["frames"] = new JsonArray([.. frames.Select(static f => (JsonNode)JsonValue.Create((string)f["hex"]!))]),
            }).ConfigureAwait(false);
            var decoded = response["decoded"]!.AsArray();
            Check(decoded.Count == frames.Count, $"decodeFrames returned {decoded.Count} decodes for {frames.Count} frames.");
            for (var i = inherited; i < frames.Count; i++)
                CheckSame(frames[i]["expect"], decoded[i], $"Session '{session["name"]}', frame {i - inherited} ({frames[i]["expect"]!["type"]})");
        }
    }

    private static async Task EncodingVectorsAsync(SuiteContext context)
    {
        foreach (var vector in context.Corpus["columnVectors"]!.AsArray())
        {
            var response = await context.Adapter.MustAsync("decodeColumn", new JsonObject { ["kind"] = vector!["kind"]!.DeepClone(), ["bytes"] = vector["bytes"]!.DeepClone() }).ConfigureAwait(false);
            CheckSame(vector["value"], response["value"], $"Column vector {vector["kindName"]} {vector["bytes"]}");
            Check((int)response["consumed"]! * 2 == ((string)vector["bytes"]!).Length, $"Column vector {vector["kindName"]} {vector["bytes"]}: consumed {response["consumed"]} bytes.");
        }

        foreach (var vector in context.Corpus["keyVectors"]!.AsArray())
        {
            var response = await context.Adapter.MustAsync("encodeKey", new JsonObject { ["kind"] = vector!["kind"]!.DeepClone(), ["value"] = vector["value"]?.DeepClone() }).ConfigureAwait(false);
            CheckSame(vector["key"], response["key"], $"Key vector {vector["kindName"]} {vector["value"]?.ToJsonString()}");
        }

        foreach (var vector in context.Corpus["argumentVectors"]!.AsArray())
        {
            var response = await context.Adapter.MustAsync("encodeArguments", new JsonObject { ["arguments"] = vector!["arguments"]!.DeepClone() }).ConfigureAwait(false);
            CheckSame(vector["bytes"], response["bytes"], $"Argument vector '{vector["about"]}'");
        }
    }

    private static async Task HandshakeAsync(SuiteContext context)
    {
        var welcome = await context.ConnectAsync().ConfigureAwait(false);
        CheckSame(ConformanceServer.Caller.ToString(), welcome["identity"], "The identity the client reports");
        Check((int)welcome["protocolVersion"]! == Protocol.MessagePackFrameSerializer.ProtocolVersion, $"Negotiated protocol {welcome["protocolVersion"]}, expected {Protocol.MessagePackFrameSerializer.ProtocolVersion}.");
    }

    private static async Task InitialSetAsync(SuiteContext context)
    {
        const int group = 300;
        var expected = new List<JsonNode>();
        for (var variant = 0; variant < 4; variant++)
        {
            context.Server.Call("PutSample", 3001L + variant, variant, group);
            expected.Add(Truth.Sample(3001 + variant, variant, group));
        }

        await context.ConnectAsync().ConfigureAwait(false);
        await context.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group = :g", Parameters(("g", Tagged.Int64(group)))).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [.. expected.Select(static row => Insert("Sample", row))], ordered: false);
        await CheckCacheAsync(context, "Sample", expected, static row => (string)row["Id"]! is var id && long.Parse(id, System.Globalization.CultureInfo.InvariantCulture) is >= 3001 and <= 3004).ConfigureAwait(false);
    }

    private static async Task CallsAsync(SuiteContext context)
    {
        await context.ConnectAsync().ConfigureAwait(false);
        var put = await context.Adapter.MustAsync("call", new JsonObject
        {
            ["reducer"] = "PutSample",
            ["arguments"] = new JsonArray(Tagged.Int64(4001), Tagged.Int64(0), Tagged.Int64(400)),
        }).ConfigureAwait(false);
        Check(ulong.Parse((string)put["lsn"]!, System.Globalization.CultureInfo.InvariantCulture) > 0, $"A committing call reported LSN {put["lsn"]}.");

        await ExpectCallErrorAsync(context, "Refuse", new JsonArray(Tagged.String("by design")), "rejected").ConfigureAwait(false);
        await ExpectCallErrorAsync(context, "NoSuchReducer", [], "unknown_reducer").ConfigureAwait(false);
        await ExpectCallErrorAsync(context, "PutSample", new JsonArray(Tagged.String("not a number")), "invalid_args").ConfigureAwait(false);

        var produced = await context.Adapter.MustAsync("callProducer", new JsonObject
        {
            ["producer"] = "Summarize",
            ["arguments"] = new JsonArray(Tagged.Int64(400)),
        }).ConfigureAwait(false);
        CheckSame(
            Tagged.Array(Tagged.Int64(1), Tagged.Int64(4001), Tagged.Null(), Tagged.Array(Tagged.Int64(4001))),
            produced["value"],
            "Summarize(400) with the trailing defaulted label left off");
    }

    private static async Task MergedCacheAsync(SuiteContext context)
    {
        await context.ConnectAsync().ConfigureAwait(false);
        var wide = await context.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group BETWEEN :lo AND :hi", Parameters(("lo", Tagged.Int64(100)), ("hi", Tagged.Int64(110)))).ConfigureAwait(false);
        var narrow = await context.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group = :g", Parameters(("g", Tagged.Int64(105)))).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [], ordered: true);

        await context.WriteAndSyncAsync("PutSample", 1001L, 0, 105).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [Insert("Sample", Truth.Sample(1001, 0, 105))], ordered: true, "A row both subscriptions cover arrives twice on the wire and must raise one insert");

        await context.WriteAndSyncAsync("ChangeSample", 1001L, 2).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [Update("Sample", Truth.Sample(1001, 0, 105), Truth.Sample(1001, 2, 105))], ordered: true, "A change to a doubly covered row must raise one update");

        await context.WriteAndSyncAsync("PutSample", 1002L, 0, 101).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [Insert("Sample", Truth.Sample(1002, 0, 101))], ordered: true);

        await context.Adapter.MustAsync("unsubscribe", new JsonObject { ["subscription"] = narrow }).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [], ordered: true, "Unsubscribing one of two covering subscriptions must not delete the row the other still covers");

        await context.Adapter.MustAsync("unsubscribe", new JsonObject { ["subscription"] = wide }).ConfigureAwait(false);
        CheckEvents(
            context.Adapter.TakeEvents(),
            [Delete("Sample", Truth.Sample(1001, 2, 105)), Delete("Sample", Truth.Sample(1002, 0, 101))],
            ordered: false,
            "Unsubscribing the last covering subscription must delete what only it covered");
        await CheckCacheAsync(context, "Sample", [], static _ => true).ConfigureAwait(false);
    }

    private static async Task RescopeAsync(SuiteContext context)
    {
        context.Server.Call("PutSample", 2001L, 0, 205);
        context.Server.Call("PutSample", 2002L, 2, 215);
        await context.ConnectAsync().ConfigureAwait(false);
        var subscription = await context.SubscribeAsync("Sample", "SELECT * FROM Sample WHERE Group BETWEEN :lo AND :hi", Parameters(("lo", Tagged.Int64(200)), ("hi", Tagged.Int64(210)))).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [Insert("Sample", Truth.Sample(2001, 0, 205))], ordered: true);

        await context.Adapter.MustAsync("rescope", new JsonObject
        {
            ["subscription"] = subscription,
            ["parameters"] = Parameters(("lo", Tagged.Int64(211)), ("hi", Tagged.Int64(220))),
        }).ConfigureAwait(false);

        // A re-scope has no acknowledgement and its diff carries no LSN, so the cursor cannot be
        // the barrier: wait for the diff's own events. Only then is a commit in the new scope sure
        // to arrive as an ordinary delta rather than inside the diff.
        await context.Adapter.WaitForEventsAsync(static events => events.Count >= 2, EventWait).ConfigureAwait(false);
        CheckEvents(
            context.Adapter.TakeEvents(),
            [Delete("Sample", Truth.Sample(2001, 0, 205)), Insert("Sample", Truth.Sample(2002, 2, 215))],
            ordered: false,
            "A re-scope's diff arrives at LSN 0 and must apply without the anchor test");

        await context.WriteAndSyncAsync("PutSample", 2003L, 0, 212).ConfigureAwait(false);
        CheckEvents(context.Adapter.TakeEvents(), [Insert("Sample", Truth.Sample(2003, 0, 212))], ordered: true, "The re-scoped subscription must carry on live");
        await CheckCacheAsync(context, "Sample", [Truth.Sample(2002, 2, 215), Truth.Sample(2003, 0, 212)], static _ => true).ConfigureAwait(false);
    }

    private static async Task ColumnMasksAsync(SuiteContext context)
    {
        context.Server.Call("PutSecret", "mask-mine", ConformanceServer.Caller, 1, "only me");
        context.Server.Call("PutSecret", "mask-theirs", SampleRows.OtherIdentity, 2, "not you");
        await context.ConnectAsync().ConfigureAwait(false);
        var response = await context.Adapter.RequestAsync("subscribe", new JsonObject
        {
            ["table"] = "Secret",
            ["query"] = "SELECT * FROM Secret",
            ["parameters"] = null,
        }).ConfigureAwait(false);

        // Either answer keeps C4.4: refuse the subscription, or deliver the masked row partially.
        // What no client may do is show the hidden column as a value.
        var other = SampleRows.OtherIdentity.ToString();
        var shown = context.Adapter.TakeEvents()
            .Select(static e => e["row"] ?? e["new"])
            .Concat(response["ok"]?.GetValue<bool>() == true
                ? (await context.Adapter.MustAsync("cache", new JsonObject { ["table"] = "Secret" }).ConfigureAwait(false))["rows"]!.AsArray()
                : [])
            .Where(row => (string?)row?["Owner"] == other && row?["Hidden"] is not null)
            .ToList();
        if (shown.Count > 0)
            throw new ConformanceFailure($"A column policy hid Secret.Hidden from this caller, but the client presented it as {shown[0]!["Hidden"]!.ToJsonString()}.");
    }

    private static async Task ExpectCallErrorAsync(SuiteContext context, string reducer, JsonArray arguments, string code)
    {
        var response = await context.Adapter.RequestAsync("call", new JsonObject { ["reducer"] = reducer, ["arguments"] = arguments }).ConfigureAwait(false);
        Check(response["ok"]?.GetValue<bool>() == false, $"Calling {reducer} succeeded; expected {code}.");
        CheckSame(code, response["error"]!["code"], $"The error code for {reducer}");
    }

    private static IEnumerable<JsonObject> Frames(JsonObject session) =>
        session["steps"]!.AsArray().SelectMany(static step => step!["receive"]!.AsArray()).Select(static f => f!.AsObject());

    private static JsonObject Parameters(params (string Name, JsonObject Value)[] parameters)
    {
        var map = new JsonObject();
        foreach (var (name, value) in parameters)
            map[name] = value;
        return map;
    }

    private static JsonObject Insert(string table, JsonNode row) => new() { ["event"] = "insert", ["table"] = table, ["row"] = row.DeepClone() };

    private static JsonObject Update(string table, JsonNode old, JsonNode now) => new() { ["event"] = "update", ["table"] = table, ["old"] = old.DeepClone(), ["new"] = now.DeepClone() };

    private static JsonObject Delete(string table, JsonNode row) => new() { ["event"] = "delete", ["table"] = table, ["row"] = row.DeepClone() };

    private static void CheckEvents(List<JsonObject> actual, JsonObject[] expected, bool ordered, string? because = null)
    {
        var actualText = actual.Select(Canonical).ToList();
        var expectedText = expected.Select(Canonical).ToList();
        if (!ordered)
        {
            actualText.Sort(StringComparer.Ordinal);
            expectedText.Sort(StringComparer.Ordinal);
        }

        if (!actualText.SequenceEqual(expectedText))
        {
            throw new ConformanceFailure(
                $"{(because is null ? "Unexpected events" : because)}.\n  expected: [{string.Join(", ", expectedText)}]\n  actual:   [{string.Join(", ", actualText)}]");
        }
    }

    private static async Task CheckCacheAsync(SuiteContext context, string table, IEnumerable<JsonNode> expected, Func<JsonObject, bool> filter)
    {
        var rows = (await context.Adapter.MustAsync("cache", new JsonObject { ["table"] = table }).ConfigureAwait(false))["rows"]!.AsArray();
        var actual = rows.Select(static r => r!.AsObject()).Where(filter).Select(Canonical).Order(StringComparer.Ordinal).ToList();
        var wanted = expected.Select(Canonical).Order(StringComparer.Ordinal).ToList();
        if (!actual.SequenceEqual(wanted))
            throw new ConformanceFailure($"The {table} cache holds the wrong rows.\n  expected: [{string.Join(", ", wanted)}]\n  actual:   [{string.Join(", ", actual)}]");
    }

    private static void Check(bool condition, string failure)
    {
        if (!condition)
            throw new ConformanceFailure(failure);
    }

    private static void CheckSame(JsonNode? expected, JsonNode? actual, string what)
    {
        if (Canonical(expected) != Canonical(actual))
            throw new ConformanceFailure($"{what}:\n  expected {Canonical(expected)}\n  actual   {Canonical(actual)}");
    }

    /// <summary>A node's JSON text with object members in ordinal order, so equal values compare equal as text.</summary>
    internal static string Canonical(JsonNode? node) => node switch
    {
        null => "null",
        JsonObject obj => "{" + string.Join(",", obj.OrderBy(static p => p.Key, StringComparer.Ordinal).Select(p => JsonValue.Create(p.Key).ToJsonString() + ":" + Canonical(p.Value))) + "}",
        JsonArray array => "[" + string.Join(",", array.Select(Canonical)) + "]",
        _ => JsonNode.Parse(node.ToJsonString())!.ToJsonString(),
    };

    /// <summary>What a scenario works with: the adapter, the server, and the committed corpus.</summary>
    private sealed record SuiteContext(AdapterChannel Adapter, ConformanceServer Server, JsonObject Corpus)
    {
        public Task<JsonObject> ConnectAsync() => Adapter.MustAsync("connect", new JsonObject
        {
            ["url"] = Server.SocketUri.ToString(),
            ["token"] = Server.Token,
        });

        public async Task<int> SubscribeAsync(string table, string query, JsonObject? parameters)
        {
            var response = await Adapter.MustAsync("subscribe", new JsonObject
            {
                ["table"] = table,
                ["query"] = query,
                ["parameters"] = parameters,
            }).ConfigureAwait(false);
            return (int)response["subscription"]!;
        }

        /// <summary>Commits server-side, then waits until the client's cursor has passed the commit.</summary>
        public async Task<ulong> WriteAndSyncAsync(string reducer, params object?[] arguments)
        {
            var lsn = Server.Call(reducer, arguments);
            await Adapter.MustAsync("sync", new JsonObject { ["lsn"] = CorpusJson.U64(lsn) }).ConfigureAwait(false);
            return lsn;
        }
    }
}
