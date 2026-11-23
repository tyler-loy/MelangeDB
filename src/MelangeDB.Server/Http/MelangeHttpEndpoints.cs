using System.Diagnostics;
using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Server;

/// <summary>
/// The plain-HTTP endpoints. WebSocket is the wrong shape for two of the three reference client
/// types: admin consoles run one-shot SQL, and terrain generation bulk-loads tens of thousands of
/// rows — neither wants a subscription protocol. Every endpoint requires a valid bearer JWT,
/// validated against the host's own scheme: the IdP is the gate here exactly as on the socket.
/// </summary>
internal static class MelangeHttpEndpoints
{
    /// <summary>POST {path}/call/{reducer} — one-shot reducer invocation: a JSON array of arguments.</summary>
    public static async Task CallAsync(HttpContext context, MelangeTransport transport)
    {
        if (await AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        var reducer = (string)context.Request.RouteValues["reducer"]!;
        object?[] arguments;
        try
        {
            using var body = await JsonDocument.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted).ConfigureAwait(false);
            arguments = body.RootElement.ValueKind switch
            {
                JsonValueKind.Array => body.RootElement.EnumerateArray().Select(ToArgument).ToArray(),
                JsonValueKind.Null or JsonValueKind.Undefined => [],
                _ => throw new JsonException("The request body must be a JSON array of arguments."),
            };
        }
        catch (JsonException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
            return;
        }

        try
        {
            var lsn = transport.Reducers.Call(
                reducer,
                session.Identity,
                ConnectionId.None,
                ReducerArguments.Encode(arguments),
                source: CallSource.Client(session.IsGuest),
                claims: session.CapturedClaims);
            await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
            {
                writer.WriteBoolean("ok", true);
                writer.WriteNumber("lsn", lsn);
            }).ConfigureAwait(false);
        }
        catch (ReducerArgumentException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
        }
        catch (RejectedException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.Rejected, exception.Message).ConfigureAwait(false);
        }
        catch (TransientRejectionException exception)
        {
            // A self-clearing refusal (handoff freeze, border-copy routing, a fenced node):
            // 409, retry unchanged — not a server fault, so nothing is logged.
            await WriteErrorAsync(context, StatusCodes.Status409Conflict, MelangeErrorCodes.Transient, exception.Message).ConfigureAwait(false);
        }
        catch (CommitOutcomeUnknownException exception)
        {
            // Durable, but the node lost its lease before acknowledging: not a fault, and not a
            // promise the call was not applied either. The code tells the caller to look first.
            await WriteErrorAsync(context, StatusCodes.Status503ServiceUnavailable, MelangeErrorCodes.OutcomeUnknown, exception.Message).ConfigureAwait(false);
        }
        catch (RateLimitedException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status429TooManyRequests, MelangeErrorCodes.RateLimited, exception.Message).ConfigureAwait(false);
        }
        catch (ReducerDeniedException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status403Forbidden, MelangeErrorCodes.Denied, exception.Message).ConfigureAwait(false);
        }
        catch (UnknownReducerException exception)
        {
            // Only a name that did not resolve. An ArgumentException escaping the reducer's *body*
            // — an ArgumentOutOfRangeException from a row decode, say — falls through to the arm
            // below, because by the time a body runs, resolution has provably succeeded.
            await WriteErrorAsync(context, StatusCodes.Status404NotFound, MelangeErrorCodes.UnknownReducer, exception.Message).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            // "See the server logs" is only true if something is written to them; the socket path
            // has logged this since phase 03 and this one did not.
            LogMessages.ReducerCallFailed(transport.Logger, reducer, exception);
            await WriteErrorAsync(context, StatusCodes.Status500InternalServerError, MelangeErrorCodes.Internal, "The reducer failed; see the server logs.").ConfigureAwait(false);
        }
    }

    /// <summary>
    /// POST {path}/producers/{producer} — a producer call: a JSON array of arguments in (trailing
    /// defaulted ones may be left off), <c>{"ok":true,"lsn":N,"result":...}</c> out, the result
    /// rendered as JSON by <see cref="ProducerCalls.RenderJson"/>. Errors carry the socket's codes.
    /// </summary>
    public static async Task CallProducerAsync(HttpContext context, MelangeTransport transport)
    {
        if (await AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        var producer = (string)context.Request.RouteValues["producer"]!;
        if (transport.Producers is not { } producers)
        {
            await WriteErrorAsync(context, StatusCodes.Status404NotFound, MelangeErrorCodes.UnknownProducer, $"No producer named '{producer}' is registered.").ConfigureAwait(false);
            return;
        }

        object?[] arguments;
        try
        {
            using var body = await JsonDocument.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted).ConfigureAwait(false);
            arguments = body.RootElement.ValueKind switch
            {
                JsonValueKind.Array => body.RootElement.EnumerateArray().Select(ToArgument).ToArray(),
                JsonValueKind.Null or JsonValueKind.Undefined => [],
                _ => throw new JsonException("The request body must be a JSON array of arguments."),
            };
        }
        catch (JsonException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
            return;
        }

        ProducerResult result;
        try
        {
            result = await producers.CallAsync(
                producer,
                session.Identity,
                ConnectionId.None,
                ReducerArguments.Encode(arguments),
                Activity.Current?.Context ?? default,
                CallSource.Client(session.IsGuest),
                session.CapturedClaims,
                context.RequestAborted,
                ProducerCalls.RenderJson).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            var (code, status, message) = ProducerCalls.Classify(exception, producer, transport.Logger);
            await WriteErrorAsync(context, status, code, message).ConfigureAwait(false);
            return;
        }

        await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
        {
            writer.WriteBoolean("ok", true);
            writer.WriteNumber("lsn", result.Lsn);
            writer.WritePropertyName("result");
            writer.WriteRawValue(result.Payload.Span, skipInputValidation: true);
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// POST {path}/bulk — bulk ingestion: <c>{"tables": {"TableName": [{...row}, ...]}}</c>,
    /// appended as one write set, one log record, not one transaction per row. Off until
    /// <c>Bulk:Enabled</c> opts in, and requires the caller's <c>Bulk:OwnerRole</c> claim —
    /// bulk writes bypass every reducer and its policies, so any valid token is not enough.
    /// <para>
    /// Answers <c>{"ok", "rows", "results": [{"shard", "lsn", "rows"}]}</c> — one result per engine
    /// that took rows. The atomicity guarantee is <b>per engine</b>: each engine appends its share
    /// as one write set in one transaction, and a single-node deployment has one engine, so
    /// nothing weaker is promised there than before. An LSN is meaningful only inside one log,
    /// which is why a batch spanning shards reports an array rather than a number.
    /// </para>
    /// <para>
    /// In a cluster the batch is routed rather than written locally: a hub fans it out to the
    /// shard engines owning the rows (see <see cref="IBulkRouter"/>), and a shard node refuses a
    /// batch touching <c>Partitioned</c> tables, because its own engine is not any shard's engine
    /// and the rows would land authoritative nowhere useful.
    /// </para>
    /// </summary>
    public static async Task BulkAsync(HttpContext context, MelangeTransport transport)
    {
        if (await AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        var bulkOptions = transport.Options.Bulk;
        if (!bulkOptions.Enabled)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.BulkDisabled,
                "Bulk ingestion is disabled; set Bulk:Enabled to true to opt in.").ConfigureAwait(false);
            return;
        }

        if (!session.IsBulkOwner)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.OwnerRequired,
                "This caller's token carries no Bulk:OwnerRole claim; bulk owner capability is never granted implicitly.").ConfigureAwait(false);
            return;
        }

        List<BulkRow> rows = [];
        try
        {
            using var body = await JsonDocument.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted).ConfigureAwait(false);
            if (!body.RootElement.TryGetProperty("tables", out var tables) || tables.ValueKind != JsonValueKind.Object)
                throw new JsonException("The request body must be {\"tables\": {\"TableName\": [rows...]}}.");
            foreach (var table in tables.EnumerateObject())
            {
                foreach (var row in table.Value.EnumerateArray())
                {
                    var columns = new Dictionary<string, object?>(StringComparer.Ordinal);
                    foreach (var column in row.EnumerateObject())
                        columns[column.Name] = ToArgument(column.Value);
                    rows.Add(new BulkRow(table.Name, columns));
                }
            }
        }
        catch (JsonException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
            return;
        }

        var clusterRole = transport.Options.Cluster.Role;
        if (clusterRole == ClusterRole.Shard
            && PartitionedTableIn(transport.Engine.Schema, rows) is { } partitioned)
        {
            // The mirror of the ad-hoc SQL refusal (#114): a shard node's own engine holds the
            // node's Local tables, not any shard's Partitioned rows. Writing here would succeed
            // and be authoritative nowhere the game reads from, which is worse than a refusal.
            await WriteErrorAsync(
                context,
                StatusCodes.Status400BadRequest,
                MelangeErrorCodes.PartitionedElsewhere,
                $"Table '{partitioned}' is Partitioned: its rows live in the per-shard engines, not in this node's "
                + $"(Cluster:Role is {clusterRole}). Post the batch to the hub, which fans it out to the shards that "
                + "own the rows.").ConfigureAwait(false);
            return;
        }

        try
        {
            // Role is the gate, not merely the presence of a router: the cluster package registers
            // one whenever it is added, and resolving it anywhere but the hub would stand a hub
            // runtime up on a node that is not one.
            var results = clusterRole == ClusterRole.Hub
                && context.RequestServices.GetService(typeof(IBulkRouter)) is IBulkRouter router
                ? await router.RouteAsync(session.Identity, rows, context.RequestAborted).ConfigureAwait(false)
                : LocalResult(transport.Engine.BulkInsert(session.Identity, rows), rows.Count);

            // One result per engine that took rows. `shard` is null for a node-local engine,
            // which is not any shard's — on a single-node deployment that is the only element.
            await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
            {
                writer.WriteBoolean("ok", true);
                writer.WriteNumber("rows", rows.Count);
                writer.WriteStartArray("results");
                foreach (var result in results)
                {
                    writer.WriteStartObject();
                    if (result.Shard is { } shard)
                        writer.WriteNumber("shard", shard);
                    else
                        writer.WriteNull("shard");
                    writer.WriteNumber("lsn", result.Lsn);
                    writer.WriteNumber("rows", result.Rows);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
            }).ConfigureAwait(false);
        }
        catch (ArgumentException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
        }
        catch (InvalidOperationException exception)
        {
            // A routing failure — no owner for a destination shard, no live link to it. The
            // operator's move is to fix the cluster and re-post: bulk rows are upserts and
            // [AutoInc] is originator-prefixed, so re-running a partly-applied batch is sound.
            await WriteErrorAsync(context, StatusCodes.Status503ServiceUnavailable, MelangeErrorCodes.Transient, exception.Message).ConfigureAwait(false);
        }
    }

    private static IReadOnlyList<BulkResult> LocalResult(CommitRecord? record, int rows) =>
        [new BulkResult(Shard: null, record?.Lsn ?? 0, rows)];

    /// <summary>The first <c>Partitioned</c> table named by a batch, or null when none is.</summary>
    private static string? PartitionedTableIn(SchemaRegistry schema, IReadOnlyList<BulkRow> rows)
    {
        foreach (var row in rows)
        {
            if (schema.TryGetByName(row.Table, out var table) && table.Placement == Placement.Partitioned)
                return table.Name;
        }

        return null;
    }

    /// <summary>
    /// POST {path}/sql — one-shot query: <c>{"query": "...", "params": {...}}</c>. Off until
    /// <c>Sql:AdHocEnabled</c> opts in. The row shapes run against the hot store at head,
    /// under the same cost ceilings subscriptions have; aggregate shapes (<c>COUNT</c>/<c>SUM</c>/
    /// <c>AVG</c>/<c>MIN</c>/<c>MAX</c>, <c>GROUP BY</c>, <c>DATE_TRUNC</c> bucketing) run against
    /// the relational backplane and reflect its applier's checkpoint. <c>Sql:AdHocMode</c> is the
    /// two-mode contract: <c>PolicyEnforced</c> (default) applies the caller's row and column
    /// policies exactly as a subscription would; <c>Owner</c> deliberately bypasses them, requires
    /// the <c>Sql:OwnerRole</c> claim per caller, and additionally sees private relational-backplane
    /// tables. Aggregates are owner-mode only — row policies are in-process code that cannot be
    /// pushed into Postgres, and a policy-enforced aggregate is refused loudly rather than
    /// computed unenforced. <c>[ServerOnly]</c> columns are excluded in both modes — "never
    /// leaves the process" has no modes.
    /// </summary>
    public static async Task SqlAsync(HttpContext context, MelangeTransport transport)
    {
        if (await AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        try
        {
            // The gates first, as before the body is read: a disabled endpoint answers 403
            // whatever was sent to it.
            AdHocSql.CheckAccess(transport, session);
        }
        catch (AdHocSqlRefusal refusal)
        {
            await WriteErrorAsync(context, refusal.Status, refusal.Code, refusal.Message).ConfigureAwait(false);
            return;
        }

        string query;
        Dictionary<string, object?>? parameters = null;
        try
        {
            using var body = await JsonDocument.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted).ConfigureAwait(false);
            if (!body.RootElement.TryGetProperty("query", out var queryElement) || queryElement.ValueKind != JsonValueKind.String)
                throw new JsonException("The request body must be {\"query\": \"...\", \"params\": {...}}.");
            query = queryElement.GetString()!;
            if (body.RootElement.TryGetProperty("params", out var paramsElement) && paramsElement.ValueKind == JsonValueKind.Object)
            {
                parameters = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var parameter in paramsElement.EnumerateObject())
                    parameters[parameter.Name] = ToArgument(parameter.Value);
            }
        }
        catch (JsonException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
            return;
        }

        AdHocSqlResult result;
        try
        {
            result = await AdHocSql.ExecuteAsync(transport, session, query, parameters, context.RequestServices, context.RequestAborted).ConfigureAwait(false);
        }
        catch (AdHocSqlRefusal refusal)
        {
            await WriteErrorAsync(context, refusal.Status, refusal.Code, refusal.Message).ConfigureAwait(false);
            return;
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            return;
        }

        await WriteJsonAsync(context, StatusCodes.Status200OK, writer => AdHocSql.WriteResult(writer, result)).ConfigureAwait(false);
    }

    /// <summary>
    /// POST {path}/ticket — exchanges a valid JWT (over TLS) for a single-use, short-lived connect
    /// ticket presented on the socket URL. The path that works from browsers, whose WebSocket API
    /// cannot set headers; a token in the query string would end up in access and proxy logs.
    /// </summary>
    public static async Task TicketAsync(HttpContext context, MelangeTransport transport)
    {
        if (await AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        var (ticket, expiresInSeconds) = transport.Tickets.Mint(session);
        await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
        {
            writer.WriteString("ticket", ticket);
            writer.WriteNumber("expiresInSeconds", expiresInSeconds);
        }).ConfigureAwait(false);
    }

    /// <summary>Validates the request's bearer token, or writes 401 and returns null.</summary>
    internal static async Task<AuthResult?> AuthenticateAsync(HttpContext context, MelangeTransport transport)
    {
        var token = MelangeEndpointRouteBuilderExtensions.BearerToken(context);
        switch (await transport.Authenticator.ValidateAsync(token).ConfigureAwait(false))
        {
            case AuthResult session when !transport.Sessions.IsRevoked(session.Identity):
                return session;
            case AuthResult:
                await WriteErrorAsync(context, StatusCodes.Status401Unauthorized, MelangeErrorCodes.Unauthorized, "This identity is revoked.").ConfigureAwait(false);
                return null;
            case AuthFailure failure:
                await WriteErrorAsync(context, StatusCodes.Status401Unauthorized, MelangeErrorCodes.Unauthorized, failure.Reason).ConfigureAwait(false);
                return null;
            default:
                return null;
        }
    }

    /// <summary>
    /// JSON to argument mapping: numbers to long/ulong/double, plus <c>{"$identity": "hex"}</c>,
    /// <c>{"$bytes": "base64"}</c>, and <c>{"$timestamp": micros}</c> for the kinds JSON lacks.
    /// <c>{"$interval": micros}</c> exists for bulk-loading repeating timer rows: it coerces to a
    /// repeating <c>ScheduleAt</c>, where a <c>$timestamp</c> coerces to a one-shot.
    /// </summary>
    internal static object? ToArgument(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.TryGetInt64(out var signed)
            ? signed
            : element.TryGetUInt64(out var unsigned) ? unsigned : (object)element.GetDouble(),
        JsonValueKind.Array => element.EnumerateArray().Select(ToArgument).ToArray(),
        JsonValueKind.Object when element.TryGetProperty("$identity", out var hex) =>
            new Identity(Convert.FromHexString(hex.GetString() ?? string.Empty)),
        JsonValueKind.Object when element.TryGetProperty("$bytes", out var base64) =>
            Convert.FromBase64String(base64.GetString() ?? string.Empty),
        JsonValueKind.Object when element.TryGetProperty("$timestamp", out var micros) =>
            new Timestamp(micros.GetInt64()),
        JsonValueKind.Object when element.TryGetProperty("$interval", out var interval) =>
            TimeSpan.FromMicroseconds(interval.GetInt64()),
        _ => throw new JsonException($"JSON value of kind {element.ValueKind} is not a valid argument."),
    };

    /// <summary>
    /// A SQL result value as JSON. 64-bit integers and timestamps are decimal strings: JSON has
    /// one number type, and a JavaScript consumer's <c>JSON.parse</c> rounds an integer past 2^53
    /// without an error, so an <c>Int64</c> id, a <c>UInt64</c> counter or a <c>COUNT</c> would
    /// come back wrong. Integers of 32 bits and less stay numbers, since every consumer reads them
    /// exactly.
    /// </summary>
    internal static void WriteJsonValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case long signed:
                writer.WriteStringValue(signed.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
            case ulong unsigned:
                writer.WriteStringValue(unsigned.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
            case sbyte or short or int:
                writer.WriteNumberValue(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case byte or ushort or uint:
                writer.WriteNumberValue(Convert.ToUInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case float f:
                writer.WriteNumberValue(f);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case decimal m:
                writer.WriteNumberValue(m);
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case byte[] bytes:
                writer.WriteStringValue(Convert.ToBase64String(bytes));
                break;
            case Identity identity:
                writer.WriteStringValue(identity.ToString());
                break;
            case Timestamp timestamp:
                writer.WriteStringValue(timestamp.UnixTimeMicroseconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
            default:
                writer.WriteStringValue(value.ToString());
                break;
        }
    }

    /// <summary>
    /// The header names an error response is correlated by. They are the de-facto convention a
    /// host's own middleware already stamps, so a caller reads one name whether the failure came
    /// from here or from something in front of us.
    /// </summary>
    private const string TraceIdHeader = "X-Trace-Id";
    private const string RequestIdHeader = "X-Request-Id";

    /// <summary>
    /// Writes the error body, and stamps the ids that make this exact request findable in the
    /// server's log store. A failed call is the one a player reports, and "it didn't work around
    /// nine" is unfindable without an id — the server is already tracing, so the id existed all
    /// along and simply never reached the caller. It goes in both places on purpose: in the body
    /// so a client that only has the response text can still quote it, and in the headers so one
    /// that never reads the body — or that got a non-JSON failure from a proxy — has it too.
    /// <see cref="Activity"/> is absent when the host configured no tracing, which is why
    /// <see cref="HttpContext.TraceIdentifier"/> is always written as well: something quotable
    /// exists in every deployment.
    /// </summary>
    internal static Task WriteErrorAsync(HttpContext context, int status, string code, string message)
    {
        var traceId = Activity.Current?.TraceId.ToHexString();
        var requestId = context.TraceIdentifier;
        var headers = context.Response.Headers;
        // Assigned only when absent: a host that stamps its own correlation ids owns the value,
        // and the indexer would otherwise replace it with ours rather than duplicating it.
        if (traceId is not null && !headers.ContainsKey(TraceIdHeader))
            headers[TraceIdHeader] = traceId;
        if (!string.IsNullOrEmpty(requestId) && !headers.ContainsKey(RequestIdHeader))
            headers[RequestIdHeader] = requestId;

        return WriteJsonAsync(context, status, writer =>
        {
            writer.WriteString("error", code);
            writer.WriteString("message", message);
            if (traceId is not null)
                writer.WriteString("traceId", traceId);
            if (!string.IsNullOrEmpty(requestId))
                writer.WriteString("requestId", requestId);
        });
    }

    private static async Task WriteJsonAsync(HttpContext context, int status, Action<Utf8JsonWriter> write)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json";
        await using var writer = new Utf8JsonWriter(context.Response.BodyWriter);
        writer.WriteStartObject();
        write(writer);
        writer.WriteEndObject();
        await writer.FlushAsync(context.RequestAborted).ConfigureAwait(false);
    }

    internal sealed class NullSink : IDeltaSink
    {
        public static readonly NullSink Instance = new();

        public void EnqueueDelta(TransactionUpdateFrame frame)
        {
        }
    }

    private static class LogMessages
    {
        /// <summary>The socket path's 1204, on the one-shot path: the same failure deserves the same id.</summary>
        private static readonly Action<ILogger, string, Exception?> ReducerCallFailedMessage =
            LoggerMessage.Define<string>(
                LogLevel.Error,
                new EventId(1204, "ReducerCallFailed"),
                "Reducer '{Reducer}' threw an unexpected exception during a transport call.");

        public static void ReducerCallFailed(ILogger logger, string reducer, Exception failure) =>
            ReducerCallFailedMessage(logger, reducer, failure);
    }
}
