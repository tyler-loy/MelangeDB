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
                source: CallSource.Client(session.IsGuest));
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
    /// the relational tier and reflect its applier's checkpoint. <c>Sql:AdHocMode</c> is the
    /// two-mode contract: <c>PolicyEnforced</c> (default) applies the caller's row and column
    /// policies exactly as a subscription would; <c>Owner</c> deliberately bypasses them, requires
    /// the <c>Sql:OwnerRole</c> claim per caller, and additionally sees private relational-tier
    /// tables. Aggregates are owner-mode only — row policies are in-process code that cannot be
    /// pushed into Postgres, and a policy-enforced aggregate is refused loudly rather than
    /// computed unenforced. <c>[ServerOnly]</c> columns are excluded in both modes — "never
    /// leaves the process" has no modes.
    /// </summary>
    public static async Task SqlAsync(HttpContext context, MelangeTransport transport)
    {
        if (await AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        var sqlOptions = transport.Options.Sql;
        if (!sqlOptions.AdHocEnabled)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.SqlDisabled,
                "Ad-hoc SQL is disabled; set Sql:AdHocEnabled to true to opt in.").ConfigureAwait(false);
            return;
        }

        var owner = sqlOptions.AdHocMode == AdHocSqlMode.Owner;
        if (owner && !session.IsSqlOwner)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.OwnerRequired,
                "Sql:AdHocMode is Owner and this caller's token carries no Sql:OwnerRole claim; owner mode is never granted implicitly.").ConfigureAwait(false);
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

        try
        {
            var adHoc = SqlSubsetParser.ParseAdHoc(query, parameters);
            if (adHoc.Aggregate is { } aggregate)
            {
                await AggregateAsync(context, transport, aggregate, owner).ConfigureAwait(false);
                return;
            }

            var parsed = adHoc.Rows!;
            var clusterRole = transport.Options.Cluster.Role;
            var limits = transport.Options.Subscriptions;
            var enforced = !owner;
            var callerContext = enforced
                ? new PolicyContext(session.Identity, session.IsGuest, transport.Engine.CommittedView)
                : null;
            var (subscription, rows) = transport.Engine.ReadConsistent(head =>
            {
                var compiled = ServerSubscription.Compile(
                    NullSink.Instance, 0, parsed, transport.Engine.Schema, limits,
                    enforced ? transport.Policies : null, callerContext,
                    allowPrivateRelational: owner);

                // Placement is inert on a single node, so this fires only in a cluster — where
                // a Partitioned table's rows live in the shard engines and this endpoint reads
                // the node-local one: on a hub that holds Global/Replicated tables, on a shard
                // node its own engine rather than any shard's. Refusing beats the empty result
                // it would otherwise return, which an operator cannot distinguish from "no rows"
                // and will read as fact. Checked after Compile on purpose: Compile answers
                // unknown and private tables identically so the error reveals no existence, and
                // pre-checking here would turn this refusal into that oracle.
                if (clusterRole != ClusterRole.None && compiled.Schema.Placement == Placement.Partitioned)
                {
                    throw new SubscriptionRejectedException(
                        MelangeErrorCodes.PartitionedElsewhere,
                        $"Table '{compiled.Schema.Name}' is Placement.Partitioned, and its rows live in the shard "
                        + $"engines rather than in this node's (Cluster:Role is {clusterRole}). Ad-hoc SQL reads the "
                        + "node-local engine only. Reach partitioned rows through a reducer or a subscription, which "
                        + "the gateway routes to the owning shard.");
                }

                var collected = new List<(ReadOnlyMemory<byte> Row, IReadOnlySet<string>? Columns)>();
                long bytes = 0;
                foreach (var pair in compiled.MatchingRows(transport.Engine.HotStore))
                {
                    if (!compiled.PolicyAdmits(pair.Value.Span))
                        continue;

                    // Column masks are evaluated here, under the lock, so they read the same
                    // committed state the rows were collected at.
                    collected.Add((pair.Value, compiled.VisibleColumns(pair.Value.Span)));
                    bytes += pair.Value.Length;
                    if (collected.Count > limits.MaxRowsPerSubscription || bytes > limits.MaxBytesPerSubscription)
                    {
                        throw new SubscriptionRejectedException(
                            MelangeErrorCodes.TooManyRows,
                            "The result exceeds the configured row or byte ceiling; narrow the predicate.");
                    }
                }

                return (compiled, collected);
            });

            var schema = subscription.Schema;
            var columns = subscription.StaticWireColumns is { } wire
                ? schema.Columns.Select(c => c.Name).Where(wire.Contains).ToArray()
                : [.. schema.Columns.Select(c => c.Name)];
            await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
            {
                writer.WriteStartArray("columns");
                foreach (var column in columns)
                    writer.WriteStringValue(column);
                writer.WriteEndArray();
                writer.WriteStartArray("rows");
                foreach (var (rowBytes, visibleColumns) in rows)
                {
                    // A column-policy mask writes null for a hidden value; [ServerOnly] columns
                    // are absent from the column list entirely.
                    var values = RowWire.ToColumns(schema, rowBytes.Span, visibleColumns);
                    writer.WriteStartArray();
                    foreach (var column in columns)
                        WriteJsonValue(writer, values.GetValueOrDefault(column));
                    writer.WriteEndArray();
                }

                writer.WriteEndArray();
            }).ConfigureAwait(false);
        }
        catch (SqlParseException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.ParseError, exception.Message).ConfigureAwait(false);
        }
        catch (SubscriptionRejectedException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, exception.Code, exception.Message).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The aggregate half of <c>{path}/sql</c>: owner mode only, relational tier only, executed by
    /// the registered <see cref="IRelationalQueryExecutor"/> — absent means no relational tier is
    /// configured, which is an explicit error rather than an empty result. Results reflect the
    /// tier at its applier's checkpoint; that lag is the design, not a bug, and the applier's
    /// health check is where it becomes visible.
    /// </summary>
    private static async Task AggregateAsync(HttpContext context, MelangeTransport transport, AggregateQuery aggregate, bool owner)
    {
        if (!owner)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.OwnerRequired,
                "Aggregates run in owner mode only: row policies are in-process code that cannot be pushed into Postgres, " +
                "and a policy-enforced aggregate would silently drop enforcement. Set Sql:AdHocMode to Owner.").ConfigureAwait(false);
            return;
        }

        var built = AdHocAggregateBuilder.Build(aggregate, transport.Engine.Schema);

        // Placement and tier are independent axes, so closing the empty-result lie for row shapes
        // (#114) did not close it here: a Partitioned table can also be Relational, and this node's
        // relational tier holds only what this node's engine wrote. On a hub that is every shard's
        // rows except the partitioned ones — the aggregate would answer a confident number over a
        // subset, which is worse than the empty result the row path refuses. Checked after Build
        // for the same reason the row path checks after Compile: Build answers unknown and
        // wrong-tier tables first, so this refusal is never an existence oracle.
        var clusterRole = transport.Options.Cluster.Role;
        if (clusterRole != ClusterRole.None && built.Table.Placement == Placement.Partitioned)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status400BadRequest, MelangeErrorCodes.PartitionedElsewhere,
                $"Table '{built.Table.Name}' is Placement.Partitioned, and its rows live in the shard engines rather "
                + $"than in this node's (Cluster:Role is {clusterRole}). An aggregate here would count only what this "
                + "node wrote and report it as the total.").ConfigureAwait(false);
            return;
        }

        var executor = (Core.IRelationalQueryExecutor?)context.RequestServices.GetService(typeof(Core.IRelationalQueryExecutor));
        if (executor is null)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status400BadRequest, MelangeErrorCodes.NoRelationalTier,
                "No relational tier is configured; aggregates need AddPostgres(...).").ConfigureAwait(false);
            return;
        }
        Core.RelationalQueryResult result;
        try
        {
            result = await executor.ExecuteAsync(built, context.RequestAborted).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            return;
        }
        catch (Exception)
        {
            await WriteErrorAsync(
                context, StatusCodes.Status503ServiceUnavailable, MelangeErrorCodes.RelationalUnavailable,
                "The relational tier did not answer; see the server logs. Writes and subscriptions are unaffected.").ConfigureAwait(false);
            return;
        }

        await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
        {
            writer.WriteStartArray("columns");
            foreach (var column in result.Columns)
                writer.WriteStringValue(column);
            writer.WriteEndArray();
            writer.WriteStartArray("rows");
            foreach (var row in result.Rows)
            {
                writer.WriteStartArray();
                foreach (var value in row)
                    WriteJsonValue(writer, value);
                writer.WriteEndArray();
            }

            writer.WriteEndArray();
        }).ConfigureAwait(false);
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
    private static object? ToArgument(JsonElement element) => element.ValueKind switch
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

    private static void WriteJsonValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case sbyte or short or int or long:
                writer.WriteNumberValue(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case byte or ushort or uint or ulong:
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
                writer.WriteNumberValue(timestamp.UnixTimeMicroseconds);
                break;
            default:
                writer.WriteStringValue(value.ToString());
                break;
        }
    }

    internal static Task WriteErrorAsync(HttpContext context, int status, string code, string message) =>
        WriteJsonAsync(context, status, writer =>
        {
            writer.WriteString("error", code);
            writer.WriteString("message", message);
        });

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

    private sealed class NullSink : IDeltaSink
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
