using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.AspNetCore.Http;

namespace MelangeDB.Server;

/// <summary>
/// The plain-HTTP endpoints. WebSocket is the wrong shape for two of the three reference client
/// types: admin consoles run one-shot SQL, and terrain generation bulk-loads tens of thousands of
/// rows — neither wants a subscription protocol. Identity here is stubbed from the bearer token
/// until phase 04 validates it.
/// </summary>
internal static class MelangeHttpEndpoints
{
    /// <summary>POST {path}/call/{reducer} — one-shot reducer invocation: a JSON array of arguments.</summary>
    public static async Task CallAsync(HttpContext context, MelangeTransport transport)
    {
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
            var lsn = transport.Reducers.Call(reducer, CallerOf(context), ConnectionId.None, ReducerArguments.Encode(arguments));
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
        catch (ArgumentException)
        {
            await WriteErrorAsync(context, StatusCodes.Status404NotFound, MelangeErrorCodes.UnknownReducer, $"No reducer named '{reducer}' is registered.").ConfigureAwait(false);
        }
        catch (Exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status500InternalServerError, MelangeErrorCodes.Internal, "The reducer failed; see the server logs.").ConfigureAwait(false);
        }
    }

    /// <summary>
    /// POST {path}/bulk — bulk ingestion: <c>{"tables": {"TableName": [{...row}, ...]}}</c>,
    /// appended as one write set, one log record, not one transaction per row.
    /// </summary>
    public static async Task BulkAsync(HttpContext context, MelangeTransport transport)
    {
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

        try
        {
            var record = transport.Engine.BulkInsert(CallerOf(context), rows);
            await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
            {
                writer.WriteBoolean("ok", true);
                writer.WriteNumber("lsn", record?.Lsn ?? 0);
                writer.WriteNumber("rows", rows.Count);
            }).ConfigureAwait(false);
        }
        catch (ArgumentException exception)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, MelangeErrorCodes.InvalidArguments, exception.Message).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// POST {path}/sql — one-shot query: <c>{"query": "...", "params": {...}}</c>. Phase 03
    /// executes the same four shapes subscriptions support, under the same public-table rule and
    /// cost ceilings; aggregates land in phase 08.
    /// </summary>
    public static async Task SqlAsync(HttpContext context, MelangeTransport transport)
    {
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
            var parsed = SqlSubsetParser.Parse(query, parameters);
            var limits = transport.Options.Subscriptions;
            var (schema, projection, rows) = transport.Engine.ReadConsistent(head =>
            {
                var subscription = ServerSubscription.Compile(NullSink.Instance, 0, parsed, transport.Engine.Schema, limits);
                var collected = new List<KeyValuePair<RowKey, ReadOnlyMemory<byte>>>();
                long bytes = 0;
                foreach (var pair in subscription.MatchingRows(transport.Engine.HotStore))
                {
                    collected.Add(pair);
                    bytes += pair.Value.Length;
                    if (collected.Count > limits.MaxRowsPerSubscription || bytes > limits.MaxBytesPerSubscription)
                    {
                        throw new SubscriptionRejectedException(
                            MelangeErrorCodes.TooManyRows,
                            "The result exceeds the configured row or byte ceiling; narrow the predicate.");
                    }
                }

                return (subscription.Schema, subscription.Projection, collected);
            });

            var columns = parsed.Projection ?? [.. schema.Columns.Select(c => c.Name)];
            await WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
            {
                writer.WriteStartArray("columns");
                foreach (var column in columns)
                    writer.WriteStringValue(column);
                writer.WriteEndArray();
                writer.WriteStartArray("rows");
                foreach (var (_, rowBytes) in rows)
                {
                    var values = RowWire.ToColumns(schema, rowBytes.Span, projection);
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

    /// <summary>POST {path}/ticket — mints a single-use connect ticket. Redemption semantics land in phase 04.</summary>
    public static Task TicketAsync(HttpContext context, MelangeTransport transport)
    {
        var (ticket, expiresInSeconds) = transport.Tickets.Mint();
        return WriteJsonAsync(context, StatusCodes.Status200OK, writer =>
        {
            writer.WriteString("ticket", ticket);
            writer.WriteNumber("expiresInSeconds", expiresInSeconds);
        });
    }

    private static Identity CallerOf(HttpContext context)
    {
        string? token = null;
        var authorization = context.Request.Headers.Authorization.ToString();
        if (authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            token = authorization["Bearer ".Length..];
        return StubIdentity.FromToken(token);
    }

    /// <summary>
    /// JSON to argument mapping: numbers to long/ulong/double, plus <c>{"$identity": "hex"}</c>,
    /// <c>{"$bytes": "base64"}</c>, and <c>{"$timestamp": micros}</c> for the kinds JSON lacks.
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

    private static Task WriteErrorAsync(HttpContext context, int status, string code, string message) =>
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
}

/// <summary>Phase 03's identity stub: a stable hash of the presented token. Phase 04 replaces this with validated issuer+subject.</summary>
internal static class StubIdentity
{
    public static Identity FromToken(string? token) =>
        string.IsNullOrEmpty(token)
            ? Identity.Hash("melange-anonymous")
            : Identity.Hash("melange-stub-token:" + token);
}
