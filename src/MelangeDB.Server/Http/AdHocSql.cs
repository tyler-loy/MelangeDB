using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.AspNetCore.Http;

namespace MelangeDB.Server;

/// <summary>An ad-hoc query's answer: column names, and one value array per row in that order.</summary>
internal sealed record AdHocSqlResult(IReadOnlyList<string> Columns, IReadOnlyList<object?[]> Rows);

/// <summary>An ad-hoc query refused or failed, with the HTTP status and wire code the endpoint answers it with.</summary>
internal sealed class AdHocSqlRefusal(int status, string code, string message) : Exception(message)
{
    public int Status { get; } = status;

    public string Code { get; } = code;
}

/// <summary>
/// Ad-hoc SQL, independent of the HTTP request that usually carries it: the gates
/// (<c>Sql:AdHocEnabled</c>, owner mode and its role), the row path through a compiled
/// subscription under the caller's policies, and the aggregate path through the relational
/// backplane. <c>{path}/sql</c> and the MCP <c>query</c> tool both run it, so an agent gets exactly
/// the answer, and exactly the refusal, that the endpoint would give the same caller.
/// </summary>
internal static class AdHocSql
{
    /// <summary>The gates that precede reading the query at all; <see cref="ExecuteAsync"/> applies them too.</summary>
    public static void CheckAccess(MelangeTransport transport, AuthResult session)
    {
        var sqlOptions = transport.Options.Sql;
        if (!sqlOptions.AdHocEnabled)
        {
            throw new AdHocSqlRefusal(
                StatusCodes.Status403Forbidden, MelangeErrorCodes.SqlDisabled,
                "Ad-hoc SQL is disabled; set Sql:AdHocEnabled to true to opt in.");
        }

        if (sqlOptions.AdHocMode == AdHocSqlMode.Owner && !session.IsSqlOwner)
        {
            throw new AdHocSqlRefusal(
                StatusCodes.Status403Forbidden, MelangeErrorCodes.OwnerRequired,
                "Sql:AdHocMode is Owner and this caller's token carries no Sql:OwnerRole claim; owner mode is never granted implicitly.");
        }
    }

    /// <summary>Runs one query for <paramref name="session"/>. Every refusal is an <see cref="AdHocSqlRefusal"/>.</summary>
    public static async Task<AdHocSqlResult> ExecuteAsync(
        MelangeTransport transport,
        AuthResult session,
        string query,
        IReadOnlyDictionary<string, object?>? parameters,
        IServiceProvider services,
        CancellationToken ct)
    {
        CheckAccess(transport, session);
        var owner = transport.Options.Sql.AdHocMode == AdHocSqlMode.Owner;
        try
        {
            var adHoc = SqlSubsetParser.ParseAdHoc(query, parameters);
            return adHoc.Aggregate is { } aggregate
                ? await AggregateAsync(transport, aggregate, owner, services, ct).ConfigureAwait(false)
                : Rows(transport, session, adHoc.Rows!, owner);
        }
        catch (SqlParseException exception)
        {
            throw new AdHocSqlRefusal(StatusCodes.Status400BadRequest, MelangeErrorCodes.ParseError, exception.Message);
        }
        catch (SubscriptionRejectedException exception)
        {
            throw new AdHocSqlRefusal(StatusCodes.Status400BadRequest, exception.Code, exception.Message);
        }
    }

    private static AdHocSqlResult Rows(MelangeTransport transport, AuthResult session, SubscriptionQuery parsed, bool owner)
    {
        var clusterRole = transport.Options.Cluster.Role;
        var limits = transport.Options.Subscriptions;
        var enforced = !owner;
        var callerContext = enforced
            ? new PolicyContext(session.Identity, session.IsGuest, transport.Engine.CommittedView)
            : null;
        var (subscription, rows) = transport.Engine.ReadConsistent(head =>
        {
            var compiled = ServerSubscription.Compile(
                MelangeHttpEndpoints.NullSink.Instance, 0, parsed, transport.Engine.Schema, limits,
                enforced ? transport.Policies : null, callerContext,
                allowPrivateBackplane: owner);

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
        string[] columns = subscription.StaticWireColumns is { } wire
            ? [.. schema.Columns.Select(c => c.Name).Where(wire.Contains)]
            : [.. schema.Columns.Select(c => c.Name)];

        // A column-policy mask yields null for a hidden value; [ServerOnly] columns are absent
        // from the column list entirely.
        var values = rows.Select(row =>
        {
            var byName = RowWire.ToColumns(schema, row.Row.Span, row.Columns);
            return columns.Select(column => byName.GetValueOrDefault(column)).ToArray();
        }).ToList();
        return new AdHocSqlResult(columns, values);
    }

    /// <summary>
    /// The aggregate half: owner mode only, relational backplane only, executed by the registered
    /// <see cref="IRelationalQueryExecutor"/> — absent means no relational backplane is configured,
    /// which is an explicit error rather than an empty result. Results reflect the tier at its
    /// applier's checkpoint; that lag is the design, not a bug, and the applier's health check is
    /// where it becomes visible.
    /// </summary>
    private static async Task<AdHocSqlResult> AggregateAsync(
        MelangeTransport transport, AggregateQuery aggregate, bool owner, IServiceProvider services, CancellationToken ct)
    {
        if (!owner)
        {
            throw new AdHocSqlRefusal(
                StatusCodes.Status403Forbidden, MelangeErrorCodes.OwnerRequired,
                "Aggregates run in owner mode only: row policies are in-process code that cannot be pushed into Postgres, " +
                "and a policy-enforced aggregate would silently drop enforcement. Set Sql:AdHocMode to Owner.");
        }

        var built = AdHocAggregateBuilder.Build(aggregate, transport.Engine.Schema);

        // Placement and tier are independent axes, so closing the empty-result lie for row shapes
        // (#114) did not close it here: a Partitioned table can also be Relational, and this node's
        // relational backplane holds only what this node's engine wrote. On a hub that is every shard's
        // rows except the partitioned ones — the aggregate would answer a confident number over a
        // subset, which is worse than the empty result the row path refuses. Checked after Build
        // for the same reason the row path checks after Compile: Build answers unknown and
        // wrong-tier tables first, so this refusal is never an existence oracle.
        var clusterRole = transport.Options.Cluster.Role;
        if (clusterRole != ClusterRole.None && built.Table.Placement == Placement.Partitioned)
        {
            throw new AdHocSqlRefusal(
                StatusCodes.Status400BadRequest, MelangeErrorCodes.PartitionedElsewhere,
                $"Table '{built.Table.Name}' is Placement.Partitioned, and its rows live in the shard engines rather "
                + $"than in this node's (Cluster:Role is {clusterRole}). An aggregate here would count only what this "
                + "node wrote and report it as the total.");
        }

        if (services.GetService(typeof(IRelationalQueryExecutor)) is not IRelationalQueryExecutor executor)
        {
            throw new AdHocSqlRefusal(
                StatusCodes.Status400BadRequest, MelangeErrorCodes.NoRelationalTier,
                "No relational backplane is configured; aggregates need AddPostgres(...).");
        }

        RelationalQueryResult result;
        try
        {
            result = await executor.ExecuteAsync(built, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            throw new AdHocSqlRefusal(
                StatusCodes.Status503ServiceUnavailable, MelangeErrorCodes.RelationalUnavailable,
                "The relational backplane did not answer; see the server logs. Writes and subscriptions are unaffected.");
        }

        return new AdHocSqlResult(result.Columns, [.. result.Rows.Select(static row => row.ToArray())]);
    }

    /// <summary>The endpoint's success body: <c>{"columns":[...],"rows":[[...],...]}</c>, without the enclosing object.</summary>
    public static void WriteResult(Utf8JsonWriter writer, AdHocSqlResult result)
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
                MelangeHttpEndpoints.WriteJsonValue(writer, value);
            writer.WriteEndArray();
        }

        writer.WriteEndArray();
    }
}
