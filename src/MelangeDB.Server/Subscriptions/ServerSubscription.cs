using System.Globalization;
using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>Receives delta frames computed under the engine's write lock. Implementations must not block.</summary>
internal interface IDeltaSink
{
    /// <summary>Queues one transaction's deltas for this connection. Called in LSN order.</summary>
    void EnqueueDelta(TransactionUpdateFrame frame);
}

/// <summary>Thrown when a subscription fails validation or its estimated cost exceeds a ceiling.</summary>
internal sealed class SubscriptionRejectedException : Exception
{
    public SubscriptionRejectedException(string code, string message)
        : base(message)
        => Code = code;

    /// <summary>A <see cref="MelangeErrorCodes"/> value; doubles as the rejection metric's reason dimension.</summary>
    public string Code { get; }
}

/// <summary>
/// One registered subscription: the compiled predicate tested against row ops on the fan-out path,
/// the column projection its deltas are masked to, and the caller's policy context. Registration
/// state is mutated only under the engine's write lock. Row visibility is predicate AND policy
/// (policies union among themselves); wire columns are projection ∩ non-<c>[ServerOnly]</c> ∩
/// every column mask — rows union, columns intersect.
/// </summary>
internal sealed class ServerSubscription
{
    private ServerSubscription(IDeltaSink sink, uint id, TableSchema schema, TablePolicyEvaluator? evaluator, PolicyContext? context)
    {
        Sink = sink;
        Id = id;
        Schema = schema;
        Evaluator = evaluator;
        Context = context;
    }

    public IDeltaSink Sink { get; }

    public uint Id { get; }

    public TableSchema Schema { get; }

    /// <summary>The table's row and column policies, or null when it has none.</summary>
    public TablePolicyEvaluator? Evaluator { get; }

    /// <summary>
    /// The caller this subscription filters for; null runs no policies (the owner-mode SQL path).
    /// Replaced on re-authentication so a guest conversion updates <c>IsGuest</c> in place.
    /// </summary>
    public PolicyContext? Context { get; set; }

    /// <summary>
    /// The columns this subscription puts on the wire before per-row masks: the client's
    /// projection, else all columns minus <c>[ServerOnly]</c>; null means every column. Computed
    /// at compile time so subscriptions without column policies pay nothing per row.
    /// </summary>
    public HashSet<string>? StaticWireColumns { get; private set; }

    /// <summary>
    /// The shape every row this subscription sends is encoded in — the ordered, kinded columns of
    /// <see cref="StaticWireColumns"/>. Sent once on the first initial-set chunk and stable for the
    /// subscription's life: re-scoping cannot change a projection, and a schema change means a new
    /// epoch and a fresh subscribe.
    /// </summary>
    public WireDescriptor Descriptor { get; private set; } = null!;

    public HashSet<string>? Projection { get; private set; }

    public PredicateKind Predicate { get; private set; }

    public string? Column { get; private set; }

    public bool ColumnIsPrimaryKey { get; private set; }

    public RowKey EqualsValue { get; private set; }

    /// <summary>
    /// The inclusive lower bound of a <see cref="PredicateKind.Range"/>, and also of a
    /// <see cref="PredicateKind.NotDefault"/> — which compiles to the range that excludes the
    /// default's bucket, so both shapes are served by one comparison and one index walk.
    /// </summary>
    public RowKey RangeLow { get; private set; }

    /// <summary>The inclusive upper bound; see <see cref="RangeLow"/>.</summary>
    public RowKey RangeHigh { get; private set; }

    /// <summary>
    /// Compiles and validates a query against the schema and the configured cost limits.
    /// <paramref name="context"/> is the caller policies filter for; null (with a null
    /// <paramref name="policies"/>) is the deliberate owner-mode bypass — no policy can make a
    /// private table visible either way, since a private table never compiles.
    /// <paramref name="allowPrivateRelational"/> is owner-mode ad-hoc SQL's one extra visibility:
    /// private <em>relational-tier</em> tables compile, because that tier exists for tooling and
    /// its tables (statistics, history) are private by default. Private hot tables stay
    /// server-internal in every mode, and no subscription ever passes true here.
    /// </summary>
    public static ServerSubscription Compile(
        IDeltaSink sink,
        uint id,
        SubscriptionQuery query,
        SchemaRegistry registry,
        SubscriptionsOptions limits,
        PolicySet? policies = null,
        PolicyContext? context = null,
        bool allowPrivateRelational = false)
    {
        if (!registry.TryGetByName(query.Table, out var schema)
            || !(schema.IsPublic || (allowPrivateRelational && schema.Tier == StorageTier.Relational)))
        {
            // One message for unknown and private: a subscription cannot name a private table, and
            // the error must not reveal whether the name exists server-side.
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.UnknownTable,
                $"No public table named '{query.Table}' is subscribable.");
        }

        var subscription = new ServerSubscription(sink, id, schema, policies?.For(schema.Id), context);
        subscription.ApplyProjection(query);
        subscription.ApplyPredicate(query, limits);
        subscription.ComputeWireShape();
        return subscription;
    }

    /// <summary>
    /// Re-scopes this subscription to a new predicate — the moving-range pattern. The table and
    /// projection must not change; deltas for the diff are the caller's to emit.
    /// </summary>
    public void Rescope(SubscriptionQuery query, SubscriptionsOptions limits)
    {
        if (!string.Equals(query.Table, Schema.Name, StringComparison.Ordinal))
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.ParseError,
                "Re-scoping a subscription cannot change its table; unsubscribe and subscribe instead.");
        }

        var sameProjection = (query.Projection is null && Projection is null)
            || (query.Projection is not null && Projection is not null && Projection.SetEquals(query.Projection));
        if (!sameProjection)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.ParseError,
                "Re-scoping a subscription cannot change its projection; unsubscribe and subscribe instead.");
        }

        ApplyPredicate(query, limits);
    }

    /// <summary>
    /// Adopts an already-registered subscription's <see cref="StaticWireColumns"/> when it holds
    /// the same names, so that equal projections on a table converge on one set instance.
    /// <para>
    /// Purely an identity change — the contents are equal either way. It exists so the fan-out's
    /// wire-column memo can key on reference identity instead of comparing sets per row per
    /// subscriber: two hundred players running the same client ask for the same columns, and
    /// without this they would each hold a private, equal, un-poolable set. Registration is rare
    /// and already scans the table for an initial set, so the peer walk is free by comparison.
    /// </para>
    /// </summary>
    internal void ShareWireColumns(IReadOnlyList<ServerSubscription> peers)
    {
        // Null is "every column" — already a shared sentinel, and nothing to converge.
        if (StaticWireColumns is not { } mine)
            return;

        foreach (var peer in peers)
        {
            if (peer.StaticWireColumns is { } theirs && !ReferenceEquals(theirs, mine) && theirs.SetEquals(mine))
            {
                StaticWireColumns = theirs;
                return;
            }
        }
    }

    /// <summary>
    /// Whether the caller's row policies admit <paramref name="row"/> — union: any policy is
    /// enough, and a table with no row policies is fully visible (it compiled, so it is public).
    /// True with no <see cref="Context"/>: that is the owner-mode bypass.
    /// </summary>
    public bool PolicyAdmits(ReadOnlySpan<byte> row) =>
        Evaluator is null || Context is null || !Evaluator.HasRowPolicies || Evaluator.IsRowVisible(row, Context);

    /// <summary><see cref="PolicyAdmits(ReadOnlySpan{byte})"/> over a row decoded once for every subscriber.</summary>
    public bool PolicyAdmits(DecodedRow row) =>
        Evaluator is null || Context is null || !Evaluator.HasRowPolicies || Evaluator.IsRowVisible(row, Context);

    /// <summary>Predicate AND policy — whether the caller receives this row at all.</summary>
    public bool RowVisible(in RowKey key, ReadOnlySpan<byte> row) =>
        Matches(key, row) && PolicyAdmits(row);

    /// <summary>
    /// The columns of <paramref name="row"/> the caller receives: the static wire set intersected
    /// with every column mask. Null means all columns. Callers on the fan-out path evaluate this
    /// under the engine's write lock, so masks read committed state.
    /// </summary>
    public IReadOnlySet<string>? VisibleColumns(ReadOnlySpan<byte> row)
    {
        if (Evaluator is not { HasColumnPolicies: true } evaluator || Context is null)
            return StaticWireColumns;
        var visible = FreshWireColumns();
        evaluator.IntersectColumns(row, Context, visible);
        return visible;
    }

    /// <summary><see cref="VisibleColumns(ReadOnlySpan{byte})"/> over a row decoded once for every subscriber.</summary>
    public IReadOnlySet<string>? VisibleColumns(DecodedRow row)
    {
        if (Evaluator is not { HasColumnPolicies: true } evaluator || Context is null)
            return StaticWireColumns;
        var visible = FreshWireColumns();
        evaluator.IntersectColumns(row, Context, visible);
        return visible;
    }

    private HashSet<string> FreshWireColumns() =>
        StaticWireColumns is not null
            ? new HashSet<string>(StaticWireColumns, StringComparer.Ordinal)
            : new HashSet<string>(Schema.Columns.Select(c => c.Name), StringComparer.Ordinal);

    /// <summary>
    /// The mask that accompanies a row whose visible columns are <paramref name="visible"/>: empty
    /// unless a column policy narrowed them for this row specifically. Reference equality with
    /// <see cref="StaticWireColumns"/> is the test, and it is exact — <see cref="VisibleColumns"/>
    /// returns that very instance when no column policy applies, and a fresh set when one does.
    /// </summary>
    public ReadOnlyMemory<byte> MaskFor(IReadOnlySet<string>? visible) =>
        ReferenceEquals(visible, StaticWireColumns) ? default : RowWire.Mask(Descriptor.Columns, visible!);

    /// <summary>
    /// One row's wire form for this subscription: descriptor-shaped bytes and its column mask. The
    /// fan-out path does not use this — it shares one projection across every subscriber through
    /// the memo — but the cold paths (initial sets, re-scope diffs, resume replay) do.
    /// </summary>
    public (ReadOnlyMemory<byte> Row, ReadOnlyMemory<byte> Mask) WireForm(ReadOnlyMemory<byte> row)
    {
        var visible = VisibleColumns(row.Span);
        return (RowWire.Project(Schema, row, visible), MaskFor(visible));
    }

    /// <summary>Whether a row belongs to this subscription. <paramref name="key"/> is the primary key.</summary>
    public bool Matches(in RowKey key, ReadOnlySpan<byte> row) =>
        Predicate == PredicateKind.None
        || Decide(ColumnIsPrimaryKey ? key : RowWire.EncodeColumn(Schema, Column!, row));

    /// <summary>
    /// <see cref="Matches(in RowKey, ReadOnlySpan{byte})"/> over a row decoded once for every
    /// subscriber — the fan-out's form. A predicate on an indexed column used to decode the whole
    /// row per subscriber to encode one column of it; the memo encodes it once per op.
    /// </summary>
    public bool Matches(in RowKey key, DecodedRow row) =>
        Predicate == PredicateKind.None
        || Decide(ColumnIsPrimaryKey ? key : row.EncodeColumn(Column!));

    /// <summary>The predicate's verdict on the encoded column value; a null value matches nothing.</summary>
    private bool Decide(RowKey? candidate)
    {
        if (candidate is not { } value)
            return false;
        return Predicate switch
        {
            PredicateKind.Equality => value == EqualsValue,
            PredicateKind.Range or PredicateKind.NotDefault => value.CompareTo(RangeLow) >= 0 && value.CompareTo(RangeHigh) <= 0,
            _ => false,
        };
    }

    /// <summary>Enumerates the store rows this subscription currently matches, in a deterministic order.</summary>
    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> MatchingRows(IHotStore store)
    {
        switch (Predicate)
        {
            case PredicateKind.None:
                return store.Scan(Schema.Id);
            case PredicateKind.Equality when ColumnIsPrimaryKey:
                return store.TryGetRow(Schema.Id, EqualsValue, out var row)
                    ? [new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(EqualsValue, row)]
                    : [];
            case PredicateKind.Equality:
                return store.ScanIndex(Schema.Id, Column!, EqualsValue);
            case PredicateKind.Range or PredicateKind.NotDefault when ColumnIsPrimaryKey:
                return ScanPrimaryKeyRange(store);
            case PredicateKind.Range or PredicateKind.NotDefault:
                return store.ScanIndexRange(Schema.Id, Column!, RangeLow, RangeHigh);
            default:
                return [];
        }
    }

    /// <summary>
    /// Seeks the key directory to the range, and materializes only what falls inside it.
    ///
    /// <para>Two fixes, and the second is the one that finished the job. This used to filter
    /// <see cref="IHotStore.Scan"/>, which reads every row it passes — so a range near the end of a
    /// paged table paged in the whole table ahead of it and threw all of it away. The keys are
    /// ordered, so the rows below <see cref="RangeLow"/> were never candidates and never needed
    /// reading; <see cref="IHotStore.ScanKeys"/> touches no buffer pool. On a 24k-row table of 9KB
    /// blobs that was the difference between ~3s and ~5ms per subscribe.</para>
    ///
    /// <para>But it still <em>walked</em> to the window, one key at a time, from key zero — the
    /// cost merely fell from "read every row before the window" to "compare every key before it",
    /// and it still grew with the table and with how far in the window sat. On a 1.4M-row terrain
    /// table with clients holding a ring of nineteen windows, re-scoped on every chunk crossing,
    /// that walk ran under the engine's write lock and took the whole deployment's write throughput
    /// with it. <see cref="IHotStore.ScanKeyRange"/> asks the store to seek instead, so the cost is
    /// the size of the window rather than the distance to it.</para>
    /// </summary>
    private IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanPrimaryKeyRange(IHotStore store)
    {
        foreach (var key in store.ScanKeyRange(Schema.Id, RangeLow, RangeHigh))
        {
            if (store.TryGetRow(Schema.Id, key, out var row))
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, row);
        }
    }

    private void ApplyProjection(SubscriptionQuery query)
    {
        if (query.Projection is null)
            return;
        var projection = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in query.Projection)
        {
            var column = Schema.Columns.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.Ordinal));
            if (column is null)
            {
                throw new SubscriptionRejectedException(
                    MelangeErrorCodes.UnknownColumn,
                    $"Table '{Schema.Name}' has no column '{name}'.");
            }

            // An explicit request for a [ServerOnly] column is an error, never a silently empty
            // field — a null a client can misread as "no value" would hide the policy.
            if (column.IsServerOnly)
            {
                throw new SubscriptionRejectedException(
                    MelangeErrorCodes.ServerOnlyColumn,
                    $"Table '{Schema.Name}': column '{name}' is [ServerOnly] and never leaves the server.");
            }

            projection.Add(name);
        }

        Projection = projection;
    }

    /// <summary>
    /// Precomputes the wire column set and the descriptor derived from it. The set is the
    /// projection (already validated ServerOnly-free), else all columns minus <c>[ServerOnly]</c>,
    /// else null meaning "all" — so subscriptions on tables with nothing to hide pay nothing per
    /// row, and their rows go out as the store's own bytes.
    /// </summary>
    private void ComputeWireShape()
    {
        StaticWireColumns = Projection ?? (Schema.Columns.Any(c => c.IsServerOnly)
            ? new HashSet<string>(Schema.Columns.Where(c => !c.IsServerOnly).Select(c => c.Name), StringComparer.Ordinal)
            : null);

        var columns = new List<WireColumn>(Schema.Columns.Count);
        foreach (var column in Schema.Columns)
        {
            if (StaticWireColumns is null || StaticWireColumns.Contains(column.Name))
                columns.Add(new WireColumn(column.Name, column.Kind));
        }

        Descriptor = new WireDescriptor(Schema.Name, columns);
    }

    private void ApplyPredicate(SubscriptionQuery query, SubscriptionsOptions limits)
    {
        RequirePredicateRule(query, limits);
        if (query.Predicate == PredicateKind.None)
        {
            Predicate = PredicateKind.None;
            Column = null;
            return;
        }

        var column = Schema.Column(query.Column!);

        // A predicate on a [ServerOnly] column would leak its values through hit-versus-miss —
        // membership is information too.
        if (column.IsServerOnly)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.ServerOnlyColumn,
                $"Table '{Schema.Name}': column '{column.Name}' is [ServerOnly] and cannot appear in a predicate.");
        }

        if (column is { IsPrimaryKey: false, IsIndexed: false, IsUnique: false })
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.UnindexedColumn,
                $"Table '{Schema.Name}': column '{column.Name}' is not indexed; a subscription predicate needs [Index], [Unique], or the primary key.");
        }

        Column = column.Name;
        ColumnIsPrimaryKey = column.IsPrimaryKey;
        Predicate = query.Predicate;
        if (query.Predicate == PredicateKind.Equality)
        {
            EqualsValue = EncodeOperand(column, query.EqualsValue);
            return;
        }

        if (query.Predicate == PredicateKind.NotDefault)
        {
            ApplyNotDefault(column, query);
            return;
        }

        CheckRangeSpan(column, query, limits);
        RangeLow = EncodeOperand(column, query.RangeLow);
        RangeHigh = EncodeOperand(column, query.RangeHigh);
    }

    /// <summary>
    /// Compiles <c>col &lt;&gt; &lt;default&gt;</c> into the index range that excludes the default's
    /// bucket. One range, never two, because the supported kinds are exactly those whose default is
    /// also their minimum — which is what lets this shape skip <see cref="CheckRangeSpan"/>
    /// honestly. It is an index walk that steps over one value, not an unbounded window somebody
    /// clamped to get past the parser, and the row and byte ceilings bound it like anything else.
    /// <para>
    /// Both refusals are <see cref="MelangeErrorCodes.UnsupportedPredicate"/> rather than parse
    /// errors: the text was valid SQL, and the caller needs to hear that the shape is the problem.
    /// </para>
    /// </summary>
    private void ApplyNotDefault(ColumnSchema column, SubscriptionQuery query)
    {
        if (NotDefaultOperands(column.Kind) is not { } operands)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.UnsupportedPredicate,
                $"Table '{Schema.Name}': '<>' on column '{column.Name}' of kind {column.Kind} is not supported. "
                + "The shape serves Bool and unsigned integer columns, whose default is also their minimum, so "
                + "'not the default' is a single index range; a signed column would need two ranges and a string "
                + "or byte column has no upper bound to scan to.");
        }

        // The operand has to *be* the default, which is a stricter question than "coerces to it".
        // The write path's coercion is deliberately forgiving — it rounds, and it reads a missing
        // value as the default — and borrowing it here would quietly widen the shape: `<> 0.5` and
        // a null parameter both coerce to 0, and both would compile as "not the default", naming a
        // predicate the caller did not write. So: exact, or refused. One refusal still covers a
        // bad operand and a wrong-but-valid one, and says the same useful thing about both.
        if (!NamesDefaultExactly(query.EqualsValue, operands.Default))
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.UnsupportedPredicate,
                $"Table '{Schema.Name}': '<>' on column '{column.Name}' compares against the column's default "
                + $"({(operands.Default is bool ? "false" : "0")}) only, written exactly — the rows where it has "
                + "been set at all. An arbitrary inequality has no index affinity and would be a table scan "
                + "wearing a predicate, and a null or fractional operand names no value at all.");
        }

        RangeLow = SchemaKeyCodec.Encode(column, operands.Low);
        RangeHigh = SchemaKeyCodec.Encode(column, operands.High);
    }

    /// <summary>
    /// For a kind that supports <c>&lt;&gt;</c>: its default, the value one step above that default,
    /// and its maximum — the three operands the compiled range needs. Null for the kinds this first
    /// cut refuses. Signed integers are excluded because zero is not their minimum, so "not zero"
    /// is two ranges rather than one; <c>String</c>, <c>Bytes</c>, <c>Identity</c> and
    /// <c>Timestamp</c> because they have no maximum worth scanning to.
    /// <para>
    /// The bounds are built by handing these to <see cref="SchemaKeyCodec"/> rather than by
    /// composing key bytes here. A hand-rolled bound that is one value off produces a silently
    /// short scan — right shape, missing rows, no error — and the only defence that scales is to
    /// keep every byte decision in the one codec both the server and the client already run.
    /// </para>
    /// </summary>
    /// <summary>
    /// Whether the operand as written names the column's default and nothing else. Numbers are
    /// judged by value rather than by CLR type, because a literal arrives as <c>long</c> and a
    /// JSON parameter as whatever the document held — but a value that merely <em>rounds</em> to
    /// the default does not name it, and neither does an absent one. Null is refused rather than
    /// read as the default: an unbound parameter is a caller's mistake, and compiling it into the
    /// one predicate that happens to want zero would hide it.
    /// </summary>
    private static bool NamesDefaultExactly(object? operand, object @default) => (operand, @default) switch
    {
        (null, _) => false,
        (bool value, bool expected) => value == expected,
        (_, bool) => false,
        (sbyte or byte or short or ushort or int or uint or long, _) =>
            Convert.ToInt64(operand, CultureInfo.InvariantCulture) == 0,
        (ulong value, _) => value == 0,
        (double value, _) => value == 0,
        (float value, _) => value == 0,
        (decimal value, _) => value == 0,
        _ => false,
    };

    private static (object Default, object Low, object High)? NotDefaultOperands(ColumnKind kind) => kind switch
    {
        ColumnKind.Bool => (false, true, true),
        ColumnKind.UInt8 => ((byte)0, (byte)1, byte.MaxValue),
        ColumnKind.UInt16 => ((ushort)0, (ushort)1, ushort.MaxValue),
        ColumnKind.UInt32 => (0u, 1u, uint.MaxValue),
        ColumnKind.UInt64 => (0ul, 1ul, ulong.MaxValue),
        _ => null,
    };

    private void RequirePredicateRule(SubscriptionQuery query, SubscriptionsOptions limits)
    {
        foreach (var entry in limits.RequirePredicateOn)
        {
            var separator = entry.IndexOf('.');
            var table = separator < 0 ? entry : entry[..separator];
            if (!string.Equals(table, Schema.Name, StringComparison.Ordinal))
                continue;

            var requiredColumn = separator < 0 ? null : entry[(separator + 1)..];
            var satisfied = query.Predicate != PredicateKind.None
                && (requiredColumn is null || string.Equals(query.Column, requiredColumn, StringComparison.Ordinal));
            if (!satisfied)
            {
                var requirement = requiredColumn is null ? "a predicate" : $"a predicate on column '{requiredColumn}'";
                throw new SubscriptionRejectedException(
                    MelangeErrorCodes.PredicateRequired,
                    $"Table '{Schema.Name}' requires {requirement} (Subscriptions:RequirePredicateOn); an unbounded subscription is rejected before any rows are read.");
            }
        }
    }

    private void CheckRangeSpan(ColumnSchema column, SubscriptionQuery query, SubscriptionsOptions limits)
    {
        // Span is only computable for integer kinds; row and byte ceilings still bound the rest.
        var span = column.Kind switch
        {
            ColumnKind.Int8 or ColumnKind.Int16 or ColumnKind.Int32 or ColumnKind.Int64 =>
                SafeSpan(() => checked(ToInt64(query.RangeHigh) - ToInt64(query.RangeLow))),
            ColumnKind.UInt8 or ColumnKind.UInt16 or ColumnKind.UInt32 or ColumnKind.UInt64 =>
                SafeSpan(() =>
                {
                    var high = ToUInt64(query.RangeHigh);
                    var low = ToUInt64(query.RangeLow);
                    return high < low ? 0L : checked((long)(high - low));
                }),
            _ => (long?)null,
        };

        if (span is { } width && width > limits.MaxRangeSpan)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.RangeTooWide,
                $"Range of width {width} on '{Schema.Name}.{column.Name}' exceeds Subscriptions:MaxRangeSpan ({limits.MaxRangeSpan}). Stream a ring around the player, not the map.");
        }
    }

    private static long? SafeSpan(Func<long> compute)
    {
        try
        {
            return compute();
        }
        catch (OverflowException)
        {
            return long.MaxValue;
        }
    }

    private RowKey EncodeOperand(ColumnSchema column, object? value)
    {
        var coerced = RowSerializer.CoerceValue(Schema, column, value)
            ?? throw new SubscriptionRejectedException(
                MelangeErrorCodes.ParseError,
                $"A predicate value for '{Schema.Name}.{column.Name}' cannot be null.");
        return SchemaKeyCodec.Encode(column, coerced);
    }

    private static long ToInt64(object? value) => Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);

    private static ulong ToUInt64(object? value) => value switch
    {
        ulong unsigned => unsigned,
        _ => Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture) is var signed && signed < 0
            ? 0UL
            : (ulong)Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture),
    };
}
