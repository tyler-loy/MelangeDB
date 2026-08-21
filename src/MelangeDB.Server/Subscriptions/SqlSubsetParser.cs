using MelangeDB.Core;

namespace MelangeDB.Server;

/// <summary>The predicate shapes MelangeDB SQL supports.</summary>
internal enum PredicateKind
{
    None,
    Equality,
    Range,

    /// <summary>
    /// <c>col &lt;&gt; &lt;default&gt;</c> — the sparse subset of a column that has been set at all.
    /// The operand must be the column's own default, which is what keeps this an index scan that
    /// steps over one value rather than an arbitrary inequality with no index affinity. Bounded by
    /// the row and byte ceilings like any other subscription, and deliberately not by
    /// <c>MaxRangeSpan</c>: a counter has no span, and clamping one to invent a span is the lie
    /// this shape exists to make unnecessary (issue #122).
    /// </summary>
    NotDefault,
}

/// <summary>
/// One parsed subscription (or ad-hoc row) query: exactly the supported shapes, operands already
/// resolved from named parameters or literals. Schema validation happens later, against the
/// registry, so parse errors and semantic errors stay distinguishable.
/// </summary>
/// <param name="EqualsValue">
/// The single-operand comparison value: <c>=</c>'s right-hand side, or <c>&lt;&gt;</c>'s — which
/// the compiler then requires to be the column's default. One field, because the parser's job is
/// to read the operand, not to judge it.
/// </param>
internal sealed record SubscriptionQuery(
    string Table,
    IReadOnlyList<string>? Projection,
    PredicateKind Predicate,
    string? Column,
    object? EqualsValue,
    object? RangeLow,
    object? RangeHigh);

/// <summary>One item of an aggregate query's select or group-by list.</summary>
internal sealed record AggregateItem(
    RelationalSelectionKind Kind,
    string? Column,
    AggregateFunction? Function,
    TimeBucketUnit? Bucket);

/// <summary>
/// One parsed ad-hoc aggregate query: select items (columns, <c>DATE_TRUNC</c> buckets,
/// aggregates), an optional predicate of the same two shapes row queries have, and the group-by
/// list. Aggregates are one-shot only — a subscription can never carry one.
/// </summary>
internal sealed record AggregateQuery(
    string Table,
    IReadOnlyList<AggregateItem> Items,
    IReadOnlyList<AggregateItem> GroupBy,
    PredicateKind Predicate,
    string? Column,
    object? EqualsValue,
    object? RangeLow,
    object? RangeHigh);

/// <summary>An ad-hoc parse result: exactly one of a row-shape query or an aggregate query.</summary>
internal sealed record AdHocQuery(SubscriptionQuery? Rows, AggregateQuery? Aggregate);

/// <summary>Thrown for text that is not valid MelangeDB SQL. The message names what was expected.</summary>
internal sealed class SqlParseException : Exception
{
    public SqlParseException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The hand-rolled parser for the MelangeDB SQL subset. The row shapes are precisely five:
/// <c>SELECT * FROM t</c>, <c>SELECT * FROM t WHERE col = :p</c>,
/// <c>SELECT * FROM t WHERE col BETWEEN :lo AND :hi</c>,
/// <c>SELECT * FROM t WHERE col &lt;&gt; 0</c>, and any of those with an explicit column
/// list. Ad-hoc SQL additionally parses aggregates — <c>COUNT(*)</c>, <c>COUNT/SUM/AVG/MIN/MAX(col)</c>,
/// <c>DATE_TRUNC('hour', col)</c> bucketing, and <c>GROUP BY</c>. A subset this small is a
/// feature: "valid MelangeDB SQL" stays unambiguous, every client language can target it, and no
/// client text ever reaches Postgres unparsed.
/// </summary>
internal static class SqlSubsetParser
{
    /// <summary>Parses a subscription query — the row shapes only; aggregates are rejected.</summary>
    public static SubscriptionQuery Parse(string query, IReadOnlyDictionary<string, object?>? parameters)
    {
        var parsed = ParseAdHoc(query, parameters);
        if (parsed.Rows is null)
            throw new SqlParseException("Aggregates and GROUP BY are one-shot ad-hoc SQL; a subscription cannot carry them.");
        return parsed.Rows;
    }

    /// <summary>Parses an ad-hoc query: one of the row shapes, or an aggregate query.</summary>
    public static AdHocQuery ParseAdHoc(string query, IReadOnlyDictionary<string, object?>? parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        var tokens = new Tokenizer(query);
        tokens.ExpectKeyword("SELECT");

        var star = false;
        List<AggregateItem> items = [];
        if (tokens.TryConsumeSymbol('*'))
        {
            star = true;
        }
        else
        {
            items.Add(ParseSelectItem(ref tokens));
            while (tokens.TryConsumeSymbol(','))
                items.Add(ParseSelectItem(ref tokens));
        }

        tokens.ExpectKeyword("FROM");
        var table = tokens.ExpectIdentifier("a table name");

        var predicate = PredicateKind.None;
        string? column = null;
        object? equalsValue = null;
        object? rangeLow = null;
        object? rangeHigh = null;

        if (tokens.TryConsumeKeyword("WHERE"))
        {
            column = tokens.ExpectIdentifier("a column name");
            if (tokens.TryConsumeSymbol('='))
            {
                predicate = PredicateKind.Equality;
                equalsValue = tokens.ExpectOperand(parameters);
            }
            else if (tokens.TryConsumeNotEquals())
            {
                predicate = PredicateKind.NotDefault;
                equalsValue = tokens.ExpectOperand(parameters);
            }
            else if (tokens.TryConsumeKeyword("BETWEEN"))
            {
                predicate = PredicateKind.Range;
                rangeLow = tokens.ExpectOperand(parameters);
                tokens.ExpectKeyword("AND");
                rangeHigh = tokens.ExpectOperand(parameters);
            }
            else
            {
                throw new SqlParseException("Expected '=', '<>', or BETWEEN after the WHERE column.");
            }
        }

        List<AggregateItem>? groupBy = null;
        if (tokens.TryConsumeKeyword("GROUP"))
        {
            tokens.ExpectKeyword("BY");
            groupBy = [ParseGroupItem(ref tokens)];
            while (tokens.TryConsumeSymbol(','))
                groupBy.Add(ParseGroupItem(ref tokens));
        }

        tokens.ExpectEnd();

        var isAggregate = groupBy is not null || items.Any(i => i.Kind != RelationalSelectionKind.Column);
        if (!isAggregate)
        {
            return new AdHocQuery(
                new SubscriptionQuery(
                    table,
                    star ? null : items.Select(i => i.Column!).ToList(),
                    predicate, column, equalsValue, rangeLow, rangeHigh),
                Aggregate: null);
        }

        if (star)
            throw new SqlParseException("An aggregate query selects aggregates, columns, or DATE_TRUNC buckets, not '*'.");
        return new AdHocQuery(
            Rows: null,
            new AggregateQuery(table, items, groupBy ?? [], predicate, column, equalsValue, rangeLow, rangeHigh));
    }

    private static AggregateItem ParseSelectItem(ref Tokenizer tokens)
    {
        var name = tokens.ExpectIdentifier("a column name, aggregate, or DATE_TRUNC");
        if (!tokens.TryConsumeSymbol('('))
            return new AggregateItem(RelationalSelectionKind.Column, name, Function: null, Bucket: null);

        if (name.Equals("DATE_TRUNC", StringComparison.OrdinalIgnoreCase))
        {
            var item = ParseDateTruncArguments(ref tokens);
            tokens.ExpectSymbol(')');
            return item;
        }

        AggregateFunction function = name.ToUpperInvariant() switch
        {
            "COUNT" => AggregateFunction.Count,
            "SUM" => AggregateFunction.Sum,
            "AVG" => AggregateFunction.Avg,
            "MIN" => AggregateFunction.Min,
            "MAX" => AggregateFunction.Max,
            _ => throw new SqlParseException($"Unknown function '{name}'; supported: COUNT, SUM, AVG, MIN, MAX, DATE_TRUNC."),
        };

        string? argument = null;
        if (tokens.TryConsumeSymbol('*'))
        {
            if (function != AggregateFunction.Count)
                throw new SqlParseException($"{name.ToUpperInvariant()}(*) is not valid; only COUNT takes '*'.");
        }
        else
        {
            argument = tokens.ExpectIdentifier("a column name");
        }

        tokens.ExpectSymbol(')');
        return new AggregateItem(RelationalSelectionKind.Aggregate, argument, function, Bucket: null);
    }

    private static AggregateItem ParseGroupItem(ref Tokenizer tokens)
    {
        var name = tokens.ExpectIdentifier("a column name or DATE_TRUNC");
        if (!tokens.TryConsumeSymbol('('))
            return new AggregateItem(RelationalSelectionKind.Column, name, Function: null, Bucket: null);
        if (!name.Equals("DATE_TRUNC", StringComparison.OrdinalIgnoreCase))
            throw new SqlParseException("GROUP BY accepts a column name or DATE_TRUNC('unit', column) only.");
        var item = ParseDateTruncArguments(ref tokens);
        tokens.ExpectSymbol(')');
        return item;
    }

    private static AggregateItem ParseDateTruncArguments(ref Tokenizer tokens)
    {
        var unitLiteral = tokens.ExpectStringLiteral("a DATE_TRUNC unit ('minute', 'hour', 'day', 'week', 'month', 'year')");
        if (!Enum.TryParse<TimeBucketUnit>(unitLiteral, ignoreCase: true, out var unit))
            throw new SqlParseException($"Unknown DATE_TRUNC unit '{unitLiteral}'; supported: minute, hour, day, week, month, year.");
        tokens.ExpectSymbol(',');
        var column = tokens.ExpectIdentifier("a timestamp column name");
        return new AggregateItem(RelationalSelectionKind.Bucket, column, Function: null, unit);
    }

    private ref struct Tokenizer(string text)
    {
        private readonly string _text = text;
        private int _position;

        public void ExpectKeyword(string keyword)
        {
            if (!TryConsumeKeyword(keyword))
                throw new SqlParseException($"Expected {keyword} at position {_position}.");
        }

        public bool TryConsumeKeyword(string keyword)
        {
            SkipWhitespace();
            var end = _position + keyword.Length;
            if (end > _text.Length)
                return false;
            if (!_text.AsSpan(_position, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
                return false;
            if (end < _text.Length && IsIdentifierChar(_text[end]))
                return false;
            _position = end;
            return true;
        }

        public bool TryConsumeSymbol(char symbol)
        {
            SkipWhitespace();
            if (_position >= _text.Length || _text[_position] != symbol)
                return false;
            _position++;
            return true;
        }

        /// <summary>
        /// Consumes <c>&lt;&gt;</c> or <c>!=</c>. Both spellings are real SQL and both are accepted,
        /// because a client generator in another language should not have to know which one this
        /// parser happened to pick.
        /// </summary>
        public bool TryConsumeNotEquals()
        {
            SkipWhitespace();
            if (_position + 2 > _text.Length)
                return false;
            var pair = _text.AsSpan(_position, 2);
            if (!pair.SequenceEqual("<>") && !pair.SequenceEqual("!="))
                return false;
            _position += 2;
            return true;
        }

        public void ExpectSymbol(char symbol)
        {
            if (!TryConsumeSymbol(symbol))
                throw new SqlParseException($"Expected '{symbol}' at position {_position}.");
        }

        public string ExpectIdentifier(string what)
        {
            SkipWhitespace();
            var start = _position;
            while (_position < _text.Length && IsIdentifierChar(_text[_position]))
                _position++;
            if (_position == start || char.IsAsciiDigit(_text[start]))
                throw new SqlParseException($"Expected {what} at position {start}.");
            return _text[start.._position];
        }

        public string ExpectStringLiteral(string what)
        {
            SkipWhitespace();
            if (_position >= _text.Length || _text[_position] != '\'')
                throw new SqlParseException($"Expected {what} at position {_position}.");
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != '\'')
                _position++;
            if (_position >= _text.Length)
                throw new SqlParseException("Unterminated string literal.");
            var literal = _text[start.._position];
            _position++;
            return literal;
        }

        public object? ExpectOperand(IReadOnlyDictionary<string, object?>? parameters)
        {
            SkipWhitespace();
            if (_position >= _text.Length)
                throw new SqlParseException("Expected a value at end of query.");

            var c = _text[_position];
            if (c == ':')
            {
                _position++;
                var name = ExpectIdentifier("a parameter name");
                if (parameters is null || !parameters.TryGetValue(name, out var value))
                    throw new SqlParseException($"Query names parameter :{name} but no value for it was supplied.");
                return value;
            }

            if (c == '\'')
                return ExpectStringLiteral("a string literal");

            if (c == '-' || char.IsAsciiDigit(c))
            {
                var start = _position;
                _position++;
                var isFloat = false;
                while (_position < _text.Length && (char.IsAsciiDigit(_text[_position]) || _text[_position] == '.'))
                {
                    isFloat |= _text[_position] == '.';
                    _position++;
                }

                var literal = _text[start.._position];
                if (isFloat)
                    return double.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);
                return long.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);
            }

            if (TryConsumeKeyword("TRUE"))
                return true;
            if (TryConsumeKeyword("FALSE"))
                return false;
            throw new SqlParseException($"Expected a :parameter or literal at position {_position}.");
        }

        public void ExpectEnd()
        {
            SkipWhitespace();
            if (_position < _text.Length)
                throw new SqlParseException($"Unexpected trailing text at position {_position}: '{_text[_position..]}'.");
        }

        private void SkipWhitespace()
        {
            while (_position < _text.Length && char.IsWhiteSpace(_text[_position]))
                _position++;
        }

        private static bool IsIdentifierChar(char c) => char.IsAsciiLetterOrDigit(c) || c == '_';
    }
}
