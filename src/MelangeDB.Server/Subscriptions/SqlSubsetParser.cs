namespace MelangeDB.Server;

/// <summary>The predicate shapes MelangeDB SQL supports.</summary>
internal enum PredicateKind
{
    None,
    Equality,
    Range,
}

/// <summary>
/// One parsed subscription (or ad-hoc) query: exactly the four supported shapes, operands already
/// resolved from named parameters or literals. Schema validation happens later, against the
/// registry, so parse errors and semantic errors stay distinguishable.
/// </summary>
internal sealed record SubscriptionQuery(
    string Table,
    IReadOnlyList<string>? Projection,
    PredicateKind Predicate,
    string? Column,
    object? EqualsValue,
    object? RangeLow,
    object? RangeHigh);

/// <summary>Thrown for text that is not valid MelangeDB SQL. The message names what was expected.</summary>
internal sealed class SqlParseException : Exception
{
    public SqlParseException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The hand-rolled parser for the MelangeDB SQL subset. Precisely four shapes are valid:
/// <c>SELECT * FROM t</c>, <c>SELECT * FROM t WHERE col = :p</c>,
/// <c>SELECT * FROM t WHERE col BETWEEN :lo AND :hi</c>, and any of those with an explicit column
/// list. A subset this small is a feature: "valid MelangeDB SQL" stays unambiguous, and every
/// client language can target it.
/// </summary>
internal static class SqlSubsetParser
{
    public static SubscriptionQuery Parse(string query, IReadOnlyDictionary<string, object?>? parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        var tokens = new Tokenizer(query);
        tokens.ExpectKeyword("SELECT");

        List<string>? projection = null;
        if (tokens.TryConsumeSymbol('*'))
        {
            // Whole-row selection.
        }
        else
        {
            projection = [tokens.ExpectIdentifier("a column name")];
            while (tokens.TryConsumeSymbol(','))
                projection.Add(tokens.ExpectIdentifier("a column name"));
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
            else if (tokens.TryConsumeKeyword("BETWEEN"))
            {
                predicate = PredicateKind.Range;
                rangeLow = tokens.ExpectOperand(parameters);
                tokens.ExpectKeyword("AND");
                rangeHigh = tokens.ExpectOperand(parameters);
            }
            else
            {
                throw new SqlParseException("Expected '=' or BETWEEN after the WHERE column.");
            }
        }

        tokens.ExpectEnd();
        return new SubscriptionQuery(table, projection, predicate, column, equalsValue, rangeLow, rangeHigh);
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
            {
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
