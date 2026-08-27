namespace MelangeDB;

/// <summary>
/// The claims captured from the token the caller authenticated with — the ones named by
/// <c>Auth:CaptureClaims</c>, and only those.
/// <para>
/// Capture is an allow-list rather than the whole principal on purpose. Claims are connection-
/// scoped state read once at authentication and then carried for the life of the session, through
/// the ticket store, and — in a cluster — inside every internal identity assertion the gateway
/// mints on a client's behalf. An unbounded principal would make a large token a per-connection
/// cost nobody declared, and would ride every gateway hop. Naming the handful an application
/// actually reads keeps that bounded and makes the dependency visible in configuration.
/// </para>
/// <para>
/// A claim type may legitimately repeat — <c>role</c> and <c>groups</c> routinely do — so every
/// value is kept. <see cref="this[string]"/> answers the first, which is what a single-valued
/// claim like an account id wants; <see cref="GetValues"/> answers all of them.
/// </para>
/// </summary>
public sealed class CallerClaims
{
    private static readonly string[] NoValues = [];

    private readonly IReadOnlyDictionary<string, string[]> _byType;

    private CallerClaims(IReadOnlyDictionary<string, string[]> byType) => _byType = byType;

    /// <summary>No claims: an in-process call, a scheduled fire, or a host that captures none.</summary>
    public static CallerClaims Empty { get; } = new(new Dictionary<string, string[]>(StringComparer.Ordinal));

    /// <summary>
    /// Groups captured claims by type, preserving each type's values in the order the token
    /// presented them. Answers <see cref="Empty"/> for an empty sequence, so the common case
    /// allocates nothing.
    /// </summary>
    public static CallerClaims From(IEnumerable<KeyValuePair<string, string>> claims)
    {
        ArgumentNullException.ThrowIfNull(claims);
        Dictionary<string, List<string>>? grouped = null;
        foreach (var (type, value) in claims)
        {
            if (string.IsNullOrEmpty(type))
                continue;
            grouped ??= new Dictionary<string, List<string>>(StringComparer.Ordinal);
            if (!grouped.TryGetValue(type, out var values))
                grouped[type] = values = [];
            values.Add(value);
        }

        if (grouped is null)
            return Empty;

        var byType = new Dictionary<string, string[]>(grouped.Count, StringComparer.Ordinal);
        foreach (var (type, values) in grouped)
            byType[type] = [.. values];
        return new CallerClaims(byType);
    }

    /// <summary>How many distinct claim types were captured.</summary>
    public int Count => _byType.Count;

    /// <summary>The captured claim types.</summary>
    public IEnumerable<string> Types => _byType.Keys;

    /// <summary>
    /// The first value of <paramref name="type"/>, or null when the token carried it not at all or
    /// <c>Auth:CaptureClaims</c> did not name it. The two are deliberately indistinguishable here:
    /// a reducer that must tell them apart is asking a configuration question, not a caller one.
    /// </summary>
    public string? this[string type] => TryGetValue(type, out var value) ? value : null;

    /// <summary>Whether <paramref name="type"/> was captured with at least one value.</summary>
    public bool Contains(string type) => _byType.ContainsKey(type);

    /// <summary>The first value of <paramref name="type"/>, if it was captured.</summary>
    public bool TryGetValue(string type, out string value)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (_byType.TryGetValue(type, out var values) && values.Length > 0)
        {
            value = values[0];
            return true;
        }

        value = string.Empty;
        return false;
    }

    /// <summary>
    /// Every value of <paramref name="type"/>, in the order the token presented them; empty when
    /// the claim was not captured. The overload to reach for on a claim that may repeat.
    /// </summary>
    public IReadOnlyList<string> GetValues(string type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return _byType.TryGetValue(type, out var values) ? values : NoValues;
    }

    /// <summary>Every captured claim, as type/value pairs — a repeated type appears once per value.</summary>
    public IEnumerable<KeyValuePair<string, string>> AsPairs()
    {
        foreach (var (type, values) in _byType)
        {
            foreach (var value in values)
                yield return new KeyValuePair<string, string>(type, value);
        }
    }
}
