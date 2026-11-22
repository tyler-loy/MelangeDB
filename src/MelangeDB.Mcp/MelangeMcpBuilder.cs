namespace MelangeDB.Mcp;

/// <summary>
/// What the agent endpoint may do beyond reading: the reducers it may call, each named here, in
/// code, where a grant is visible in review. There is no wildcard. Reads (schema, SQL under
/// <c>Sql:*</c>, health, signals) need no grant beyond the endpoint itself.
/// </summary>
public sealed class MelangeMcpBuilder
{
    private readonly Dictionary<string, Identity?> _reducers = new(StringComparer.Ordinal);

    internal IReadOnlyDictionary<string, Identity?> Reducers => _reducers;

    /// <summary>
    /// Lets an agent call these reducers, each as the identity in the agent's own token: the
    /// caller the reducer's policies see is the agent's, and the log records it as the caller.
    /// </summary>
    public MelangeMcpBuilder AllowReducers(params string[] names)
    {
        ArgumentNullException.ThrowIfNull(names);
        foreach (var name in names)
        {
            ArgumentException.ThrowIfNullOrEmpty(name);
            _reducers[name] = null;
        }

        return this;
    }

    /// <summary>
    /// Lets an agent call <paramref name="name"/>, always as <paramref name="runAs"/>, whatever
    /// identity its token carries: a fixed test identity for a reducer that would otherwise act
    /// as whoever the agent authenticated as. Policies see <paramref name="runAs"/>.
    /// </summary>
    public MelangeMcpBuilder AllowReducer(string name, Identity runAs)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _reducers[name] = runAs;
        return this;
    }
}
