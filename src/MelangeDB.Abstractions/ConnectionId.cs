namespace MelangeDB;

/// <summary>
/// Identifies one client socket. One <see cref="Identity"/> may hold several connections at once;
/// a connection never outlives its socket.
/// </summary>
public readonly record struct ConnectionId(Guid Value)
{
    /// <summary>The absent connection, used when work has no originating socket.</summary>
    public static ConnectionId None => default;

    /// <summary>Whether this is the absent connection.</summary>
    public bool IsNone => Value == Guid.Empty;

    /// <summary>Creates a new unique connection id.</summary>
    public static ConnectionId New() => new(Guid.NewGuid());

    public override string ToString() => Value.ToString("N");
}
