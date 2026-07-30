namespace MelangeDB;

/// <summary>
/// The value determining which shard owns a row, derived by the shard strategy. The contract:
/// rows mutated in one transaction must resolve to the same shard key. Single-node deployments
/// use <see cref="Default"/> throughout; the mechanics land with clustering.
/// </summary>
public readonly record struct ShardKey(ulong Value) : IComparable<ShardKey>
{
    /// <summary>The single shard of a non-clustered deployment.</summary>
    public static ShardKey Default => default;

    public int CompareTo(ShardKey other) => Value.CompareTo(other.Value);

    public override string ToString() => $"shard:{Value}";
}
