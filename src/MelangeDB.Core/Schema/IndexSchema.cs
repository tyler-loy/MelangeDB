namespace MelangeDB.Core;

/// <summary>A single-column secondary index. Unique indexes are enforced at write time.</summary>
public sealed class IndexSchema
{
    public required string Column { get; init; }

    public required bool Unique { get; init; }
}
