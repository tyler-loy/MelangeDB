namespace MelangeDB.Core;

/// <summary>
/// A decoupled applier's report of a condition its lag cannot show — rows it set aside instead of
/// applying, for example, while its checkpoint keeps advancing. The <c>melange-applier</c> health
/// check reads it from every registered applier that implements it, and reports Degraded.
/// </summary>
public interface IApplierCondition
{
    /// <summary>Why the applier is degraded, or null when it is not.</summary>
    string? Degraded { get; }
}
