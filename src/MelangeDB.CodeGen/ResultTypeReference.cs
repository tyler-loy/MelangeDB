namespace MelangeDB.CodeGen;

/// <summary>
/// The manifest's result type references: a scalar kind name, an enum's name, or a result type's
/// name, suffixed <c>?</c> for a nullable value type and <c>[]</c> for a list, innermost first.
/// </summary>
internal static class ResultTypeReference
{
    /// <summary>The name under every suffix.</summary>
    public static string Base(string reference)
    {
        var end = reference.Length;
        while (end > 0 && (reference[end - 1] == '?' || reference[end - 1] == ']'))
            end -= reference[end - 1] == '?' ? 1 : 2;
        return reference.Substring(0, end);
    }

    /// <summary>The scalar kind <paramref name="name"/> names, or null for an enum or a result type.</summary>
    public static WireKind? ScalarKind(string name) =>
        name is "None" or "ScheduleAt" || !Enum.TryParse<WireKind>(name, out var kind) ? null : kind;
}
