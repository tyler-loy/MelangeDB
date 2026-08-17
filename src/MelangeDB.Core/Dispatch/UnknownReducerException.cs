namespace MelangeDB.Core;

/// <summary>
/// The name does not resolve to a callable reducer — the only condition that may ever be answered
/// as <c>unknown_reducer</c>. A dedicated type rather than a bare <see cref="ArgumentException"/>
/// because the two are not distinguishable otherwise, and getting it wrong points debugging at
/// registration when the fault is inside the call: by the time a reducer's body runs, resolution
/// has provably succeeded (the arity check already passed), so anything thrown from there is the
/// reducer failing, not the reducer missing.
/// <para>
/// It is also thrown for a reducer that exists but is not client-callable — a lifecycle or
/// scheduled reducer. That is deliberate: answering "unknown" rather than "forbidden" keeps their
/// existence unconfirmed, and the caller cannot tell the two apart, which is the point.
/// </para>
/// </summary>
/// <remarks>
/// Derives from <see cref="ArgumentException"/> because the reducer name genuinely is an argument
/// to the in-process <c>Call</c>, and callers that already catch it keep working. The precision
/// lives in the type, not in the hierarchy: the transports catch <em>this</em>, so an
/// <see cref="ArgumentException"/> from inside a reducer no longer masquerades as a missing one.
/// </remarks>
public sealed class UnknownReducerException(string reducerName, string? paramName = null)
    : ArgumentException($"No reducer named '{reducerName}' is registered.", paramName)
{
    /// <summary>The name as the caller sent it.</summary>
    public string ReducerName { get; } = reducerName;

    /// <summary>
    /// The one sentence, without <see cref="ArgumentException"/>'s <c>(Parameter '…')</c> suffix.
    /// The message reaches clients verbatim, and the suffix would both change the wire text and —
    /// because the two throw sites name different parameters — let a caller tell a name that does
    /// not exist from a lifecycle or scheduled reducer that does. Keeping those indistinguishable
    /// is the reason the second case answers "unknown" at all.
    /// </summary>
    public override string Message { get; } = $"No reducer named '{reducerName}' is registered.";
}
