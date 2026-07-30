namespace MelangeDB.Sample;

/// <summary>
/// The sample's feature flag, bound from <c>Sample:Greeting</c>. Read through
/// <see cref="Microsoft.Extensions.Options.IOptionsMonitor{TOptions}"/> inside a reducer, so a
/// changed value alters behaviour on the next invocation with no restart — the whole point of
/// MelangeDB being a library in an ordinary host.
/// </summary>
public sealed class GreetingOptions
{
    /// <summary>When true, visitors are greeted with considerably more enthusiasm.</summary>
    public bool Excited { get; set; }
}
