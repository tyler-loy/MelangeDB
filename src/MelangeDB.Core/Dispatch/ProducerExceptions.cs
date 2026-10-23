namespace MelangeDB.Core;

/// <summary>
/// The name does not resolve to a registered producer — the only condition answered as
/// <c>unknown_producer</c>. As with reducers, anything thrown once the body runs is the producer
/// failing, not the producer missing.
/// </summary>
public sealed class UnknownProducerException(string producerName)
    : Exception($"No producer named '{producerName}' is registered.")
{
    /// <summary>The name as the caller sent it.</summary>
    public string ProducerName { get; } = producerName;
}

/// <summary>
/// The call ran past <c>Producers:TimeoutSeconds</c>. Its cancellation token fired and its read
/// view was released; a body that ignored the token is abandoned, and any read it makes afterwards
/// throws rather than reading a released pin.
/// </summary>
public sealed class ProducerTimeoutException(string producerName, TimeSpan timeout)
    : Exception($"Producer '{producerName}' did not finish within Producers:TimeoutSeconds ({timeout.TotalSeconds:0.###} s).")
{
    public string ProducerName { get; } = producerName;
}
