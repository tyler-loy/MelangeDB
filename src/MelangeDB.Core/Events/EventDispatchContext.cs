namespace MelangeDB.Core;

/// <summary>
/// The ambient publish depth: zero everywhere except inside an event handler's delivery, where it
/// is one more than the depth of the event being handled. <see cref="EventStage"/> stamps it onto
/// every published event, which is the volatile half of the cycle guard — the durable half is the
/// depth byte in the event record.
/// </summary>
internal static class EventDispatchContext
{
    private static readonly AsyncLocal<int> Depth = new();

    public static int CurrentDepth => Depth.Value;

    /// <summary>Scopes the ambient depth around one handler invocation.</summary>
    public static DepthScope Enter(int depth)
    {
        var previous = Depth.Value;
        Depth.Value = depth;
        return new DepthScope(previous);
    }

    internal readonly struct DepthScope(int previous) : IDisposable
    {
        public void Dispose() => Depth.Value = previous;
    }
}
