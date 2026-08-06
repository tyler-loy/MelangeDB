namespace MelangeDB.Protocol;

/// <summary>
/// The seam in front of the wire format. MessagePack is the v1 implementation because it has
/// implementations in every client language; a source-generated binary format can replace it
/// behind this interface once there is something to measure.
/// </summary>
public interface IMelangeSerializer
{
    /// <summary>The serializer's stable name, negotiated implicitly by configuration.</summary>
    string Name { get; }

    /// <summary>Serializes one frame into one websocket message.</summary>
    byte[] Serialize(Frame frame);

    /// <summary>
    /// The exact number of bytes <see cref="Serialize"/> would produce for this frame, without
    /// producing them. Implementations must answer by running their own write path against a
    /// counting sink rather than by computing sizes separately — a size calculator maintained
    /// beside the writer is free to drift from it.
    /// <para>
    /// The delta path needs a frame's size under the engine's write lock to judge backpressure,
    /// but wants the encoding to happen on the connection's sender. This is what lets those two
    /// happen in different places.
    /// </para>
    /// </summary>
    int Measure(Frame frame);

    /// <summary>Deserializes one websocket message. Malformed input throws <see cref="MelangeProtocolException"/>.</summary>
    Frame Deserialize(ReadOnlySpan<byte> message);
}
