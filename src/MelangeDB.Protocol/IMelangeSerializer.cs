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

    /// <summary>Deserializes one websocket message. Malformed input throws <see cref="MelangeProtocolException"/>.</summary>
    Frame Deserialize(ReadOnlySpan<byte> message);
}
