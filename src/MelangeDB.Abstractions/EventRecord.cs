namespace MelangeDB;

/// <summary>
/// One domain event as it is recorded in a commit record: the event type's full name, the publish
/// depth (how many event → reducer hops preceded it — the durable half of the cycle guard), and
/// the serialized payload. The payload is opaque bytes plus a type name on purpose: the log format
/// does not care how an event serializes, so the codec can be superseded without a format change.
/// </summary>
public readonly record struct EventRecord(string EventType, byte Depth, ReadOnlyMemory<byte> Payload);
