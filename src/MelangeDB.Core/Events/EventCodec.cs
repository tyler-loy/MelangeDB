using System.Text.Json;
using System.Text.Json.Serialization;

namespace MelangeDB.Core;

/// <summary>
/// Serializes domain events for the commit record. Events are ordinary user POCOs and records, so
/// this is reflection-based JSON (<see cref="JsonSerializer"/>, in the framework — no package)
/// with converters for MelangeDB's value types. The record format treats the payload as opaque
/// bytes plus a type name, so a schema-registered binary codec can supersede this one later
/// without touching the log format.
/// </summary>
internal static class EventCodec
{
    private static readonly JsonSerializerOptions Options = new()
    {
        IncludeFields = true,
        Converters = { new IdentityJsonConverter(), new TimestampJsonConverter() },
    };

    public static byte[] Serialize(object @event) =>
        JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), Options);

    public static object Deserialize(Type eventType, ReadOnlyMemory<byte> payload) =>
        JsonSerializer.Deserialize(payload.Span, eventType, Options)
        ?? throw new InvalidDataException($"Event payload deserialized to null for type {eventType.FullName}.");

    /// <summary>Renders a payload as a JSON string — what the dead-letter record embeds.</summary>
    public static string ToJsonString(ReadOnlyMemory<byte> payload) =>
        System.Text.Encoding.UTF8.GetString(payload.Span);

    private sealed class IdentityJsonConverter : JsonConverter<Identity>
    {
        public override Identity Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            new(Convert.FromHexString(reader.GetString()!));

        public override void Write(Utf8JsonWriter writer, Identity value, JsonSerializerOptions options) =>
            writer.WriteStringValue(value.ToString());
    }

    private sealed class TimestampJsonConverter : JsonConverter<Timestamp>
    {
        public override Timestamp Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            new(reader.GetInt64());

        public override void Write(Utf8JsonWriter writer, Timestamp value, JsonSerializerOptions options) =>
            writer.WriteNumberValue(value.UnixTimeMicroseconds);
    }
}
