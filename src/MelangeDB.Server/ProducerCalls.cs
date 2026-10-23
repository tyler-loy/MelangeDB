using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Server;

/// <summary>What the socket and HTTP transports share about producer calls: error codes and JSON results.</summary>
internal static class ProducerCalls
{
    /// <summary>
    /// The wire code, HTTP status, and client-facing message for a failed call. Internal failures
    /// are logged here (EventId 1206) and answered generically; everything else is the caller's to
    /// read and is not logged.
    /// </summary>
    public static (string Code, int Status, string Message) Classify(Exception exception, string producer, ILogger logger)
    {
        switch (exception)
        {
            case UnknownProducerException:
                return (MelangeErrorCodes.UnknownProducer, StatusCodes.Status404NotFound, exception.Message);
            case ReducerArgumentException:
                return (MelangeErrorCodes.InvalidArguments, StatusCodes.Status400BadRequest, exception.Message);
            case RejectedException:
                return (MelangeErrorCodes.Rejected, StatusCodes.Status400BadRequest, exception.Message);
            case TransientRejectionException:
                return (MelangeErrorCodes.Transient, StatusCodes.Status409Conflict, exception.Message);
            case RateLimitedException:
                return (MelangeErrorCodes.RateLimited, StatusCodes.Status429TooManyRequests, exception.Message);
            case ReducerDeniedException:
                return (MelangeErrorCodes.Denied, StatusCodes.Status403Forbidden, exception.Message);
            case ProducerTimeoutException:
                return (MelangeErrorCodes.Timeout, StatusCodes.Status504GatewayTimeout, exception.Message);
            case ProducerResultTooLargeException:
                return (MelangeErrorCodes.TooManyBytes, StatusCodes.Status413PayloadTooLarge, exception.Message);
            default:
                LogProducerCallFailed(logger, producer, exception);
                return (MelangeErrorCodes.Internal, StatusCodes.Status500InternalServerError, "The producer failed; see the server logs.");
        }
    }

    /// <summary>
    /// Renders a result as JSON, under the response cap: a record as an object of its public fields
    /// and properties under their declared names — the names the schema manifest carries — an
    /// <see cref="Identity"/> as its hex string, a <see cref="Timestamp"/> as Unix microseconds, a
    /// <c>byte[]</c> as base64, and an enum as its number, the forms the HTTP reducer endpoint uses.
    /// </summary>
    public static ReadOnlyMemory<byte> RenderJson(object? value, int maxBytes)
    {
        var buffer = new CappedBufferWriter(maxBytes);
        using (var writer = new Utf8JsonWriter(buffer))
            JsonSerializer.Serialize(writer, value, value?.GetType() ?? typeof(object), JsonOptions);
        return buffer.Written;
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        IncludeFields = true,
        Converters = { new IdentityJsonConverter(), new TimestampJsonConverter() },
    };

    private static readonly Action<ILogger, string, Exception?> ProducerCallFailedMessage =
        LoggerMessage.Define<string>(
            LogLevel.Error,
            new EventId(1206, "ProducerCallFailed"),
            "Producer '{Producer}' threw an unexpected exception during a transport call.");

    private static void LogProducerCallFailed(ILogger logger, string producer, Exception failure) =>
        ProducerCallFailedMessage(logger, producer, failure);

    private sealed class IdentityJsonConverter : JsonConverter<Identity>
    {
        public override Identity Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            new(Convert.FromHexString(reader.GetString() ?? string.Empty));

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

    /// <summary>A buffer that refuses to grow past the response cap, so an oversized answer is never rendered in full.</summary>
    private sealed class CappedBufferWriter(int maxBytes) : IBufferWriter<byte>
    {
        private readonly ArrayBufferWriter<byte> _inner = new(256);

        public ReadOnlyMemory<byte> Written => _inner.WrittenMemory;

        public void Advance(int count)
        {
            if ((long)_inner.WrittenCount + count > maxBytes)
                throw new ProducerResultTooLargeException(maxBytes);
            _inner.Advance(count);
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => _inner.GetMemory(sizeHint);

        public Span<byte> GetSpan(int sizeHint = 0) => _inner.GetSpan(sizeHint);
    }
}
