using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// One dead-lettered event: the durable record of a poisoned delivery — which subscriber gave up
/// on which event, after how many attempts, and why. Appended as one JSON line to
/// <c>melange.deadletter.ndjson</c> under <c>Events:DeadLetterPath</c>; the payload rides along as
/// raw JSON so the event is recoverable by tooling without the emitting type.
/// </summary>
public sealed record DeadLetterRecord(
    string DeadLetteredAt,
    string Subscriber,
    string EventType,
    ulong Lsn,
    int Depth,
    int Attempts,
    string Error,
    string ErrorType,
    JsonElement Payload);

/// <summary>Appends dead-letter records durably. One line per event, flushed per append.</summary>
internal sealed class DeadLetterStore(string directory)
{
    private readonly Lock _lock = new();
    private bool _created;

    public string FilePath { get; } = Path.Combine(directory, "melange.deadletter.ndjson");

    public void Append(DeadLetterRecord record)
    {
        lock (_lock)
        {
            if (!_created)
            {
                Directory.CreateDirectory(directory);
                _created = true;
            }

            using var stream = new FileStream(FilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            using var writer = new StreamWriter(stream);
            writer.WriteLine(JsonSerializer.Serialize(record));
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }
    }
}
