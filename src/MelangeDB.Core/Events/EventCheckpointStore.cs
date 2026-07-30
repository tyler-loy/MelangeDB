using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// The durable subscriber checkpoints: one entry per logical subscriber, holding its applied LSN,
/// its last-active timestamp (what expiry is measured against), and — after eviction — a tombstone,
/// which is how a returning subscriber is told it lost its place instead of silently resuming. A
/// sidecar file beside the commit log, per the epoch-sidecar precedent: the log format is
/// untouched, and losing the file only costs redelivery, which at-least-once already permits.
/// Writes replace the file atomically so a crash never leaves a torn checkpoint set.
/// </summary>
internal sealed class EventCheckpointStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    private readonly string _path;

    public EventCheckpointStore(string logDirectory)
    {
        _path = Path.Combine(logDirectory, "melange.events.json");
        if (!File.Exists(_path))
            return;
        try
        {
            var loaded = JsonSerializer.Deserialize<Dictionary<string, Entry>>(File.ReadAllBytes(_path));
            if (loaded is not null)
                Entries = loaded;
        }
        catch (JsonException)
        {
            // A torn or foreign file: start empty. Subscribers restart from current state, which
            // the bus reports loudly; silent corruption must not be able to wedge startup.
        }
    }

    /// <summary>The full path of the checkpoint sidecar.</summary>
    public string FilePath => _path;

    /// <summary>All entries, keyed by subscriber name. Mutated by the bus under its own lock.</summary>
    public Dictionary<string, Entry> Entries { get; } = [];

    /// <summary>Persists the entries, replacing the file atomically.</summary>
    public void Save()
    {
        var temp = _path + ".tmp";
        File.WriteAllBytes(temp, JsonSerializer.SerializeToUtf8Bytes(Entries, SerializerOptions));
        File.Move(temp, _path, overwrite: true);
    }

    internal sealed class Entry
    {
        /// <summary>The subscriber's applied LSN: every event at or below it has been handled (or dead-lettered).</summary>
        public ulong Lsn { get; set; }

        /// <summary>When the subscriber last advanced or was seen alive — the expiry clock.</summary>
        public long LastActiveUnixMs { get; set; }

        /// <summary>The tombstone: set when expiry evicted this checkpoint.</summary>
        public bool Evicted { get; set; }

        public long? EvictedAtUnixMs { get; set; }
    }
}
