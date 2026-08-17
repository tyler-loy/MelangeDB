using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// What a cloned world is a clone of, recorded beside its log as <c>melange.provenance.json</c>.
/// The support question — "what is this staging world, and how old is it?" — answered by a file
/// rather than by someone's memory of which archive they used, and read back at every boot so it
/// is answered by the server's log too.
/// <para>
/// It is a directory-local artifact, not part of the archive: a backup captures a <em>world</em>,
/// and a restore of a clone's archive is a rewind of the clone, whose own origin the operator
/// names. Keeping it out of the archive also keeps the sidecar set an older build's restore
/// understands unchanged.
/// </para>
/// </summary>
/// <param name="Kind">Always <c>clone</c> today; present so a future verb can record its own provenance without a format break.</param>
/// <param name="SourceEpoch">The epoch of the world the archive was captured from — the identity a support question starts at.</param>
/// <param name="SourceHeadLsn">The captured head: how far into the source world this clone reaches.</param>
/// <param name="Archive">The archive's file name (not its path — a path is this machine's business, and may hold a credential).</param>
/// <param name="ArchiveCapturedAtUnixMs">When the archive was taken, which is how stale this world is.</param>
/// <param name="ClonedAtUnixMs">When the clone was made.</param>
/// <param name="Epoch">This world's own fresh epoch, minted by the clone.</param>
public sealed record CloneProvenance(
    string Kind,
    Guid SourceEpoch,
    ulong SourceHeadLsn,
    string Archive,
    long ArchiveCapturedAtUnixMs,
    long ClonedAtUnixMs,
    Guid Epoch)
{
    /// <summary>The sidecar's file name, beside the log in each engine's directory.</summary>
    public const string FileName = "melange.provenance.json";

    /// <summary>The only <see cref="Kind"/> this build writes.</summary>
    public const string CloneKind = "clone";

    /// <summary>
    /// Reads the provenance beside a data directory's log, or null when the world is not a clone.
    /// Null on any doubt — an unreadable or unparseable sidecar is not worth failing a boot over,
    /// and the file is a support artifact rather than a correctness one.
    /// </summary>
    public static CloneProvenance? TryRead(string dataDirectory)
    {
        try
        {
            var path = Path.Combine(dataDirectory, FileName);
            return File.Exists(path) ? JsonSerializer.Deserialize<CloneProvenance>(File.ReadAllBytes(path)) : null;
        }
        catch (Exception exception) when (exception is IOException or JsonException or UnauthorizedAccessException or NotSupportedException)
        {
            return null;
        }
    }

    internal void Write(string dataDirectory) =>
        File.WriteAllBytes(
            Path.Combine(dataDirectory, FileName),
            JsonSerializer.SerializeToUtf8Bytes(this, new JsonSerializerOptions { WriteIndented = true }));
}
