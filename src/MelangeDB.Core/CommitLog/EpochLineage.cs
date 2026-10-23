using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>One ancestor in a log's lineage: an epoch, and the last LSN of its history this log inherited.</summary>
/// <param name="Epoch">The ancestor epoch.</param>
/// <param name="UpToLsn">The fork point: records up to and including this LSN are this log's history too.</param>
public sealed record EpochAncestor(Guid Epoch, ulong UpToLsn);

/// <summary>
/// Which earlier epochs a log descends from, persisted in the <c>melange.lineage</c> sidecar
/// beside <c>melange.epoch</c>. A restore mints a new epoch that continues numbering from the
/// restored head, so an LSN means something only together with its epoch — and a projection that
/// keeps rows outside the log (the archive tier) has to know which (epoch, LSN) pairs are this
/// log's history and which belong to a future the restore rewound.
/// <para>
/// A row is in this log's history when its epoch is the current one, or an ancestor's and its LSN
/// is at or below that ancestor's fork point. A directory without the sidecar has no ancestors:
/// it was never restored, or was restored before lineage was recorded.
/// </para>
/// </summary>
public sealed class EpochLineage
{
    /// <summary>The sidecar's file name, beside the log.</summary>
    public const string FileName = "melange.lineage";

    public EpochLineage(IEnumerable<EpochAncestor> ancestors)
    {
        ArgumentNullException.ThrowIfNull(ancestors);
        Ancestors = [.. ancestors];
    }

    public static EpochLineage Empty { get; } = new([]);

    /// <summary>The ancestors, oldest first.</summary>
    public IReadOnlyList<EpochAncestor> Ancestors { get; }

    /// <summary>The fork point inherited from <paramref name="epoch"/>, or null when it is not an ancestor.</summary>
    public ulong? UpToFor(Guid epoch)
    {
        foreach (var ancestor in Ancestors)
        {
            if (ancestor.Epoch == epoch)
                return ancestor.UpToLsn;
        }

        return null;
    }

    /// <summary>
    /// The lineage of a log restored from <paramref name="sourceEpoch"/> at <paramref name="restoredHead"/>:
    /// this lineage plus the source. LSNs are continuous along a lineage, so an ancestor whose fork
    /// point lies past the restored head is clamped to it — a restore to an earlier LSN rewinds
    /// inherited history too.
    /// </summary>
    public EpochLineage Extend(Guid sourceEpoch, ulong restoredHead) =>
        new(Ancestors
            .Where(ancestor => ancestor.Epoch != sourceEpoch)
            .Select(ancestor => ancestor with { UpToLsn = Math.Min(ancestor.UpToLsn, restoredHead) })
            .Append(new EpochAncestor(sourceEpoch, restoredHead)));

    /// <summary>Loads the sidecar from a log directory; <see cref="Empty"/> when there is none.</summary>
    public static EpochLineage Load(string directory)
    {
        var path = Path.Combine(directory, FileName);
        if (!File.Exists(path))
            return Empty;
        try
        {
            var ancestors = JsonSerializer.Deserialize<List<EpochAncestor>>(File.ReadAllBytes(path));
            return ancestors is null
                ? throw new InvalidDataException($"The lineage sidecar '{path}' is empty; restore it from backup.")
                : new EpochLineage(ancestors);
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                $"The lineage sidecar '{path}' is not valid JSON. It records which earlier epochs this log descends from, " +
                "which a Postgres archive needs after a restore; restore it from backup rather than deleting it.", exception);
        }
    }

    /// <summary>Writes the sidecar atomically — temp file, then rename over.</summary>
    public void Save(string directory)
    {
        var path = Path.Combine(directory, FileName);
        var temp = path + ".tmp";
        File.WriteAllBytes(temp, JsonSerializer.SerializeToUtf8Bytes(Ancestors));
        File.Move(temp, path, overwrite: true);
    }
}
