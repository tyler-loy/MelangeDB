using MelangeDB.Core;

namespace MelangeDB.Cluster;

/// <summary>
/// One migratable entity, as named by <see cref="IMigrationAnchors"/>: the identity whose rows
/// travel together (the <see cref="IHandoffSet"/> collects them by it), and whether it migrates
/// immediately on crossing. Players are <em>not</em> immediate — the hysteresis margin
/// (<c>Cluster:HandoffMarginChunks</c>) stops a player pacing on the line from thrashing —
/// but creatures are: a creature's AI only ticks it on the shard its position resolves to, so a
/// creature waiting out a margin would stand unticked at the boundary.
/// </summary>
public readonly record struct MigrationAnchor(Identity Id, bool Immediate);

/// <summary>
/// Names the rows that anchor automatic migration — the application's knowledge, on the same
/// mechanism-versus-meaning line as the shard strategy and the handoff set. The boundary monitor
/// asks this for every committed Partitioned write; a row with no anchor never migrates by itself
/// (companion rows follow their anchor via the <see cref="IHandoffSet"/>, and terrain follows
/// nothing). Register one in DI on shard nodes to enable seamless, walking-triggered handoff.
/// </summary>
public interface IMigrationAnchors
{
    /// <summary>The entity this row anchors, or null when the row never migrates by itself.</summary>
    MigrationAnchor? AnchorOf(TableSchema table, in RowRef row);
}

/// <summary>
/// Hub-side application hook, invoked when a transfer's destination becomes authoritative — the
/// moment the session-to-shard mapping the application's <c>ShardForSession</c> locator reads
/// must flip. Invoked for coordinator-driven transfers and for transfers resolved by a node's
/// reconciler after a crash (EventId 1714); implementations must be idempotent, because a
/// recovered saga may report the same transfer more than once.
/// </summary>
public interface IShardTransferListener
{
    /// <summary>The entity's rows now live on <paramref name="to"/>.</summary>
    void OnTransferred(Identity entity, ShardKey from, ShardKey to);
}
