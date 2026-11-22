using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>
/// Subscriptions that are one query: the same table, projection, predicate and operands, on a
/// table with no row or column policies. Every member's delta for an op is the same, so the
/// fan-out computes it once for the group, keeps one op list per commit for all of them, and,
/// when there are at least two, encodes that list once (road-to-0.4 phase 31, step 2).
/// <para>
/// Membership and the per-commit scratch are touched only under the engine's write lock, by
/// registration, re-scope, unregistration, and the fan-out.
/// </para>
/// </summary>
internal sealed class SubscriptionGroup(SubscriptionGroup.GroupKey key)
{
    /// <summary>
    /// What makes two subscriptions one query. <see cref="Columns"/> compares by reference:
    /// registration converges equal wire-column sets onto one instance
    /// (<see cref="ServerSubscription.ShareWireColumns"/>), and null means every column.
    /// </summary>
    internal readonly record struct GroupKey(
        TableId Table,
        HashSet<string>? Columns,
        PredicateKind Predicate,
        string? Column,
        RowKey EqualsValue,
        RowKey RangeLow,
        RowKey RangeHigh);

    public GroupKey Key { get; } = key;

    public List<ServerSubscription> Members { get; } = [];

    /// <summary>The member the fan-out evaluates for all of them; any member gives the same verdicts.</summary>
    public ServerSubscription Representative => Members[0];

    /// <summary>The fan-out's op counter value <see cref="OpResult"/> was computed for.</summary>
    public long OpStamp { get; set; } = -1;

    /// <summary>The group's delta for the op <see cref="OpStamp"/> names; null when it gets none.</summary>
    public WireRowOp? OpResult { get; set; }

    /// <summary>The fan-out's record counter value <see cref="Ops"/> belongs to.</summary>
    public long RecordStamp { get; set; } = -1;

    /// <summary>This record's ops for every member; handed to every member's frame, never mutated after.</summary>
    public List<WireRowOp>? Ops { get; set; }

    /// <summary><see cref="Ops"/> encoded once for the frames, when the group has more than one member.</summary>
    public byte[]? EncodedOps { get; set; }

    /// <summary>
    /// Whether a subscription may share a group's verdicts: only when its table has no row or
    /// column policies, so nothing it receives depends on who asked (owner decision, 2026-11-22).
    /// </summary>
    public static bool CanGroup(ServerSubscription subscription) =>
        subscription.Evaluator is null
        || (!subscription.Evaluator.HasRowPolicies && !subscription.Evaluator.HasColumnPolicies);

    public static GroupKey KeyOf(ServerSubscription subscription) => new(
        subscription.Schema.Id,
        subscription.StaticWireColumns,
        subscription.Predicate,
        subscription.Column,
        subscription.EqualsValue,
        subscription.RangeLow,
        subscription.RangeHigh);
}
