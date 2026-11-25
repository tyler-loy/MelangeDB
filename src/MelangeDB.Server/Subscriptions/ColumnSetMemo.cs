using System.Runtime.CompilerServices;
using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>
/// The visible-column sets one commit record's fan-out hands out, shared between the subscribers
/// whose column policies return equal masks (#315).
/// <para>
/// A column policy is asked for a mask per subscriber per row, and the fan-out used to turn each
/// answer into a fresh set: a copy of the subscription's wire columns, filtered through the mask.
/// Fresh sets defeat everything downstream that keys on set identity, so each subscriber also
/// paid its own projected row, its own wire mask and a column-by-column comparison. A typical
/// policy has two answers (everything for the owner, one fixed mask for everyone else), so on a
/// busy table almost all of that work was repeated N times under the write lock.
/// </para>
/// <para>
/// Each mask is applied as one step, keyed on the set it starts from (by reference) and the
/// mask's content (<see cref="ColumnMask.SameAs"/>). Equal masks over the same starting set
/// therefore return the same instance, a chain of column policies shares each intermediate step,
/// and a mask that removes nothing returns its input unchanged. Content, not the mask's array
/// reference, is the key: masks built per call share too, and two masks of different modes over
/// one array can never be confused.
/// </para>
/// <para>
/// Lifetime is one commit record. The fan-out clears the memo before each record, so nothing here
/// outlives the record and a policy whose masks really are per row costs one lookup more than
/// before, not a growing cache. The sets never escape the record: a delta carries the projected
/// bytes and the wire mask, not the set. Only the fan-out thread touches it, under the engine's
/// write lock.
/// </para>
/// </summary>
internal sealed class ColumnSetMemo
{
    /// <summary>Above this many entries a cleared dictionary is replaced, so one huge record does not pin its capacity.</summary>
    private const int RetainedCapacity = 4096;

    private Dictionary<StepKey, IReadOnlySet<string>> _steps = [];
    private readonly Dictionary<TableSchema, HashSet<string>> _allColumns = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<IReadOnlySet<string>, ReadOnlyMemory<byte>> _wireMasks = new(ReferenceEqualityComparer.Instance);

    /// <summary>Sets built since this memo was created: a miss on a distinct mask. Tests read it.</summary>
    public long SetsBuilt { get; private set; }

    /// <summary>Starts a commit record: forgets every set handed out for the last one.</summary>
    public void Clear()
    {
        if (_steps.Count > RetainedCapacity)
            _steps = [];
        else
            _steps.Clear();
        _allColumns.Clear();
        _wireMasks.Clear();
    }

    /// <summary>
    /// Every column of <paramref name="schema"/>: the starting set for a subscription whose static
    /// wire columns are null (meaning all of them), shared so its steps can be too.
    /// </summary>
    public IReadOnlySet<string> AllColumns(TableSchema schema)
    {
        if (!_allColumns.TryGetValue(schema, out var all))
        {
            all = new HashSet<string>(schema.Columns.Select(c => c.Name), StringComparer.Ordinal);
            _allColumns[schema] = all;
        }

        return all;
    }

    /// <summary>The columns of <paramref name="input"/> that <paramref name="mask"/> admits, shared for equal masks.</summary>
    public IReadOnlySet<string> Apply(IReadOnlySet<string> input, in ColumnMask mask)
    {
        if (mask.AdmitsAll)
            return input;

        var key = new StepKey(input, mask);
        if (_steps.TryGetValue(key, out var shared))
            return shared;

        var built = new HashSet<string>(input.Count, StringComparer.Ordinal);
        foreach (var column in input)
        {
            if (mask.Admits(column))
                built.Add(column);
        }

        // A mask that removes nothing keeps the input's identity, so what follows (the next
        // policy's step, the projection memo, the wire mask) sees the set it would without it.
        IReadOnlySet<string> result = built.Count == input.Count ? input : built;
        _steps[new StepKey(input, mask.Detached())] = result;
        SetsBuilt++;
        return result;
    }

    /// <summary>
    /// The wire mask for a set this memo handed out, computed once per set against the first
    /// asking subscription's descriptor. Every subscription that reaches a shared set has the
    /// same descriptor: the set descends from that subscription's static wire columns (shared by
    /// reference only between subscriptions with equal ones, <see cref="ServerSubscription.ShareWireColumns"/>)
    /// or from <see cref="AllColumns"/>, and the descriptor is a function of exactly that.
    /// </summary>
    public ReadOnlyMemory<byte> WireMask(IReadOnlySet<string> visible, WireDescriptor descriptor)
    {
        if (!_wireMasks.TryGetValue(visible, out var mask))
        {
            mask = RowWire.Mask(descriptor.Columns, visible);
            _wireMasks[visible] = mask;
        }

        return mask;
    }

    private readonly struct StepKey(IReadOnlySet<string> input, ColumnMask mask) : IEquatable<StepKey>
    {
        private readonly IReadOnlySet<string> _input = input;
        private readonly ColumnMask _mask = mask;
        private readonly int _hash = HashCode.Combine(RuntimeHelpers.GetHashCode(input), mask.ContentHash());

        public bool Equals(StepKey other) =>
            _hash == other._hash && ReferenceEquals(_input, other._input) && _mask.SameAs(other._mask);

        public override bool Equals(object? obj) => obj is StepKey other && Equals(other);

        public override int GetHashCode() => _hash;
    }
}
