namespace MelangeDB.Core;

/// <summary>
/// Allocates AutoInc ids from durable per-table sequences. Layout: bit 63 always zero, 16-bit
/// originator, 47-bit per-shard sequence — so every id fits a signed 64-bit integer and
/// round-trips through Postgres <c>bigint</c> unchanged. The contract is unique, not dense.
/// Durability comes from the log itself: allocated values ride in the write set, and recovery
/// re-observes them, so an aborted transaction (which appended nothing) consumes nothing.
/// </summary>
public sealed class AutoIncSequencer
{
    internal const int OriginatorShift = 47;
    internal const ulong MaxSequence = (1UL << OriginatorShift) - 1;

    private readonly Dictionary<TableId, ulong> _nextSequence = [];

    public AutoIncSequencer(ushort originator = 0) => Originator = originator;

    /// <summary>This node's originator. Zero on a single-node deployment; assigned by phase 09's membership store.</summary>
    public ushort Originator { get; }

    /// <summary>Composes an id from originator and sequence. Bit 63 is always zero.</summary>
    public static ulong Compose(ushort originator, ulong sequence)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(sequence, MaxSequence);
        return ((ulong)originator << OriginatorShift) | sequence;
    }

    /// <summary>The next sequence value (not composed id) this table would allocate.</summary>
    public ulong PeekNextSequence(TableId table) => _nextSequence.GetValueOrDefault(table, 1UL);

    /// <summary>
    /// Re-observes ids in a committed record during startup recovery, bumping each table's
    /// sequence past every value this originator ever durably allocated.
    /// </summary>
    public void Observe(CommitRecord record, SchemaRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(registry);
        foreach (var op in record.WriteSet)
        {
            if (op.Kind == RowOpKind.Delete || !registry.TryGet(op.Table, out var schema) || schema.AutoIncColumns.Count == 0)
                continue;
            var row = RowSerializer.Deserialize(schema, op.Row);
            foreach (var column in schema.AutoIncColumns)
            {
                var value = ToUInt64(column.GetValue(row));
                if (value is { } id)
                    ObserveValue(op.Table, id);
            }
        }
    }

    /// <summary>Begins a transaction-scoped allocation stage. Nothing is consumed until <see cref="AutoIncStage.Commit"/>.</summary>
    public AutoIncStage BeginStage() => new(this);

    internal void ObserveValue(TableId table, ulong id)
    {
        if (id == 0 || (ushort)(id >> OriginatorShift) != Originator)
            return;
        var sequence = id & MaxSequence;
        if (sequence >= PeekNextSequence(table))
            _nextSequence[table] = sequence + 1;
    }

    internal void Advance(TableId table, ulong nextSequence)
    {
        if (nextSequence > PeekNextSequence(table))
            _nextSequence[table] = nextSequence;
    }

    internal static ulong? ToUInt64(object? value) => value switch
    {
        ulong u => u,
        long l and >= 0 => (ulong)l,
        _ => null,
    };
}

/// <summary>
/// A transaction's staged view of the sequencer: allocations are visible to the transaction
/// immediately but consumed from the durable sequence only at <see cref="Commit"/> — which the
/// dispatcher calls after the log append succeeds. An abandoned stage consumes nothing.
/// </summary>
public sealed class AutoIncStage
{
    private readonly AutoIncSequencer _sequencer;
    private readonly Dictionary<TableId, ulong> _staged = [];

    internal AutoIncStage(AutoIncSequencer sequencer) => _sequencer = sequencer;

    /// <summary>Allocates the next id for a table, composed with this node's originator.</summary>
    public ulong Allocate(TableId table)
    {
        var next = _staged.GetValueOrDefault(table, _sequencer.PeekNextSequence(table));
        if (next > AutoIncSequencer.MaxSequence)
            throw new InvalidOperationException($"AutoInc sequence for table {table} is exhausted.");
        _staged[table] = next + 1;
        return AutoIncSequencer.Compose(_sequencer.Originator, next);
    }

    /// <summary>
    /// Notes an explicitly supplied id so the sequence skips past it if it belongs to this
    /// originator's range.
    /// </summary>
    public void ObserveExplicit(TableId table, ulong id)
    {
        if (id == 0 || (ushort)(id >> AutoIncSequencer.OriginatorShift) != _sequencer.Originator)
            return;
        var sequence = id & AutoIncSequencer.MaxSequence;
        var next = _staged.GetValueOrDefault(table, _sequencer.PeekNextSequence(table));
        if (sequence >= next)
            _staged[table] = sequence + 1;
    }

    /// <summary>Publishes the staged allocations into the durable sequence. Called only after the commit point.</summary>
    public void Commit()
    {
        foreach (var (table, next) in _staged)
            _sequencer.Advance(table, next);
        _staged.Clear();
    }
}
