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

    // Guards _nextSequence. Serialized transactions touch it only under the engine's write lock, so
    // this buys them nothing — but a snapshot-isolated body runs *outside* that lock, and allocating
    // an id reads this dictionary while another transaction's Commit writes it. A concurrent
    // read/write on a plain Dictionary is undefined: a torn read, a wrong answer, or a hang.
    private readonly Lock _gate = new();
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
    public ulong PeekNextSequence(TableId table)
    {
        lock (_gate)
            return PeekUnlocked(table);
    }

    private ulong PeekUnlocked(TableId table) => _nextSequence.GetValueOrDefault(table, 1UL);

    /// <summary>
    /// Reads and consumes a table's next sequence value in one atomic step — the allocation path for
    /// a transaction whose body runs outside the engine's write lock. Staging (peek now, consume at
    /// commit) cannot be used there: two concurrent bodies peek the same value and allocate the same
    /// id, which surfaces as a duplicate-key insert or, worse, as a reconcile silently turning one of
    /// them into an update over the other's row.
    /// <para>
    /// The cost is that an aborted transaction consumes the value it reserved, leaving a gap. That is
    /// within the sequencer's stated contract — ids are <b>unique, not dense</b> — and it costs
    /// nothing durable: the sequence is rebuilt at recovery by re-observing what actually committed.
    /// </para>
    /// </summary>
    internal ulong Reserve(TableId table)
    {
        lock (_gate)
        {
            var next = PeekUnlocked(table);
            if (next > MaxSequence)
                throw new InvalidOperationException($"AutoInc sequence for table {table} is exhausted.");
            _nextSequence[table] = next + 1;
            return next;
        }
    }

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

    /// <summary>
    /// Begins a transaction-scoped allocation stage. Nothing is consumed until
    /// <see cref="AutoIncStage.Commit"/> — unless <paramref name="reserveEagerly"/>, which a
    /// snapshot-isolated transaction requires; see <see cref="Reserve"/>.
    /// </summary>
    public AutoIncStage BeginStage(bool reserveEagerly = false) => new(this, reserveEagerly);

    internal void ObserveValue(TableId table, ulong id)
    {
        if (id == 0 || (ushort)(id >> OriginatorShift) != Originator)
            return;
        var sequence = id & MaxSequence;
        lock (_gate)
        {
            if (sequence >= PeekUnlocked(table))
                _nextSequence[table] = sequence + 1;
        }
    }

    internal void Advance(TableId table, ulong nextSequence)
    {
        lock (_gate)
        {
            if (nextSequence > PeekUnlocked(table))
                _nextSequence[table] = nextSequence;
        }
    }

    /// <summary>
    /// The durable sequence state a snapshot captures. A copy, not the live dictionary: the caller
    /// walks it while transactions may still be allocating.
    /// </summary>
    internal IReadOnlyDictionary<TableId, ulong> ExportSequences()
    {
        lock (_gate)
            return new Dictionary<TableId, ulong>(_nextSequence);
    }

    /// <summary>
    /// Restores one table's sequence from a snapshot. Recovery restores the snapshot's state first,
    /// then re-observes the log tail — so ids allocated after the snapshot still advance past.
    /// </summary>
    internal void RestoreSequence(TableId table, ulong nextSequence) => Advance(table, nextSequence);

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
/// <para>
/// A stage begun with <c>reserveEagerly</c> inverts that: it consumes on allocation instead, which
/// is what a body running outside the engine's write lock requires. Staging is safe only because a
/// serialized transaction is the only one running; two concurrent bodies staging against the same
/// sequence hand out the same id. The trade is a gap in the sequence when such a transaction
/// aborts — ids are unique, not dense.
/// </para>
/// </summary>
public sealed class AutoIncStage
{
    private readonly AutoIncSequencer _sequencer;
    private readonly Dictionary<TableId, ulong> _staged = [];
    private readonly bool _reserveEagerly;

    internal AutoIncStage(AutoIncSequencer sequencer, bool reserveEagerly = false)
    {
        _sequencer = sequencer;
        _reserveEagerly = reserveEagerly;
    }

    /// <summary>Allocates the next id for a table, composed with this node's originator.</summary>
    public ulong Allocate(TableId table)
    {
        if (_reserveEagerly)
            return AutoIncSequencer.Compose(_sequencer.Originator, _sequencer.Reserve(table));

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

        // Eagerly too: an explicit id that the durable sequence has not skipped past is an id a
        // concurrent body can still allocate.
        if (_reserveEagerly)
        {
            _sequencer.ObserveValue(table, id);
            return;
        }

        var sequence = id & AutoIncSequencer.MaxSequence;
        var next = _staged.GetValueOrDefault(table, _sequencer.PeekNextSequence(table));
        if (sequence >= next)
            _staged[table] = sequence + 1;
    }

    /// <summary>
    /// Publishes the staged allocations into the durable sequence. Called only after the commit
    /// point. A no-op for an eagerly reserving stage, which consumed as it went.
    /// </summary>
    public void Commit()
    {
        foreach (var (table, next) in _staged)
            _sequencer.Advance(table, next);
        _staged.Clear();
    }
}
