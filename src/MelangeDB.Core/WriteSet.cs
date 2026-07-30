namespace MelangeDB.Core;

/// <summary>
/// The ordered row operations a transaction accumulates, keyed by (table, primary key) with
/// last-write-wins collapsing: touching one row many times yields one net op. An insert that is
/// later deleted vanishes entirely; a delete followed by a re-insert collapses to an update of the
/// stored row. The collapsed list is the authoritative payload of the transaction's log record.
/// </summary>
public sealed class WriteSet
{
    private readonly Dictionary<(TableId Table, RowKey Key), int> _slots = [];
    private readonly List<RowOp?> _ops = [];
    private int _count;

    /// <summary>The number of live (collapsed) ops.</summary>
    public int Count => _count;

    /// <summary>
    /// Stages one op, collapsing against any earlier op on the same key. The caller (the
    /// transaction view) is responsible for having validated the transition — e.g. no update of a
    /// missing row.
    /// </summary>
    public void Stage(in RowOp op)
    {
        var slotKey = (op.Table, op.Key);
        if (!_slots.TryGetValue(slotKey, out var slot))
        {
            _slots.Add(slotKey, _ops.Count);
            _ops.Add(op);
            _count++;
            return;
        }

        var existing = _ops[slot]!.Value;
        switch (existing.Kind, op.Kind)
        {
            case (RowOpKind.Insert, RowOpKind.Update):
                _ops[slot] = new RowOp(RowOpKind.Insert, op.Table, op.Key, op.Row);
                break;
            case (RowOpKind.Insert, RowOpKind.Delete):
                // Inserted then deleted inside one transaction: net nothing.
                _ops[slot] = null;
                _slots.Remove(slotKey);
                _count--;
                break;
            case (RowOpKind.Update, RowOpKind.Update):
                _ops[slot] = op;
                break;
            case (RowOpKind.Update, RowOpKind.Delete):
                _ops[slot] = new RowOp(RowOpKind.Delete, op.Table, op.Key);
                break;
            case (RowOpKind.Delete, RowOpKind.Insert):
                // The stored row was deleted and re-inserted: net an update of the stored row.
                _ops[slot] = new RowOp(RowOpKind.Update, op.Table, op.Key, op.Row);
                break;
            default:
                throw new InvalidOperationException($"Invalid write-set transition {existing.Kind} -> {op.Kind}.");
        }
    }

    /// <summary>Looks up the pending op for a key, if any — the overlay's write half.</summary>
    public bool TryGetPending(TableId table, RowKey key, out RowOp op)
    {
        if (_slots.TryGetValue((table, key), out var slot))
        {
            op = _ops[slot]!.Value;
            return true;
        }

        op = default;
        return false;
    }

    /// <summary>Enumerates pending ops for one table in staging order.</summary>
    public IEnumerable<RowOp> OpsFor(TableId table)
    {
        foreach (var op in _ops)
        {
            if (op is { } live && live.Table == table)
                yield return live;
        }
    }

    /// <summary>The collapsed ops in first-touch order — the log record payload.</summary>
    public IReadOnlyList<RowOp> ToOps()
    {
        var result = new List<RowOp>(_count);
        foreach (var op in _ops)
        {
            if (op is { } live)
                result.Add(live);
        }

        return result;
    }
}
