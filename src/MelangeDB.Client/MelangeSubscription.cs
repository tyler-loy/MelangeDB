using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>One row in a subscription's local cache: the encoded primary key and the column values.</summary>
public sealed record MelangeRow(byte[] Key, IReadOnlyDictionary<string, object?> Columns);

/// <summary>
/// A live subscription and its locally maintained row cache — a projection of the server's state,
/// loaded from the initial set and advanced by deltas. Events fire on the client's receive loop.
/// Deltas that arrive while the initial set is still streaming are buffered and applied once the
/// set completes, keeping the anchor-LSN boundary gap-free and duplicate-free on this side too.
/// </summary>
public sealed class MelangeSubscription
{
    private readonly Lock _lock = new();
    private readonly Dictionary<ByteKey, MelangeRow> _rows = [];
    private readonly List<WireRow> _initialRows = [];
    private readonly List<(ulong Lsn, IReadOnlyList<WireRowOp> Ops)> _pending = [];
    private TaskCompletionSource _applied = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private ulong _anchorLsn;
    private bool _live;

    internal MelangeSubscription(uint id, string query, IReadOnlyDictionary<string, object?>? parameters)
    {
        Id = id;
        Query = query;
        Parameters = parameters;
    }

    public uint Id { get; }

    public string Query { get; }

    /// <summary>The current parameter values; replaced when the subscription is re-scoped.</summary>
    public IReadOnlyDictionary<string, object?>? Parameters { get; internal set; }

    /// <summary>Completes when the initial result set has been fully applied.</summary>
    public Task Applied => _applied.Task;

    /// <summary>The LSN the initial set was consistent at.</summary>
    public ulong AnchorLsn => _anchorLsn;

    /// <summary>Fires for a row entering the subscription — by insert, or by moving into scope.</summary>
    public event Action<MelangeRow>? OnInsert;

    /// <summary>Fires for a changed row: (previous, current).</summary>
    public event Action<MelangeRow, MelangeRow>? OnUpdate;

    /// <summary>Fires for a row leaving the subscription — by delete, or by moving out of scope.</summary>
    public event Action<MelangeRow>? OnDelete;

    /// <summary>
    /// Deltas that contradicted the cache: an insert for a key already present, or an update for a
    /// key never seen. Always zero unless the initial-set/delta boundary leaked a gap or a
    /// duplicate — the invariant the anchored-LSN design exists to hold. Deletes for absent keys
    /// are not counted; resume replay emits those legitimately.
    /// </summary>
    public long Inconsistencies { get; private set; }

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _rows.Count;
            }
        }
    }

    /// <summary>A snapshot of the cached rows.</summary>
    public IReadOnlyList<MelangeRow> Rows
    {
        get
        {
            lock (_lock)
            {
                return [.. _rows.Values];
            }
        }
    }

    public bool TryGetRow(byte[] key, out MelangeRow row)
    {
        lock (_lock)
        {
            return _rows.TryGetValue(new ByteKey(key), out row!);
        }
    }

    internal void AcceptInitialChunk(SubscriptionAppliedFrame chunk)
    {
        List<(ulong, IReadOnlyList<WireRowOp>)>? replay = null;
        lock (_lock)
        {
            _initialRows.AddRange(chunk.Rows);
            if (!chunk.IsLast)
                return;

            _anchorLsn = chunk.AnchorLsn;
            _rows.Clear();
            foreach (var row in _initialRows)
                _rows[new ByteKey(row.Key)] = new MelangeRow(row.Key, row.Columns);
            _initialRows.Clear();
            _live = true;
            replay = [.. _pending];
            _pending.Clear();
        }

        if (replay is not null)
        {
            foreach (var (lsn, ops) in replay)
                Apply(lsn, ops);
        }

        _applied.TrySetResult();
    }

    internal void Apply(ulong lsn, IReadOnlyList<WireRowOp> ops)
    {
        lock (_lock)
        {
            if (!_live)
            {
                _pending.Add((lsn, ops));
                return;
            }

            // Deltas at or below the anchor were already in the initial set; the server never
            // sends them, and dropping any stray one is what "no duplicate" means here.
            if (lsn != 0 && lsn <= _anchorLsn)
                return;
        }

        foreach (var op in ops)
            ApplyOp(op);
    }

    internal void ResetForResync()
    {
        lock (_lock)
        {
            _live = false;
            _rows.Clear();
            _initialRows.Clear();
            _pending.Clear();
            if (_applied.Task.IsCompleted)
                _applied = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    internal void FailSubscribe(string code, string message) =>
        _applied.TrySetException(new MelangeSubscriptionException(code, message));

    internal bool IsLive
    {
        get
        {
            lock (_lock)
            {
                return _live;
            }
        }
    }

    private void ApplyOp(in WireRowOp op)
    {
        MelangeRow? previous = null;
        MelangeRow? current = null;
        var kind = op.Kind;
        lock (_lock)
        {
            var key = new ByteKey(op.Key);
            switch (op.Kind)
            {
                case RowOpKind.Insert:
                    if (_rows.TryGetValue(key, out var duplicate))
                    {
                        Inconsistencies++;
                        previous = duplicate;
                        kind = RowOpKind.Update;
                    }

                    current = new MelangeRow(op.Key, op.Columns!);
                    _rows[key] = current;
                    break;
                case RowOpKind.Update:
                    if (_rows.TryGetValue(key, out var existing))
                    {
                        previous = existing;
                    }
                    else
                    {
                        Inconsistencies++;
                        kind = RowOpKind.Insert;
                    }

                    current = new MelangeRow(op.Key, op.Columns!);
                    _rows[key] = current;
                    break;
                case RowOpKind.Delete:
                    if (_rows.Remove(key, out var removed))
                        previous = removed;
                    break;
            }
        }

        switch (kind)
        {
            case RowOpKind.Insert when current is not null:
                OnInsert?.Invoke(current);
                break;
            case RowOpKind.Update when current is not null:
                OnUpdate?.Invoke(previous ?? current, current);
                break;
            case RowOpKind.Delete when previous is not null:
                OnDelete?.Invoke(previous);
                break;
        }
    }

    private readonly struct ByteKey(byte[] bytes) : IEquatable<ByteKey>
    {
        private readonly byte[] _bytes = bytes;

        public bool Equals(ByteKey other) => _bytes.AsSpan().SequenceEqual(other._bytes);

        public override bool Equals(object? obj) => obj is ByteKey other && Equals(other);

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.AddBytes(_bytes);
            return hash.ToHashCode();
        }
    }
}
