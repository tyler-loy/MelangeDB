using MelangeDB.Core;

namespace MelangeDB.Testing;

/// <summary>
/// One committed transaction as a test sees it: what the log recorded and what subscribers were
/// sent, with the write set decodable into the game's own row types. Asserting here asserts on
/// the observable contract, not on incidental store state.
/// </summary>
public sealed class WorldCommit
{
    private readonly SchemaRegistry _schema;

    internal WorldCommit(CommitRecord record, SchemaRegistry schema)
    {
        Record = record;
        _schema = schema;
    }

    /// <summary>The record exactly as the log holds it: the escape hatch for anything below.</summary>
    public CommitRecord Record { get; }

    public ulong Lsn => Record.Lsn;

    /// <summary>The reducer that committed it; a scheduled fire's reducer for a tick.</summary>
    public string Reducer => Record.ReducerName;

    public Identity Caller => Record.Caller;

    public Timestamp Timestamp => Record.Timestamp;

    /// <summary>Every row operation on <typeparamref name="TRow"/>'s table, in write-set order.</summary>
    public IReadOnlyList<RowChange<TRow>> Of<TRow>()
        where TRow : struct
    {
        var table = _schema.Get(typeof(TRow));
        if (table.Codec is not RowCodec<TRow> codec)
            throw new InvalidOperationException($"Table '{table.Name}' has no generated codec for {typeof(TRow).Name}.");
        var changes = new List<RowChange<TRow>>();
        foreach (var op in Record.WriteSet)
        {
            if (op.Table != table.Id)
                continue;
            changes.Add(new RowChange<TRow>(
                op.Kind,
                op.Key,
                op.Kind == RowOpKind.Delete ? null : codec.Deserialize(op.Row.Span)));
        }

        return changes;
    }

    /// <summary>The rows of <typeparamref name="TRow"/> this commit inserted.</summary>
    public IReadOnlyList<TRow> Inserted<TRow>()
        where TRow : struct => [.. Of<TRow>().Where(static c => c.Kind == RowOpKind.Insert).Select(static c => c.Row!.Value)];

    /// <summary>The rows of <typeparamref name="TRow"/> this commit updated, as they are after it.</summary>
    public IReadOnlyList<TRow> Updated<TRow>()
        where TRow : struct => [.. Of<TRow>().Where(static c => c.Kind == RowOpKind.Update).Select(static c => c.Row!.Value)];

    /// <summary>How many rows of <typeparamref name="TRow"/> this commit deleted.</summary>
    public int DeletedCount<TRow>()
        where TRow : struct => Of<TRow>().Count(static c => c.Kind == RowOpKind.Delete);

    public override string ToString() => $"LSN {Lsn}: {Reducer} ({Record.WriteSet.Count} row op(s))";
}

/// <summary>
/// One row operation of a write set, decoded. <see cref="Row"/> is the row after the operation,
/// and null for a delete (the log records a delete by key alone).
/// </summary>
public readonly record struct RowChange<TRow>(RowOpKind Kind, RowKey Key, TRow? Row)
    where TRow : struct;

/// <summary>
/// A test kit expectation that did not hold: a call that should have been rejected committed, a
/// fault came back as a rejection, a world that did not survive a restart. Framework-neutral, so
/// any test runner reports it as a failure.
/// </summary>
public sealed class MelangeTestFailure(string message, Exception? innerException = null) : Exception(message, innerException);
