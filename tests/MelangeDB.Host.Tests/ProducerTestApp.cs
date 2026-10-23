using MelangeDB.Core;

namespace MelangeDB.Host.Tests;

public enum Mood : byte
{
    Calm = 1,
    Loud = 2,
}

/// <summary>A result record: every scalar kind a client sees, an enum, a nullable value, and bytes.</summary>
public sealed record NoteLine(ulong Id, string Text, double Score, Identity Author, Mood Mood, int? Rank, byte[]? Blob);

/// <summary>A struct result type with a field and a property, in that declaration order.</summary>
public struct NoteSummary
{
    public int Count;

    public long Total { get; set; }
}

/// <summary>Nesting: a list of records, a nullable string, a timestamp, and a struct.</summary>
public sealed class NotePage
{
    public List<NoteLine> Lines { get; set; } = [];

    public string? Next { get; set; }

    public Timestamp At { get; set; }

    public NoteSummary Summary { get; set; }
}

/// <summary>Producers over the <see cref="Note"/> table, one per result shape the generator handles.</summary>
public sealed class NoteProducers
{
    [Producer]
    public int Answer(ProducerContext ctx) => 42;

    /// <summary>Async, defaulted parameters, and a nested result — the shape a paged history read has.</summary>
    [Producer]
    public async Task<NotePage> Page(ProducerContext ctx, int size = 10, string? after = null, Mood mood = Mood.Calm)
    {
        await Task.Yield();
        var lines = ctx.Db.Note.Iter()
            .Where(note => after is null || string.CompareOrdinal(note.Text, after) > 0)
            .OrderBy(static note => note.Id)
            .Take(size)
            .Select(note => new NoteLine(note.Id, note.Text, note.Score, note.Author, mood, note.Id == 1 ? 1 : null, null))
            .ToList();
        return new NotePage
        {
            Lines = lines,
            Next = lines.Count == size ? lines[^1].Text : null,
            At = ctx.Timestamp,
            Summary = new NoteSummary { Count = lines.Count, Total = (long)lines.Sum(static l => l.Score) },
        };
    }

    [Producer]
    public ValueTask<string[]> Echo(ProducerContext ctx, string[] words) => new(words);

    [Producer(Name = "Nothing")]
    public NotePage? NoPage(ProducerContext ctx) => null;

    /// <summary>A lazy query over the pinned view, returned unmaterialized: the encoder reads it while the pin is held.</summary>
    [Producer]
    public IEnumerable<ulong> NoteIds(ProducerContext ctx) => ctx.Db.Note.Iter().Select(static note => note.Id);
}

/// <summary>Lets a test hold a producer mid-call while it commits underneath it.</summary>
public sealed class ProducerGate
{
    public TaskCompletionSource Entered { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Release { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Reset()
    {
        Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>Allows a caller only once they have written a note — a policy that reads committed state.</summary>
public sealed class AuthorsOnly : IReducerPolicy
{
    public bool MayCall(string reducerName, PolicyContext context) =>
        context.Db.Note.Author.Filter(context.Caller).Any();
}

/// <summary>Producers that exercise the host: the pin, the timeout, the policy, the no-write rule.</summary>
public sealed class HostedProducers(ProducerGate gate, MelangeReducerHost reducers)
{
    /// <summary>Counts notes, waits for the test to commit more, and counts again: both counts are the pin's.</summary>
    [Producer]
    public async Task<long[]> CountAcrossACommit(ProducerContext ctx)
    {
        var before = ctx.Db.Note.Count;
        gate.Entered.TrySetResult();
        await gate.Release.Task.WaitAsync(ctx.CancellationToken);
        return [before, ctx.Db.Note.Count];
    }

    [Producer]
    public async Task<int> WaitForCancellation(ProducerContext ctx)
    {
        await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
        return 0;
    }

    /// <summary>Ignores its token: the host abandons it at the timeout anyway.</summary>
    [Producer]
    public async Task<int> IgnoreCancellation(ProducerContext ctx)
    {
        await gate.Release.Task;
        return 0;
    }

    [Producer]
    public string Big(ProducerContext ctx, int length) => new('x', length);

    [Producer]
    public int TryToWrite(ProducerContext ctx)
    {
        reducers.Call("AddNote", ctx.Caller, "from a producer", 1.0);
        return 0;
    }

    [Producer]
    public int Refuse(ProducerContext ctx) => throw new RejectedException("No.");

    [Producer(Policy = typeof(AuthorsOnly))]
    public long MyNoteCount(ProducerContext ctx) => ctx.Db.Note.Author.Filter(ctx.Caller).LongCount();
}
