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
