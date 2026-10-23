namespace MelangeDB.Transport.Tests;

/// <summary>A chunk as a client sees it through a producer: its id and position, never its bytes.</summary>
public sealed record ChunkView(long Id, long X, Identity AskedBy);

/// <summary>Lets a test hold a producer mid-call and see whether its token fired.</summary>
public sealed class ProducerProbe
{
    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>Producers the transport tests call over the socket and over HTTP.</summary>
public sealed class TransportProducers(ProducerProbe probe)
{
    [Producer]
    public List<ChunkView> Chunks(ProducerContext ctx, long minX = 0) =>
        ctx.Db.Chunk.X.Filter(minX, long.MaxValue)
            .OrderBy(static chunk => chunk.Id)
            .Select(chunk => new ChunkView(chunk.Id, chunk.X, ctx.Caller))
            .ToList();

    /// <summary>Runs until its token fires — a disconnect or the timeout — and records that it did.</summary>
    [Producer]
    public async Task<int> Hang(ProducerContext ctx)
    {
        probe.Entered.TrySetResult();
        try
        {
            await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            probe.Cancelled.TrySetResult();
            throw;
        }

        return 0;
    }
}
