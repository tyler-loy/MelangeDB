using BenchmarkDotNet.Attributes;
using MelangeDB.Core;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Does reading the commit log from an LSN pay for where that LSN sits in the file?
/// <para>
/// It should not. Every incremental consumer of the log — a client's resume replay, a replica
/// stream, the Postgres applier's catch-up, the cluster's event and border pumps — reads from
/// "my cursor plus one" and takes a batch, so the cost of reaching the cursor is paid on every
/// batch, forever. <c>ReadFrom</c> used to start at the header and read, CRC-check and decode every
/// record below the one it was asked for; this suite is the same ten-record read at three
/// positions in a log large enough that walking to the window and seeking to it differ by orders of
/// magnitude rather than by noise. Like <see cref="IndexRangeBenchmarks"/>, the <i>shape</i> is the
/// result: <b>a rising line from Low to High is the bug</b>, a flat one is the fix.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LogSeekBenchmarks
{
    private const int Records = 100_000;
    private const int Window = 10;

    private string _root = string.Empty;
    private FileCommitLog _log = null!;
    private ulong _first;

    [Params(RangePosition.Low, RangePosition.Middle, RangePosition.High)]
    public RangePosition Position { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-logseek-").FullName;

        // Interval fsync: the suite measures reads, and a hundred thousand fsyncs would be the setup.
        _log = new FileCommitLog(new CommitLogOptions { Path = _root, FsyncPolicy = FsyncPolicy.Interval });
        var arguments = new byte[256];
        var op = new RowOp(RowOpKind.Insert, TableId.FromName("Bench"), new RowKey([1, 2, 3, 4, 5, 6, 7, 8]), new byte[64]);
        for (var i = 0; i < Records; i++)
            _log.Append(new CommitRequest(new Timestamp(i + 1), Identity.Hash("bench"), "bench", arguments, [op]));
        _log.FlushToDisk();

        _first = Position switch
        {
            RangePosition.Low => 1,
            RangePosition.Middle => Records / 2,
            _ => Records - Window + 1,
        };
    }

    /// <summary>Ten records from the chosen position — one consumer's batch.</summary>
    [Benchmark(Description = "read ten records from an LSN")]
    public int ReadTenFrom()
    {
        var count = 0;
        foreach (var record in _log.ReadFrom(_first))
        {
            if (++count == Window)
                break;
        }

        return count;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _log.Dispose();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A handle Windows has not released yet; the temp reaper gets it.
        }
    }
}
