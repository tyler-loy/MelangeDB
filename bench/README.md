# Benchmarks

Measured claims in the design documents come from here, so that "identical container memory" is
reproducible rather than remembered.

```
dotnet run -c Release --project bench/MelangeDB.Benchmarks                          # list the suites
dotnet run -c Release --project bench/MelangeDB.Benchmarks -- --filter '*Container*'
dotnet run -c Release --project bench/MelangeDB.Benchmarks -- --filter '*ReadView*'
```

Add `--job short` for a quick pass while iterating; drop it for numbers you intend to quote.

**These do not run in CI.** They are minutes-long measurements, and the two-vCPU shared runners
produce numbers that indict the runner rather than the code — the same reason the cluster and load
suites stay in the local loop (see [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)). The
project still *builds* in CI, so warnings-as-errors covers it like everything else.

Quote numbers with the machine they came from. A ratio between two rows of the same run travels;
an absolute microsecond figure does not.

## The suites

| Suite | Question it answers | Recorded in |
| --- | --- | --- |
| `HotStoreContainerBenchmarks` | What does making the in-memory store's row container persistent — so pinning a read view is a reference capture rather than a copy — cost the paths that run all the time? | [docs/design/snapshot-isolation.md](../docs/design/snapshot-isolation.md) |
| `ReadViewBenchmarks` | What does a pinned read view cost at the store seam: opening one, holding one open across writes, and reading through one — for both storage engines? | [docs/design/snapshot-isolation.md](../docs/design/snapshot-isolation.md) |
| `CommitPathBenchmarks` | Under interval fsync, where does a commit's time and allocation go — log encode, append, apply, or the whole? | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `FanoutBenchmarks` | One row changed, 1→500 subscribers watching: does fan-out's cost live in matching subscriptions or in producing wire values? The `Predicated` axis puts every subscriber on an indexed-column predicate — the shape that decoded the row per subscriber. | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `ApplyBenchmarks` | What does batching a record's ops into one version publish buy over publishing one per op? | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `IndexMaintenanceBenchmarks` | What does extracting a row's indexed columns cost, and does it scale with the number of indexes? | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `WireFormatBenchmarks` | Bytes, encode, and decode: protocol v1's named column map against the schema-ordered v1 row bytes v2 sends. Keeps the map baseline it retired, because a benchmark that deletes its baseline can no longer say what was gained. | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `FasterHashBenchmarks` | What does the FASTER hash table's size cost when the row count outgrows it? | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `SnapshotBenchmarks` | How long does a snapshot hold the write lock, and how does that scale with the resident set? | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `IndexRangeBenchmarks` | Does a secondary-index range scan pay for **where** its window sits in the key space? | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |
| `LogSeekBenchmarks` | Does reading the commit log from an LSN pay for **where** that LSN sits in the file? Same shape as the index suite: a rising Low→High line is the walk, a flat one is the seek. | [docs/design/performance-sweep.md](../docs/design/performance-sweep.md) |

When a benchmark settles a design decision, record the number **and the decision it settled** in the
document that decision lives in. A benchmark whose result is not written down anywhere is a benchmark
nobody will run again.

## Two things these suites do that the first two did not

**The generator runs here.** The project references `MelangeDB.CodeGen` as an analyzer, so `[Table]`
row types get a real `RowCodec` instead of falling back to the reflection path. Without it, a suite
asking "what does the generated codec cost" would answer a question about reflection — and
`IndexMaintenanceBenchmarks` throws rather than measure the fallback, because a wrong number is worse
than no number. The row types live in [`BenchRows.cs`](MelangeDB.Benchmarks/BenchRows.cs) rather than
nested in each suite for the same reason: the generator only emits for a public or internal type.

**Some of them are shaped so a failure is visible.** `IndexRangeBenchmarks` runs the same window at
three positions in the key space; the absolute numbers say little, but a High row that costs several
times its Low row says the scan is walking the index from the left to find its window. The shape is
the result.
