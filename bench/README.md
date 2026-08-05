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

When a benchmark settles a design decision, record the number **and the decision it settled** in the
document that decision lives in. A benchmark whose result is not written down anywhere is a benchmark
nobody will run again.
