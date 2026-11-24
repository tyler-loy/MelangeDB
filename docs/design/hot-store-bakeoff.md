# Hot-store engine bake-off

**Status:** measured 2026-11-23.

**Decision, by the owner, 2026-11-23:**
- **Ship all three engines** behind `HotStore:PagedEngine`: `Faster` (still the default), plus
  `Tsavorite` and `MelangeLog` as previews.
- **Keep `Faster` as the default for now.** A minor release does not change the default.
- **`MelangeLog` is the likely future default.** It measured best overall below, and it has no
  vendored or native dependencies. Before it becomes the default it must reclaim log space (#287) and
  flush in the background. Tsavorite is the fallback if that slips.

## Why

The paged hot store keeps paged rows in `Microsoft.FASTER.Core` 2.6.5. FASTER's repository has said
"no longer actively maintained" since August 2026, and its last NuGet release was May 2024. Its
successor, Tsavorite, lives inside microsoft/garnet and is not published on NuGet. Before the pinned
package breaks on a future .NET, this bake-off asked which engine should replace it.

Rewriting the core in Rust, or writing our own engine, were ruled out first. Reducers run in-process
as DI-resolved C#, so a Rust core puts a native boundary under every `ctx.Db` access. And the
measured bottlenecks (GC mode, gateway, handoffs, fan-out) were never in storage.

## What was compared

| Engine | How it ships | Shape |
| --- | --- | --- |
| FASTER 2.6.5 (baseline) | NuGet, unmaintained | Hybrid log, hash index, in-place updates |
| Tsavorite (garnet `dabb3de`) | Source only; vendored in `src/MelangeDB.Tsavorite` (MIT) | FASTER's successor: same model |
| LMDB 1.0 via LightningDB 0.24.0 | NuGet with native libraries | Copy-on-write B+tree in a memory map |
| RocksDB 11.12 via the `RocksDB` package | NuGet with native libraries | LSM tree |
| ZoneTree 2.2.0 | NuGet, pure managed | LSM tree |

SQLite was excluded by the owner. Garnet as a server was not tested; its storage engine is
Tsavorite, which was.

## How

FASTER is used for one narrow job: key → bytes for paged rows (the main space) and for large
bytes-column payloads (the blob space). The spike put exactly that behind an internal seam,
`IPagedKv` (read, batched read, upsert, delete), in `src/MelangeDB.Storage.Faster/PagedKv.cs`.
FASTER's implementation moved to `FasterPagedKv.cs`. Everything else in `FasterHotStore` stays
shared: the key directory, secondary indexes, residency, read views and blob splitting. Each
candidate is one adapter: LMDB, RocksDB and ZoneTree in `bench/MelangeDB.HotStoreBakeoff`, and the
shipped ones (Tsavorite, MelangeLog) in `src/MelangeDB.Storage.Faster`, so every engine runs under identical
store logic. The refactor is behaviour-preserving: the 117-test FASTER suite passes unchanged.

A store is a projection rebuilt from the commit log on every start, so every adapter opens empty,
with the engine's own durability off: no WAL (RocksDB, ZoneTree), `MDB_NOSYNC` and a write
transaction committed every 1,024 writes (LMDB), no checkpoints (FASTER, Tsavorite).

**Correctness.** `MELANGE_TEST_PAGED_ENGINE=<engine>` runs the whole store test suite against that
engine (faster, tsavorite, melangelog; the bake-off-only engines were run through the same suite via
the spike's harness). That covers lookups, scans, index ranges, read views, residency moves, recovery, and the
#137 blob-shrink case.

**Performance.** `dotnet run -c Release --project bench/MelangeDB.HotStoreBakeoff` drives each
engine, each in its own process, through the same `IHotStore` workloads:
- a rebuild of 1M entity rows;
- updates and reads over a 10k-row hot set, and uniform over the whole table;
- a 90/10 update/read mix;
- full and index-window scans;
- 100k deletes;
- 4 KB terrain rows (the blob space): inserts, random reads, 1–8 KB rewrites that grow and shrink,
  and a full scan.

Every run ends by reading 25,000 rows back byte for byte. The memory budget is 64 MiB.

Two environments, because the first one turned out not to be fair:
- **Windows, uncapped** (32 cores, 126 GB RAM). LMDB, RocksDB and ZoneTree read through the OS file
  cache, which here holds everything. FASTER and Tsavorite stay inside their 64 MiB budget and read
  from disk unbuffered. "Cold reads" therefore means a disk read for two engines and a memory read
  for three.
- **Linux container capped at 1 GiB** (8 cores, no swap). Under cgroup v2 the file cache counts
  against the limit, so every engine shares the same memory. The terrain set is 4× larger here
  (200k rows, about 800 MB), so the blob space cannot fit in cache.

## Results

### Correctness

| | FASTER | Tsavorite | LMDB | RocksDB | ZoneTree |
| --- | --- | --- | --- | --- | --- |
| FASTER test suite | 117/117 | **117/117** | 115/117 | 115/117 | 115/117 |
| Bake-off read-back | yes | yes | yes | yes | yes (Windows); **out of memory** (capped) |

LMDB, RocksDB and ZoneTree each fail the same two tests. Those tests assert that reads went to disk
using the engine's fault signal, which these three engines do not expose. No behaviour failed.

### Linux, 1 GiB cap: the fair comparison

| Measure | FASTER | Tsavorite | LMDB | RocksDB | ZoneTree |
| --- | ---: | ---: | ---: | ---: | ---: |
| rebuild 1M rows (s) | 2.39 | 2.98 | 2.30 | 2.60 | 2.77 |
| hot updates (ops/s) | 208,881 | 213,719 | 215,020 | 180,889 | 235,090 |
| uniform updates (ops/s) | 110,777 | 103,839 | 93,388 | 104,749 | 110,062 |
| hot reads (ops/s) | 1,192,775 | 1,933,284 | 1,906,714 | 548,971 | 407,989 |
| cold reads (ops/s) | 100,535 | 243,055 | 397,877 | 152,308 | 288,520 |
| cold read p99 (µs) | 98.4 | 10.2 | 4.68 | 19.1 | 5.91 |
| mixed 90/10 (ops/s) | 108,628 | 120,426 | 212,468 | 162,582 | 230,776 |
| full scan (rows/s) | 798,122 | 1,178,591 | 2,202,339 | 890,300 | 2,052,049 |
| index windows, ~256 rows (scans/s) | 2,915 | 3,904 | 6,760 | 3,040 | 7,222 |
| deletes (ops/s) | 280,413 | 278,894 | 292,272 | 246,745 | 319,810 |
| blob inserts (ops/s) | 133,378 | 69,790 | 101,055 | 101,934 | out of memory |
| blob reads (ops/s) | 2,919 | 5,081 | 2,699 | 6,557 | — |
| blob rewrites 1–8 KB (ops/s) | 128,919 | 67,981 | **2,273** | 92,607 | — |
| blob rewrite p99 (µs) | 21.2 | 25.1 | 987 | 28.3 | — |
| blob full scan (rows/s) | 74,137 | 101,497 | 29,774 | **6,689** | — |
| working set at end (MiB) | 717 | 558 | 917 | 650 | — |
| disk at end (MiB) | 922 | 913 | 1,683 | 931 | — |

The entity table is about 100 MB, so in this round it still fits the file cache for the engines that
use one. The blob space does not fit for any engine, and that is where the engines separate.

### Windows, uncapped

Selected rows. The full output is reproducible with the command above.

| Measure | FASTER | Tsavorite | LMDB | RocksDB | ZoneTree |
| --- | ---: | ---: | ---: | ---: | ---: |
| hot updates (ops/s) | 191,412 | 205,503 | 154,285 | 143,754 | 173,335 |
| uniform updates (ops/s) | 122,407 | 125,134 | 70,952 | 104,886 | 100,964 |
| hot reads (ops/s) | 1,427,794 | 1,646,214 | 1,691,436 | 440,866 | 734,275 |
| cold reads (ops/s) | 14,192¹ | 15,943¹ | 415,580 | 105,986 | 269,435 |
| blob rewrites (ops/s) | 159,568 | 170,194 | 40,862 | 90,603 | 105,992 |
| working set at end (MiB) | 977 | 981 | 1,373 | 932 | 1,323 |

¹ An unbuffered disk read inside the 64 MiB budget, against a memory read for the other three.

## Findings

- **Tsavorite is FASTER, maintained, and mostly faster.**
  - Same model, same results through the whole test suite, same fault reporting.
  - Under the cap it beats FASTER on every read measure: cold reads 2.4× with p99 at 10 µs against
    98 µs, full scans 1.5×, blob reads 1.7×. It also used 160 MiB less memory.
  - It matches FASTER on entity writes.
  - In the round above, blob inserts and rewrites ran at about half FASTER's rate. That came from
    the device, and is fixed by choosing the right one; see "Which Linux device" below.
- **LMDB reads superbly and collapses on rewrites under memory pressure.**
  - With the blob space out of cache, every rewrite copies whole pages: 2,273 rewrites/s with p99
    near 1 ms, and about 1.8× the disk of the log-structured engines.
  - Game state is update-heavy, so this is the wrong trade.
  - Its single-writer MVCC model would have been the attraction (it could replace the read-view
    overlay), but that never comes into play if the write path cannot hold up.
- **RocksDB is a solid middle, and it needs tuning.**
  - Untuned, its reads were 30–70× slower (level 0 piling up behind a WAL-less write rate). Keeping
    indexes out of the block cache and adding compaction threads fixed that.
  - It is still the slowest blob scan by far under the cap (6,689 rows/s).
  - It is native, so it adds a platform matrix.
- **ZoneTree is fast while memory is plentiful, and cannot keep to a budget.**
  - Its in-memory segment is sized by record count, not bytes, so 800 MB of 4 KB values ran a
    1 GiB container out of memory twice.
  - It is disqualified as configured.
- **Tsavorite gotchas worth knowing** (found by the adapter's self-check):
  - **The hash index has a hard ceiling.** Overflow buckets are capped at
    `IndexOverflowThreshold`% of the main table, and past that Tsavorite throws. FASTER let chains
    grow. At FASTER's current sizing (budget / 1024 buckets), the default 300% throws near 1.64M
    keys. The adapter sets 6400%; adoption should size the index from the expected row count, or
    use `GrowIndexAsync`.
  - **The stock key hash suits our key shape poorly.** It barely varies the high bits for keys that
    share the table-id prefix. The adapter uses `Utility.GetHashCodeWithMix`. FASTER 2.x uses the
    weak hash, which may also be costing the shipped store; that was not tested.
  - **The #137 shrink-in-place contract** is handled by Tsavorite's default writer, with no custom
    code.

## Which Linux device

The rounds above ran Tsavorite on Linux through the managed `RandomAccess` device, because Garnet's
native device needs its prebuilt `libnative_device.so`. Those binaries are now vendored too: the
linux-x64, linux-arm64 and musl builds, in both variants (io_uring + libaio, and libaio only), under
`src/MelangeDB.Tsavorite/Device/runtimes/`. The device is a choice in the adapter (`TsavoriteDevice`).
The rounds below use the same 1 GiB cap, in an image with `libaio1t64` and `liburing2` installed.
Docker's default seccomp profile blocks io_uring, so these rounds ran with seccomp unconfined.

| Capped, Linux | FASTER | native libaio | native io_uring | native libaio, buffered | managed | managed, buffered |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| blob inserts (ops/s) | 134,963 | 163,756 | 157,638 | **147,781** | 94,837 | 90,987 |
| blob rewrites (ops/s) | 126,356 | 114,355 | 112,441 | **113,128** | 68,146 | 67,168 |
| blob reads (ops/s) | 2,878 | 2,414 | 2,452 | **5,630** | 2,863 | 5,561 |
| cold reads (ops/s) | 50,098–151,510 | 7,901 | 7,814 | **90,586** | 224,058 | 246,190 |
| mixed 90/10 (ops/s) | 98,329–115,538 | 55,613 | 19,012 | **168,761** | 122,509 | 122,113 |
| full scan (rows/s) | 839,012 | 90,076 | 96,071 | **883,143** | 1,177,575 | 1,188,766 |
| working set at end (MiB) | 713–758 | 567 | 570 | **555** | 795 | 547 |

The FASTER column spans two runs, and its cold reads moved threefold between them. Read single
cold-read figures as noisy.

- **The native device is what fixes writes.** The managed device writes at about 60% of the native
  rate, buffered or not.
- **Unbuffered native I/O costs reads.** Every read outside the 64 MiB budget goes to disk, even
  with free memory in the container to cache it: cold reads drop to about 8k/s and scans to about
  90k rows/s. The managed "unbuffered" device's reads were evidently served from the page cache,
  which is why it looked better on reads in the first round.
- **The native device with OS buffering on is the one to ship on Linux.** It writes at native
  speed, reads through the page cache, has the best mixed workload, and the smallest working set.
  It matches FASTER within noise everywhere else. Rewrites are about 10% lower; hot reads are a
  little lower and cold reads trail FASTER's better run.
- **io_uring buys nothing here, and costs on the mixed workload.** Docker also blocks it by default.
- **Using the page cache is a memory-policy choice, not only a speed one.** Unbuffered keeps a
  shard's memory at its budget; buffered lets the OS lend free memory and take it back under
  pressure. Under cgroup v2 that cache counts against a container's limit and is reclaimable. It
  could be a setting (`HotStore:OsCache`). Linux defaulting to buffered matches what the other
  candidates did implicitly.
- Windows keeps Tsavorite's `LocalStorageDevice`. The uncapped Windows round used it and matched
  FASTER, which also runs unbuffered there.
- **Not measured:** the arm64 binaries (the Raspberry Pi runner), and whether Windows gains from
  buffering the way Linux does.

## Can LMDB's writes be fixed?

LMDB's reads were the best of any engine, so it got its own tuning round under the same 1 GiB cap
(`LmdbTuning` in the adapter). The variants were:
- page size: 16 KB and 64 KB, so 4 KB terrain values sit in leaf pages instead of overflow pages;
- `MDB_WRITEMAP` with `MDB_MAPASYNC`;
- 8,192 writes per transaction instead of 1,024;
- two combinations of these.

| Capped, Linux | Tsavorite | LMDB | writemap | 8k txn | 16 KB page | 64 KB page | tuned 16 KB | tuned 64 KB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| blob rewrites (ops/s) | **116,691** | 2,224 | 1,244 | 2,534 | 2,997 | 2,572 | 2,444 | 2,334 |
| blob rewrite p99 (µs) | **25.1** | 970 | 1,623 | 964 | 676 | 731 | 1,015 | 962 |
| blob inserts (ops/s) | 139,341 | 108,987 | 85,485 | 90,488 | 133,262 | **193,869** | 87,258 | 126,474 |
| uniform updates (ops/s) | **108,134** | 91,994 | 94,735 | 93,442 | 64,870 | 32,381 | 71,872 | 69,800 |
| cold reads (ops/s) | 84,640 | **425,833** | 403,203 | 405,603 | 373,906 | 368,720 | 385,292 | 379,499 |
| full scan (rows/s) | 888,946 | 2,263,553 | **2,350,885** | 1,871,851 | 2,302,212 | 2,304,252 | 2,155,448 | 2,334,201 |
| mixed 90/10 (ops/s) | 168,099 | 186,064 | 189,552 | 230,354 | 194,789 | 190,876 | **237,606** | 195,167 |
| working set at end (MiB) | **561** | 944 | 941 | 976 | 950 | 907 | 941 | 898 |
| disk at end (MiB) | **913** | 1,683 | 3,072 | 1,790 | 1,708 | 1,200 | 3,072 | 3,072 |

**No setting moves blob rewrites off the floor.** Every variant does 1,200–3,000 a second, 40–90×
below Tsavorite, at about 1 ms p99. That is structural, not tuning. A B+tree update is
read-modify-write: to rewrite a row whose page is not in memory, LMDB first reads that page from
disk, then copies it and its path to the root. Tsavorite's update to a record below the in-memory
region is a blind append at the log tail; it never reads the old value. Larger pages help inserts
(64 KB beat Tsavorite) and scans, but cost uniform updates (64 KB: 32k/s) because each update copies
a bigger page. `MDB_WRITEMAP` grew the file to 3 GB and slowed writes.

What LMDB does better is real. Where the data fits the page cache (the 100 MB entity table here),
its reads are 2–5× Tsavorite's, because a memory-mapped read is a pointer dereference while
Tsavorite's buffered read is a system call and a copy. But the paged store exists for data that does
not fit in memory, and that is exactly where LMDB's writes collapse. Data that fits is better served
by a resident table. **LMDB is not a contender for this store.** If Tsavorite's read path is to
improve, the levers are its own: a larger budget, read-copy into the mutable region for hot records,
or reading straight from the device buffers.

## Our own engine: `melangelog`

The bake-off's two winning traits are Tsavorite's blind-append writes and LMDB's memory-mapped
reads. No off-the-shelf engine combines them, so an MVP was built to try:
`bench/MelangeDB.HotStoreBakeoff/MelangeLogPagedKv.cs`, about 860 lines.

**Design:**
- **Log and memory.** An append-only log, with an in-memory ring of pages in native memory sized to
  the budget (64 KB pages for Main, 1 MB for Blob).
- **Writes.** A record still in the ring is rewritten in place when it fits, which keeps the #137
  exact-length contract. Otherwise the new version is appended blind; a record that has left memory
  is never read back.
- **Reads.** Pages that leave the ring are written to 64 MB segment files, which are memory-mapped.
  A read below the ring is a pointer into that map. Batched reads ask the OS for all their pages at
  once.
- **Index.** An open-addressing hash index in native memory that grows by rehash, with no ceiling.
  Records chain to the version they replaced, so a 64-bit hash collision is still exact.
- **Integrity.** A per-record XxHash3 checksum, so a bad read throws rather than returning wrong
  bytes.

It passes the FASTER suite 117/117, including the two tests that assert reads went to disk. It also
passes a randomized self-check against a dictionary, run with forced hash collisions.

| Capped, Linux | FASTER | Tsavorite | LMDB | melangelog |
| --- | ---: | ---: | ---: | ---: |
| uniform updates (ops/s) | 115,405 | 113,996 | 94,122 | **131,789** |
| hot reads (ops/s) | 1,724,954 | 1,749,342 | 1,967,617 | **2,402,925** |
| cold reads (ops/s) | 120,705 | 86,107 | 422,291 | **445,636** |
| cold read p99 (µs) | 66.8 | 33.1 | 4.57 | **4.55** |
| mixed 90/10 (ops/s) | 112,420 | 166,424 | 195,706 | **242,483** |
| full scan (rows/s) | 843,061 | 893,935 | **2,277,844** | 2,102,712 |
| blob inserts (ops/s) | 145,510 | 140,625 | 113,521 | **154,646** |
| blob insert p99 (µs) | **6.81** | 10.9 | 7.40 | 18.0 |
| blob reads (ops/s) | 2,771 | 5,514 | 2,612 | **7,230** |
| blob rewrites (ops/s) | **149,892** | 110,988 | 2,226 | 108,782 |
| blob full scan (rows/s) | 80,604 | 74,606 | 29,477 | **141,135** |
| managed heap at end (MiB) | 379 | 378 | 288 | **284** |
| working set at end (MiB) | 763 | **550** | 933 | 992 |
| disk at end (MiB) | 922 | 913 | 1,683 | 1,024 |

Across three capped runs, melangelog's cold reads ranged 446k–463k, blob reads 7.2k–7.3k, and blob
rewrites 107k–112k.

- **Reads match LMDB and writes match Tsavorite.** The design worked as intended: cold reads 5× and
  scans 2.4× Tsavorite's, and blob rewrites 49× LMDB's.
- **It is the best of the four under memory pressure** for blob reads and blob scans. Its
  managed heap is lowest, because its pages are native memory.
- **It loses on write tail latency.** Blob insert p99 is 18 µs against 7–11 µs, because a 1 MB page
  flush runs synchronously on the writing thread. FASTER is still best at blob rewrites.
- **Its working set is the largest.** Mapped pages count in RSS, as LMDB's do; under cgroup v2 that
  is the same reclaimable page cache a buffered engine uses.

**What production would need, in order:**
1. **Compaction**, about 1–2 weeks. The log only grows (1 GiB on disk by the end of the run). Note
   that the shipped FASTER store never compacts either, so its disk use grows with uptime on
   update-heavy paged tables. That gap exists today, whichever engine is chosen.
2. **Background flushing**, 3–5 days. This removes the insert p99. It needs a read-only region
   between the mutable ring and disk.
3. **A smaller, incrementally resized index**, 2–3 days: 8-byte entries instead of 16, and a rehash
   that does not stop the world (10–20 ms at 1M keys today).
4. **Honest disk accounting**, about a day. Segments are preallocated, which on NTFS reserves the
   whole 64 MB.
5. **Platform polish:**
   - On musl, prefetch silently does nothing, because `madvise` is looked up in `libc.so.6`.
   - arm64 is untested.

It has no native dependencies, vendored source or `libaio`. The price is that we own a storage
engine.

## Recommendation

Three things are shipped: the engines behind `HotStore:PagedEngine`, Tsavorite vendored into
`src/MelangeDB.Tsavorite` and carried inside the `MelangeDB.Storage.Faster` package, and CI running
the store suite on each engine. Before `MelangeLog` becomes the default:

1. **Reclaim log space** ([#287](https://github.com/tyler-loy/MelangeDB/issues/287)). Every engine
   needs this, FASTER included: the shipped store's disk use grows with write volume until a restart.
   For MelangeLog it is segment compaction: track live bytes per segment, then re-append and drop the
   emptiest. Estimated at 1–2 weeks.
2. **Flush in the background.** This removes MelangeLog's blob-insert p99 (18 µs against 7–11 µs).
3. **Run the cluster and load-test suites** on each engine, not just the store suite. Measure on
   linux-arm64 (the Raspberry Pi runner) and Windows.
4. **A smaller, incrementally resized index.** MelangeLog uses 16-byte entries and rehashes everything
   at once (10–20 ms at 1M keys).

For Tsavorite, should it be needed:
- **Index sizing.** Size the index from the row count, rather than relying on the raised overflow
  threshold.
- **Page cache setting.** Decide whether the page cache should be a setting (`HotStore:OsCache`).

What these results mean for a deployment on 0.5.0's defaults, with the later native-directory and
overnight soak numbers, is in [PERFORMANCE.md](../PERFORMANCE.md).

Out of scope here, and worth a later look on its own: engine-native capabilities this harness
deliberately did not use. The main one: MelangeLog could keep each record's address in the store's
key directory and drop its own index, and later move that directory off the managed heap (about
350 MiB for 1M rows, the same for every engine).

## Reproducing

```
# all engines, Windows (or any host)
dotnet run -c Release --project bench/MelangeDB.HotStoreBakeoff -- --rows 1000000 --blob-rows 50000 --budget-mb 64

# the capped round
dotnet publish bench/MelangeDB.HotStoreBakeoff -c Release -r linux-x64 --self-contained false -o out
docker run --rm --memory 1g --memory-swap 1g --cpus 8 -v "$PWD/out:/app:ro" -w /tmp \
  mcr.microsoft.com/dotnet/runtime:10.0 /app/MelangeDB.HotStoreBakeoff \
  --rows 1000000 --blob-rows 200000 --budget-mb 64 --root /tmp/bakeoff

# the Linux device round: an image with the native device's dependencies
#   FROM mcr.microsoft.com/dotnet/runtime:10.0
#   RUN apt-get update && apt-get install -y --no-install-recommends libaio1t64 liburing2
docker run --rm --memory 1g --memory-swap 1g --cpus 8 --security-opt seccomp=unconfined \
  -v "$PWD/out:/app" -w /tmp <that image> /app/MelangeDB.HotStoreBakeoff \
  --engines faster,tsavorite,tsavorite-unbuffered,tsavorite-uring,tsavorite-ra,tsavorite-ra-buffered \
  --rows 1000000 --blob-rows 200000 --budget-mb 64 --root /tmp/bakeoff

# correctness, per engine (faster, tsavorite, melangelog)
MELANGE_TEST_PAGED_ENGINE=tsavorite dotnet test tests/MelangeDB.Storage.Faster.Tests
```
