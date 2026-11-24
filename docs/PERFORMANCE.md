# Performance

What to expect from the paged hot store and the cluster's node links in 0.5.0, what changes when
you operate it, where the limits are, how to measure on your own hardware, and how to switch back.

Every number here was measured on the project's own hardware and is linked to the run it came from.
None of them is a guarantee. They show the shape of the change; your hardware sets the size of it.
Re-run the tools under [Measuring on your hardware](#measuring-on-your-hardware) before you size a
deployment from them.

## Summary

0.5.0 changes three defaults:

| Change | 0.4 | 0.5.0 | Setting |
| --- | --- | --- | --- |
| Engine under the paged store | `Faster` (Microsoft.FASTER.Core, unmaintained upstream since August 2026) | `MelangeLog`, MelangeDB's own append-only log | `HotStore:PagedEngine` |
| Key directory and secondary indexes of paged tables | Persistent trees on the managed heap | Native copy-on-write B+trees, 1 KiB pages | `HotStore:NativeDirectory` |
| Row data on cluster node links | JSON | Binary | none; always on |

**What to expect.** Reads are faster: cold point reads several times FASTER's under memory
pressure, and scans about twice. Writes are level or better. Disk is bounded, at about twice the
live data, where FASTER's grew with write volume until a restart. A paged row's resident
bookkeeping is roughly a tenth of what it was, and none of it is on the GC heap. Bulk traffic
between cluster nodes allocates about a quarter as much and takes a fraction of the time.

**The caveats, up front:**
- **The two store changes were measured separately**, not together against 0.4's defaults. The
  engine bake-off ran MelangeLog against FASTER over the managed directory; the directory round ran
  native against managed, both on MelangeLog. Their effects should compound, but the combination
  has a single end-to-end run so far ([End-to-end check on 0.5.0](#end-to-end-check-on-050)).
- **The bake-off is a single-process microbenchmark** in a 1 GiB container with 8 cores. The
  paging soak is four nodes in one process on one 32-core machine, driver included. Neither is a
  production topology.
- **MelangeLog is the only engine here that MelangeDB maintains itself.** That is the point of
  switching, and it also means the engine is younger than FASTER. The store test suite runs against
  both engines in CI.
- **MelangeLog primary keys are limited to about 1 KiB.** See [Limits](#limits-and-behaviour-changes).

## Throughput and latency

### Engine: MelangeLog against FASTER

From the [bake-off](design/hot-store-bakeoff.md#our-own-engine-melangelog): 1M entity rows and 200k
terrain blobs of 4 KB, 64 MiB budget, a Linux container capped at 1 GiB, each engine in its own
process. The 1 GiB cap matters: under cgroup v2 the OS file cache counts against the limit, so every
engine pays for its memory the same way. Selected rows:

| Measure | FASTER | MelangeLog |
| --- | ---: | ---: |
| uniform updates (ops/s) | 115,405 | 131,789 |
| hot reads (ops/s) | 1,724,954 | 2,402,925 |
| cold reads (ops/s) | 120,705 | 445,636 |
| cold read p99 (µs) | 66.8 | 4.55 |
| mixed 90/10 (ops/s) | 112,420 | 242,483 |
| full scan (rows/s) | 843,061 | 2,102,712 |
| blob reads (ops/s) | 2,771 | 7,230 |
| blob rewrites (ops/s) | 149,892 | 108,782 |
| blob insert p99 (µs) | 6.81 | 18.0 |

**Why reads win.** A record that has left memory sits in a memory-mapped segment file, so reading it
is a page fault into the OS page cache rather than a system call and a copy. FASTER reads below its
in-memory region through an asynchronous device read.

**Why writes hold level.** An update to a row still in the in-memory ring is made in place; an
update to a row that has left memory is appended without reading the old version. That is FASTER's
write model too.

**Where MelangeLog is behind.** Blob rewrites ran about 27% below FASTER, and blob insert p99 was
18 µs against 7 µs, because at the time a 1 MB page flushed on the writing thread. Flushing has since
moved to a background flusher; the insert tail was not re-measured after that change.

Cold-read figures are noisy: FASTER's moved threefold between two runs of the same configuration,
and MelangeLog's ranged 446k–463k over three. Compare shapes, not single cells.

### Key directory: native against managed

Same bake-off, MelangeLog, 1M indexed rows plus 200k blobs, 1 GiB cap, two runs each
(`--native-directory off` for the managed column):

| Measure | Managed trees | Native trees (1 KiB pages) |
| --- | ---: | ---: |
| bookkeeping memory after rebuild | 279 MiB managed | 50 MiB managed + 39.5 MiB native |
| rebuild 1M rows (s) | 2.4–2.7 | 1.5 |
| hot reads (ops/s) | 2.0M | 2.6–2.9M |
| cold reads (ops/s) | 0.49M | 0.79–0.84M |
| index window scans (scans/s) | 5.3–5.7k | 7.6–8.2k |
| full scan (rows/s) | 1.74M | ~2.1M |
| hot updates (ops/s) | 239k | 234–242k |
| uniform updates (ops/s) | 119k | 146–156k |
| deletes (ops/s) | 221–278k | 267–285k |
| churn updates (ops/s) | 114k | 148–156k |

**Why reads and rebuild get faster.** The managed directory was a tree of small heap objects; the
native one keeps keys packed in 1 KiB pages, so a lookup touches a few contiguous pages instead of
chasing references, and the GC has no per-row objects to mark during a rebuild.

**Why the page size is 1 KiB.** The tree is copy-on-write: an update copies the path from leaf to
root, and every copied internal page re-references all of its children. Smaller pages make that copy
cheaper. Hot updates by page size:

| Page | 8 KiB | 4 KiB | 2 KiB | 1 KiB | 512 B |
| --- | ---: | ---: | ---: | ---: | ---: |
| hot updates (ops/s) | 95k | 146k | 161k | 234–242k | 262k |

512 B was about 9% faster still and used about 9% more memory; 1 KiB was chosen.

### Rebuild and recovery

The hot store is a projection, rebuilt from the commit log on every start. Under the bake-off,
rebuilding 1M rows took 2.39 s on FASTER and 1.5 s on MelangeLog with the native directory. A real
start also replays the commit log from the last snapshot, which these figures do not include; see
[`failover`](LOAD-TESTING.md#the-reassignment-window) for recovery measured end to end. Cold reopen
time on 0.5.0's defaults was not separately measured.

## Memory

### The budget model

`HotStore:MemoryBudgetBytes` (128 MiB by default) caps the paging buffer pool: MelangeLog's
in-memory ring of pages. Resident tables are accounted separately. Under 0.4 there was a third
cost that neither covered: the key directory and secondary indexes, on the managed heap, sized by
row count rather than by the budget. In 0.5.0 that cost is native memory and much smaller, but it
still grows with row count. A paged deployment's resident memory is roughly:

- the budget (the ring);
- plus resident tables (the residency report);
- plus the native directory, about 30–60 bytes per paged row with one index;
- plus whatever of the segment files the OS keeps mapped.

### What a paged row costs now

| | Managed directory (0.4) | Native directory (0.5.0) |
| --- | --- | --- |
| Per paged row, key directory plus one index | about 300 bytes | about 30–60 bytes |
| GC objects per paged row | about 5 | 0 |
| Freed | when the GC collects it | when the last reader of that version finishes |

**Reclamation is deterministic.** A superseded version's pages are freed as soon as its last reader
is done. Read views and lazy key walks hold their version until they finish, so a long-running scan
holds native memory proportional to what changed during it.

**Full collections get cheaper.** With the directory off the heap, a gen2 collection no longer marks
an object graph proportional to the paged row count.

### Reading the metrics

| Watch | Where | What it tells you |
| --- | --- | --- |
| Native directory size | `melange.store.directory.native_bytes`, `HotStoreStatistics.NativeDirectoryBytes` | The bookkeeping that used to be on the GC heap. Grows with row count. |
| Ring | `melange.store.memory_bytes` (`space`) | The budgeted pages. Should sit at or under the budget. |
| GC heap | your runtime metrics | **Will shrink** after upgrading. Dashboards that alert on GC heap should not read that as data loss. |
| Working set / RSS | your host | Includes MelangeLog's mapped segment pages. Those are file-backed and reclaimable, the same page cache a buffered engine uses; under cgroup v2 they count against a container's limit. |

**GC heap is not live data, especially under Server GC.** With Server GC on many cores the reported
heap can sit well above what is live. The overnight soak below held 4–5 GiB of GC heap with churn
against ~250 MiB without it. A dotMemory snapshot of a paging soak taken before the binary link
explained a large part: 322 MB of a 647 MB managed heap sat in `ArrayPool<byte>.Shared` per-thread
caches from the node link's JSON framing. Binary framing and a bounded link-owned pool remove that.
How much of the rest is Server GC's latitude rather than retention has not been settled. To compare
two builds, follow the [settled managed heap method](LOAD-TESTING.md#methodology-and-caveats), not
working set.

## Disk over time

FASTER never reclaimed log space: every update of a row that had left memory appended a new record
and kept the old one, so disk grew with write volume until a restart. MelangeLog compacts while it
runs. It copies a segment's live records forward and deletes the segment, emptiest first, in steps
of about 1 MiB that hold the store lock only for the copy. It runs while a space's log is more than
`HotStore:Compaction:TriggerRatio` (2.0) times its live bytes.

From the [overnight paging soak](LOAD-TESTING.md#paging-soak--melangelog-against-faster-under-constant-rewrites-2026-11-24):
four nodes in one process, 100 walkers, 400,000 paged rows of 1 KiB (about 50× an 8 MiB budget),
1,000 rows rewritten a second.

| | MelangeLog (2.7 h) | FASTER (1.8 h) |
| --- | --- | --- |
| Primary shards' paged log | flat at ~2.55 GiB from minute 20 | 1.4 → 22 GiB, linear |
| Hot-store disk, primaries and replicas | 5.4 → 5.5 GiB | 0 → 53.5 GiB (~29 GiB/h) |
| Reclaimed by compaction | 36.7 GiB over 44,530 steps | none |
| Call → delta p50 / p99 / max | 2.9 / 211 / 2,352 ms | 3.1 / 470 / 19,417 ms |
| Rejected calls | 4,244 | 34,325 |
| Replica staleness, max | 1.1 s | 14.9 s |
| Disconnects / inconsistencies | 0 / 0 | 0 / 0 |

The latency tail and replica staleness improved too. The run does not isolate why; FASTER's
growing log, a growing amount of disk I/O on the same box as everything else, is the likely
contributor.

### Sizing rule of thumb

```
hot-store disk ≈ live bytes × TriggerRatio × copies + preallocated segments
```

- **Live bytes** are what the paged tables hold, **including border-band mirrors** in a spatial
  cluster (see [Clusters](#clusters)). `melange.store.live_bytes` reports it.
- **TriggerRatio** is 2.0 by default. Lower keeps disk closer to live and spends more disk writes
  on compaction getting there; it cannot go below 1.1.
- **Copies** is each shard's primary plus `Cluster:ShardReplicas`; 1 on a single node.
- **Preallocated segments:** MelangeLog preallocates 64 MiB segment files, one ahead of the one
  being written, so disk runs about a segment ahead of the log per space. Add the in-memory ring
  and a few segments of slack.

The soak checks out: 1.3 GiB live × 2 × 2 ≈ 5.2 GiB, against 5.4–5.5 GiB measured.

This covers the hot store only. The commit log and snapshots are separate; see
[Limits](#limits-and-behaviour-changes) for the snapshot byte trigger.

**Pressure valves.** `HotStore:Compaction:MaxLogBytes` is a soft cap on the paged logs: above it,
compaction runs as hard as it can, down to a ratio of 1.1, and EventId 1519 warns. Free disk below
`HotStore:Compaction:MinFreeDiskBytes` (1 GiB) does the same, with EventId 1518. Neither refuses
writes. If the disk fills anyway, the failing write closes the database to writes (EventId 1517)
until a restart. `HotStore:Compaction:MaxBytesPerSecond` caps compaction's own disk writes.
`melange.store.log_bytes`, `live_bytes`, `compaction.reclaimed_bytes` and EventIds 1514–1516 show
it working. All of these are in [CONFIGURATION.md](CONFIGURATION.md) and
[OBSERVABILITY.md](OBSERVABILITY.md).

## Clusters

### Node-link framing

Node links carried row data as JSON through 0.4; in 0.5.0 they carry it as binary. From
[`link-bench`](LOAD-TESTING.md#the-node-links-framing--json-against-binary-2026-11-24), loopback,
one process:

| Message | Round trip | Allocated | On the wire | Heap held after a full GC |
| --- | --- | --- | --- | --- |
| Border batch, 500 × 1 KiB rows | 4.5 → 0.49 ms | 4.5 → 1.1 MB | 763 → 520 KB | up to 64 MiB → ~0 |
| Border batch, 50 × 64 B rows | 3.5× faster | −70% | −45% | — |
| Heartbeat | — | −31% | — | — |

**Why.** JSON encoded each row's bytes as text and rented large buffers from the shared
`ArrayPool`, whose per-thread caches then held them. Binary framing writes the bytes as they are,
and the link owns a bounded pool of its own.

**What it does to a soak.** In the paging soak A/B the p99 moved from 224/210 ms to 196/194 ms, and
the managed heap at the end was about 10% lower. The bigger effect shows under a heap limit: with a
2 GiB GC heap limit, the JSON link ran out of memory while seeding the 400,000 rows and served no
calls. The binary link seeded them and served 218,000 calls before the key directory, still managed
in that run, ran out. The native directory is the fix for that second limit. A run with both under
the same cap is pending ([End-to-end check on 0.5.0](#end-to-end-check-on-050)). Older nodes in a
mixed-version cluster keep exchanging JSON; see [Upgrading](#upgrading-to-050).

### Border-band mirroring multiplies live bytes

A partitioned row within `Cluster:BorderBandChunks` of a block edge is mirrored into the neighbouring
shard, and paged there like any other row. In the soak, with a 3-chunk band on 8-chunk blocks, six
columns and six rows in eight qualify, so a row is stored (1 + 6/8)² ≈ 3.06 times on average. 421 MiB
of raw ballast read as 1,297 MiB live across the primaries. **Size disk and memory from the stored
total, not the raw rows.** Small blocks with a wide band multiply the most.

### Replica lag

Replica staleness in the paging soak peaked at 1.1 s on MelangeLog and 14.9 s on FASTER. That is one
run of each, with the driver on the same machine. Replica lag on 0.5.0's full defaults has not been
measured separately from that run; see [Replica lag under the soak](LOAD-TESTING.md#replica-lag-under-the-soak-phase-30-2026-11-21)
for how it is measured.

## Limits and behaviour changes

**Primary keys are limited to about 1 KiB under MelangeLog.** The engine stores keys of at most
1,024 bytes, and the store prefixes each with 4–5 bytes, so the usable limit is about 1,019 bytes.
A new longer key is refused before it reaches the commit log
([#307](https://github.com/tyler-loy/MelangeDB/issues/307)), so nothing durable is lost. **A log
written under FASTER that already holds such a row cannot be rebuilt into MelangeLog**: start that
deployment with `HotStore:PagedEngine = Faster`.

**Segments are preallocated.** Each 64 MiB segment file is reserved when it is created, one ahead
of the one being written. A fresh store therefore shows disk use before it holds much data, and disk
runs a segment ahead of the log.

**Flushed records are read through memory maps** (`HotStore:MelangeLog:ReadMode = Mmap`). A disk
*read* error on a mapped page is a signal (SIGBUS on Linux) that ends the process; it restarts and
rebuilds, as after a crash. The other engines throw instead. Under `Pread`, a read error is an
exception for the one row (EventId 1511 on a scan), and cold reads are slower. Choose `Pread` where
a process restart costs more than read speed. Each segment is mapped whole, so a very large log
(about 4 TiB at Linux's default `vm.max_map_count`) needs that raised, or `Pread`.

**Snapshots also trigger on log bytes.** `Snapshots:IntervalBytes` (256 MiB) takes a snapshot once
that many bytes have been appended since the last one, even short of `Snapshots:IntervalTransactions`.
Nodes that commit rarely but in bulk now truncate their commit logs: out-of-world shards taking only
border mirrors, and blob-heavy tables. The paging soak saw the commit log grow about 5.7 GiB/h on
both engines before this change. Whether the byte trigger accounts for all of that has not yet been
re-measured.

**Native memory is outside the GC.** `GCHeapHardLimit` and `DOTNET_GCHeapHardLimit` no longer cover
the paged store's directory. A container's memory limit still does.

## Upgrading to 0.5.0

Before:
- [ ] **Check primary key lengths** on paged tables. Any row whose encoded key exceeds about 1 KiB
  keeps you on `HotStore:PagedEngine = Faster` until it is gone.
- [ ] **Check disk headroom** against the [sizing rule](#sizing-rule-of-thumb). A FASTER deployment
  that restarts often may never have shown its unbounded growth; MelangeLog's steady state is about
  twice the live data per copy, plus segments.
- [ ] **Decide `ReadMode`.** Keep `Mmap` unless a process restart on a disk read error costs more
  than slower cold reads.
- [ ] **Raise `vm.max_map_count`** if a single store's log could approach terabytes.

After:
- [ ] **Watch `melange.store.directory.native_bytes`** grow with row count and settle.
- [ ] **Expect the GC heap to shrink**, and adjust alerts that treat a smaller heap as anomalous.
- [ ] **Watch `melange.store.log_bytes` against `live_bytes`.** The ratio should settle near
  `TriggerRatio`. EventIds 1518 and 1519 mean compaction is under pressure.
- [ ] **In a cluster, roll nodes as usual.** Each node rebuilds its hot store on start, so mixed
  engines during a rollout are fine. A node advertises binary frames when it authenticates and each
  end switches only once the other reads them, so an older node keeps exchanging JSON.

### Switching back

Both store changes are a restart. The hot store is rebuilt from the commit log on every start, so
there is nothing to migrate in either direction:

| To restore | Set |
| --- | --- |
| FASTER under the paged store | `HotStore:PagedEngine = Faster` |
| The managed key directory and indexes | `HotStore:NativeDirectory = false` |

They are independent; either can be set without the other. `Tsavorite`, a preview
engine before 0.5.0, was removed; its bake-off numbers are in
[design/hot-store-bakeoff.md](design/hot-store-bakeoff.md). The binary node link has no switch.

## Measuring on your hardware

**The engine bake-off.** One engine, with the native directory on or off:

```
dotnet run -c Release --project bench/MelangeDB.HotStoreBakeoff -- \
  --engines melangelog --rows 1000000 --blob-rows 200000 --budget-mb 64 --ops 1000000 \
  [--native-directory off]
```

`--engines` takes a comma-separated list (`faster,melangelog`) and runs each in its own process,
then prints the comparison table. Run it inside a memory-capped container for numbers like the ones
above; without a cap the OS file cache holds everything and cold reads become memory reads. The
container commands are under [Reproducing](design/hot-store-bakeoff.md#reproducing).

**The paging soak.** A cluster that really pages, under constant rewrites:

```
MelangeDB.LoadTest all --players 100 --nodes 4 --soak --hot-store faster \
  --paged-engine melangelog --budget-mb 8 --ballast-rows 400000 --row-bytes 1024 \
  --ballast-churn 1000 --duration-seconds 3600 --out paging.csv
```

`--hot-store faster` selects the paged store (`HotStore:Engine`), whichever engine is under it. The
serve console's 30-second line reports the hot store's log, live and reclaimed bytes. Run the
`--paged-engine faster` arm in the same session for a comparison, and read the methodology caveats
in [LOAD-TESTING.md](LOAD-TESTING.md#methodology-and-caveats) first.

**The node link alone:**

```
MelangeDB.LoadTest link-bench --repeat 3
```

It measures round trip, allocation and wire bytes per message shape, over loopback in one process.

## End-to-end check on 0.5.0

<!-- CLUSTER-CHECK-RESULTS -->
