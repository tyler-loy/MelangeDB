# Shard storage on S3 or Azure Blob

**Shape:** a second driver behind the shard storage interface that
[phase 30](../design/shard-ha.md) introduces. Shard logs, snapshots, and small metadata live in an
object store rather than on a shared filesystem. `Cluster:ShardStorage` picks the driver; the
filesystem driver stays the default.

- **The log** is a series of write-once chunk objects, one per group-commit flush, under the
  owner's term: `shard-{k}/log/{term}/{firstLsn}`. Each is created with a create-only-if-absent
  condition (`If-None-Match: *` on S3; an ETag condition on Azure).
- **The manifest** (current term, each closed term's cut LSN, the base LSN) is one small object,
  replaced only with an `If-Match` on the ETag the writer last read. Promotion is a
  compare-and-swap on it, so exactly one new owner wins, and a paused old owner's next chunk write
  or manifest write fails. **The storage enforces the fence**, which a shared filesystem can't.
- **Truncation** deletes chunks below the floor, where the file driver rewrites the file.
- **Snapshots** are multipart uploads; cursors and sidecars are small conditional objects.
- **The hot store stays on local disk.** FASTER's files are already `deleteOnClose` scratch, rebuilt
  on every open.
- **A replica** reads new chunks by the keys the owner reports through the hub, never by listing the
  bucket, which costs more per request.

**Status:** **undecided** (2026-11-21). It came out of settling phase 30's storage fence. The owner
asked what an S3 or Azure Blob driver would cost, and the answer was to build phase 30's segments
and manifest over a storage interface from the start, and leave this driver for later.

## What it costs and what it touches

- **Commit latency.** Each group commit becomes one PUT, so the latency of acknowledging a write
  becomes PUT latency. Rough public figures, **not measured here**:
  - S3 Standard: 20–30 ms p50, 100 ms or more at the tail.
  - S3 Express One Zone and Azure premium blobs: single-digit ms.

  For comparison, local per-commit fsync measured a p90 of 19 ms after group commit
  ([phase 17](../road-to-0.2/plan-phase-17.md)). Throughput holds up, because batches grow. S3
  Standard's tail would show up as stalls players can feel.
- **Request cost.** At the default 50 ms interval, a shard that is always busy makes about 20
  PUTs/s, around 52M a month. At prices remembered rather than checked, that is about $260 a month
  on S3 Standard and about $60 on S3 Express, with Azure's hot tier in the same order as S3
  Standard. Idle shards cost almost nothing. A longer interval trades latency for cost: 200 ms is
  about a quarter of the bill.
- **Recovery.** Many small chunks make recovery a long list-and-read unless chunks are merged in the
  background or snapshots are taken more often.
- **What it touches.**
  - `ICommitLog` is narrow (append, read, durable LSN), but the engine calls `FileCommitLog`
    directly for flushing, truncation, and durability policy.
  - About 20 files use file APIs directly: snapshots, cursor sidecars, dead letters, event
    checkpoints, shape history, epoch lineage, backups.

  Phase 30 moves the log, the manifest, and the snapshot behind the interface. This driver then
  adds the object-store implementation, plus the remaining sidecars.
- **Tests.** Azurite and MinIO in CI. Whether MinIO's conditional-write support matches S3's needs
  checking.
- **The upside beyond portability.** A regional object store is far more durable than an NFS mount,
  so this largely retires the "shared storage is a single point of failure" caveat in
  [design/shard-ha.md](../design/shard-ha.md). S3 Express One Zone is the exception: it lives in a
  single availability zone.

## Reopening trigger

A deployment target without shared filesystem storage, or one where an object store is the
cheaper durable option, **and** a measurement on that cloud: PUT latency p50 and p99 and the
monthly request bill for the reference workload's shard count at its commit rate, compared with
local or NFS fsync on the same cloud. Phase 30 shipping its storage interface is a prerequisite,
not a trigger.
