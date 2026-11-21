# Phase 30, step 2: the warm replica and the term-segmented log

**Status:** planned 2026-11-21; decisions settled the same day. Steps 0 (the fence fixes, #214–#218) and 1 (detection: exact
deadlines, opt-in fast failover, the liveness ping, defaults 250 ms / 5 s) are built. The
[design record](../design/shard-ha.md) settles the shape; this plan cuts it into changes and lists
what is still open.

## Why here

With step 1, a killed owner's shards are writable again in 5.9–6.2 s at a 100k-row shard, and
1.5–1.6 s with `Cluster:FastFailoverOnLinkLoss`. In the fast case the reopen is now most of the
window, and it grows with the shard: 7 s at 1M rows on FASTER. A warm replica removes it. And
two things the design depends on are not built yet:

- **The storage fence.** Today two writers are kept apart by `melange.lock`, which only holds on
  storage that honours exclusive locks across hosts. The term-segmented log makes a paused old
  owner harmless on any shared filesystem. It is also what would let fast failover be on by
  default.
- **The lease re-check after durability.** Commit guards run before the append. Nothing re-checks
  the lease after `WaitDurable`, so an owner that pauses between the two can still acknowledge a
  write that became durable after its lease ended. Read from the code on 2026-11-21: the five
  `WaitDurable` call sites in `MelangeEngine` return straight to the caller.

## What is already true

- A shard is a directory on shared storage: `shard-k/log` holds `melange.log`, `melange.base`,
  `melange.epoch`, `melange.snapshot`, the shape sidecar, and `melange.lock`. `shard-k/hot` is the
  FASTER store's scratch (`deleteOnClose`). Reassignment copies nothing.
- `FileCommitLog` is one file per directory. Its format (magic, 8-byte frames with CRC) is shared
  with backup capture (`DataDirectoryCapture`, `SharedStorageCapture`), the read-only walker
  (`LogFileFormat`), restore (`ArchiveRestore`), and `RestoreCheck`. Truncation copies the
  surviving bytes to a new file and swaps it in.
- The fencing token in membership increases on every reassignment. Nodes learn it with each
  assignment; it is checked on hub-to-node control messages only.
- The engine's constructor does all of recovery in one pass: the log, the shape guard (which may
  write its sidecar), the store, the snapshot, the tail, then the appliers.
- The AutoInc sequencer continues from the shard's own log, by observing every replayed record.

## Deliverables

Five changes, in order. Each ships on its own and leaves the system working.

### 2a. The lease re-check after durability

- Every commit path checks the commit guards' lease again after `WaitDurable` and before it
  returns. A commit that became durable after its lease lapsed reports **outcome unknown**, a new
  transient rejection the client may retry, never success.
- The same check gates each egress that tells another party a commit happened: border batches and
  resets, forwarded events, handoff replies, and later the replica's ceiling. Reads already gated
  by `DurableLsn` stay as they are.
- A test pauses an owner between append and acknowledgement past its lease and asserts the caller
  sees outcome unknown.

Small, and independent of everything below. It closes the window the cut relies on.

### 2b. The term-segmented log, behind a shard storage interface

- **Segments per term:** an owner under fencing token *t* appends to `melange.log.t`. A small
  manifest (`melange.manifest`, written to a temp file and renamed) lists the segments in order,
  each with its term, first LSN, and **cut**: the last LSN that belongs to the log.
- **Opening under term *t*** reads the manifest. For every earlier segment without a cut, it reads
  that segment to its last valid frame, records that LSN as the cut, and writes the manifest.
  Recovery then reads each segment up to its cut, and appends go to `melange.log.t`. A paused old
  owner that resumes writes past a cut nobody reads, and 2a means none of those writes was
  acknowledged.
- **A non-clustered engine** runs as term 0 forever: one segment, one manifest entry. It behaves as
  today.
- **Migration:** an existing `melange.log` becomes the first manifest entry as it stands, with no
  rename. The next open under a higher term cuts it like any other segment.
- **Truncation** deletes whole segments below the base. Within the live segment it keeps today's
  copy-and-swap.
- **Backup, restore, `RestoreCheck`** and the read-only walker learn the manifest. The archive
  format records segments, so an archive taken from a promoted shard restores.
- **The interface:** `IShardStorage` owns the segments, the manifest, the snapshot and the
  sidecars, with the local or shared filesystem as its first driver. It exists so the
  [object-storage driver](../idea-bin/object-storage-driver.md) has somewhere to plug in; nothing
  else implements it in this phase.

The largest piece of the phase. It stands alone: after it, the storage fence holds whatever the
filesystem does with locks.

### 2c. The follower: a warm projection that tails the segments

- A `ShardFollower` owns an `IHotStore`, an AutoInc sequencer, and the shape transform. It
  bootstraps by ordinary recovery done **read-only**: no lock, no torn-tail truncation, no epoch
  minting, no shape adoption. Then it polls the segments for new frames, reopening per poll (NFS
  caches attributes), and applies records only up to a **ceiling** it is given.
- It is not a `MelangeEngine`. It has no commits, subscriptions, timers or appliers, so it cannot
  be mistaken for an owner.
- Falling behind the log's base after a truncation makes it re-bootstrap from the snapshot.
- Its hot store lives in node-local scratch, never under `shard-k/`.
- Promotion hands its store, sequencer state and cursor to the owner's engine (2e).

### 2d. Replica placement and the ceiling

- **Membership** gains the replica node per shard (`ShardAssignment.ReplicaNode`; a column in
  `PostgresMembershipStore`, with its migration).
- **Placement:** `Cluster:ShardReplicas` (default 1, settled) gives each shard a replica on a live
  node other than its owner, fewest-replicas-first. A replica moves when its node dies, and is
  dropped when only one node is left.
- **The ceiling:** the owner reports each shard's `DurableLsn` in its heartbeat load sample
  (already sent every 250 ms). The hub relays it to the replica's node in the heartbeat reply,
  along with that node's replica assignments. The follower applies nothing above it.
- **Lag** is reported as a meter instrument (records and milliseconds behind the ceiling), and the
  load tool prints it.

### 2e. Promotion

- **`MarkDead` prefers the replica:** a dead owner's shard goes to its replica's node under the
  bumped token. A shard without a live replica takes the cold path, as today.
- **On the replica's node,** an assignment for a shard it follows promotes it. The follower stops
  tailing. The owner's engine opens the log under term *t+1*, which cuts segment *t*. It starts
  from the follower's store and cursor instead of the snapshot, replays only from the cursor to
  the cut, and publishes the shard through the existing open path (timers, border publishers,
  forwarders, gateway reattach).
- **The engine gains a second recovery entry point:** start from a supplied warm store and cursor.
  Everything after recovery is unchanged.
- **Measured** with the failover tool, which gains a replica mode: promotion time and the whole
  window at 100k and 1M rows, in-memory and FASTER, plus lag under the 200-player soak.

## Out of scope

- Hub HA and shared-storage HA, as the design record says.
- Serving reads or subscriptions from a replica.
- An object-store driver: only the interface it would implement.
- More than one replica per shard. `Cluster:ShardReplicas` above 1 is refused at startup until a
  second one is worth its memory.

## Decisions to settle

All four settled by the owner on 2026-11-21, each as leaned. The reasoning is kept below.

### Where promotion's engine comes from

**Settled: a separate follower hands its store to a new engine.** The alternative is an engine
with a follower mode that turns into an owner in place. That spreads a third mode through every
path that asks "may I commit?", subscriptions and timers included. The hand-off keeps
`MelangeEngine` owner-only, at the cost of a second recovery entry point.

### Where hot stores live

**Settled: node-local scratch for owners and followers alike.** Today the owner's FASTER scratch
is `shard-k/hot`, on shared storage, though it is deleted on close and never read by another node.
A follower can't use that path, and a promoted follower's store can't move there. A node-local
path (`Cluster:LocalScratchPath`, defaulting to a temp directory) makes both the same and takes
scratch I/O off shared storage. It changes where an owner's store lives, and deployments that size
shared storage for it get that space back.

### Fast failover with a replica

**Settled: keep it opt-in in this phase.** With segments, a live old owner past the cut can't
corrupt the log, and with 2a it can't acknowledge after its lease. But fast failover declares
death *before* the lease ends, and an owner that only lost its hub link still holds a live lease.
It could acknowledge writes that land past the cut, and those would be lost. Taking `melange.lock`
first proves the process is gone only on storage that honours the lock. Turning it on by default
needs a startup probe that proves the storage honours the lock, and that probe is its own change.

### The replica's residency

**Settled: the owner's residency, configurable.** A paged replica halves the memory cost but makes promotion
fault its working set in from disk, which is the reopen cost again in another form. Make it
`Cluster:ReplicaResidency`, defaulting to the owner's, so a deployment short of memory can choose.

## Risks

- **2b touches the on-disk format** that backup, restore and the restore check all read.
  Archives taken before the change must still restore, and the restore check's corpus covers both
  layouts.
- **Tail lag on real shared storage is unmeasured.** The local filesystem says nothing about SMB
  or NFS attribute caching. The lag meter is there so a deployment sees it; the failover tool runs
  locally.
- **Memory doubles** for replicated shards, which works against the elastic layer consolidating
  onto fewer nodes. The design record accepts this; the residency setting is the release valve.

## As built

- **2a** (PR #232): `ICommitGuard.ValidateAcknowledgement`, called after durability on every
  commit path; the shard guard reports `outcome_unknown` once the lease has lapsed. Bulk keeps its
  retryable error.
- **2b**: `LogSegments` (manifest `melange.segments`, sealing at the cut, a read-only
  multi-segment reader) and `FileCommitLog` opened under `CommitLogOptions.Term`, which the shard
  runtime sets to the fencing token. A null term adopts the directory's, so non-cluster engines and
  offline tools are unchanged. Truncation drops sealed segments whole; backup capture, discovery
  and the restore check read the manifest; archives and restore are unchanged.
  **One deviation: no `IShardStorage` interface yet.** With only the filesystem behind it, the
  interface would have been a second name for the same file calls, and the part an object store
  would actually replace (the live segment's append and fsync path) is not something a thin
  interface can hide. `LogSegments` is where the manifest and the sealed segments live, and it is
  the seam a driver would start from. The interface is left to the object-storage idea, which
  would have to design it against a real second implementation.
- **2c**: `LogFollower` (Core, internal) and `LogSegments.Tail`. The follower bootstraps read-only
  (no lock, no truncation, no epoch, no shape writes: `ShapeGuard.Resolve(readOnly: true)`), then
  polls the segments, applying only up to its `Ceiling`. It starts over from the snapshot when a
  truncation passes its cursor, the epoch changes, or the shape sidecar changes. The tail reopens
  files per call and verifies its remembered offset, so a compaction of the live segment is
  re-located rather than misread. Not wired into the cluster yet; that is 2d.
