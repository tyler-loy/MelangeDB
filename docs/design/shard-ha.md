# Shard HA: a warm projection over the shared log, behind a fence that holds

**Goal:** when a shard's owner dies, the shard is writable again in a second or two rather than
12–19 s. No acknowledged write is lost, and at no instant can two nodes both make a write to the
same shard durable and acknowledge it.

**Status:** **design settled, not built** (2026-11-21). [Road-to-0.4](../road-to-0.4/README.md) commits phase 30.
The project owner set the bar on 2026-11-20: production systems get HA, and the bar for *not*
building it is high. [Phase 20](../road-to-0.3/plan-phase-20.md) measured the window this record
starts from ([idea-bin entry](../idea-bin/shard-ha-warm-replica.md), the proposal this replaces).
Nothing here is built. The owner settled the decisions at the end on 2026-11-21; they are answered
in place. The next step is a phase plan for step 0.

**Depends on:** [plan-phase-09](../road-to-0.1/plan-phase-09.md) (membership, fencing tokens,
per-shard logs on shared storage), [elastic-rebalancing.md](elastic-rebalancing.md) (the drain,
the ownership move between two live nodes), [snapshot-isolation.md](snapshot-isolation.md)
(pinned read views, which a replica's bootstrap reads through).

## Why

At default timing (heartbeat 1 s, failure timeout 10 s), process death of a shard's owner leaves the
shard unwritable for **12–19 s**, at every shard size phase 20 measured up to 1M rows
([LOAD-TESTING.md](../LOAD-TESTING.md)). Clients don't disconnect; the gateway holds or fails their
calls, so it is a loading screen for everyone on the shard. The window splits as:

| Part | At defaults | What sets it |
| --- | --- | --- |
| Detection | 10.6–13.4 s | `Cluster:FailureTimeoutMs`, plus up to half of it again from the hub's sweep cadence ([#201](https://github.com/tyler-loy/MelangeDB/issues/201)) |
| Reopen | 0.6–7.0 s | Snapshot load plus tail replay on the adopting node; grows with the shard |
| First write | ms | — |

**A warm replica alone removes only the reopen.** Detection is most of the window, and the replica
cannot be promoted before the old owner is provably fenced. So this record is mostly about the
fence, and only partly about the replica.

## What exists, read from the code

Surveyed 2026-11-21. Phase 09's docs describe the intent; these are the mechanics:

- **The shard is a directory on shared storage.** `{Cluster:ShardDataPath}/shard-{k}` holds the
  log, the snapshot, and the cluster sidecars. Reassignment copies nothing: the hub names a node
  and that node opens the same directory and recovers (snapshot, then log tail). The FASTER hot
  store's files are `deleteOnClose` scratch, rebuilt on every open.
- **The lease is per node, and computed on the node.** Each heartbeat reply renews it to
  `now + FailureTimeoutMs`. A commit guard refuses reducer and bulk commits once it has lapsed.
- **Death is timeout-only.** The hub's sweep marks a node dead once `LastSeen` is more than
  `FailureTimeoutMs` old, on a timer of `FailureTimeoutMs / 2`. A closed node link is noticed (the
  hub drops it from its link table) but never acted on.
- **Fencing tokens are checked on hub-to-node control messages only:** drain, reap, handoff,
  execute, bulk. Nothing in the log or storage carries or checks a token.
- **The real guard against two writers is `melange.lock`**, opened `FileShare.None` by the log.
  Windows enforces it natively; on Unix it is an advisory lock, and across hosts it holds only if
  the shared filesystem honours it.
- **Nothing follows another shard's whole log.** The Replicated stream (hub log → every shard,
  Replicated tables only) and border copies (a filtered band of one shard → its neighbour) are the
  nearest mechanisms. Both ship records over the hub's star topology and append them to the
  receiver's *own* log.

## Found while designing: the fence has gaps today

These are bugs in shipped code, independent of HA. Today they are masked, because the adopting node
needs another heartbeat and a full recovery before it can write, and that usually outlasts them.
HA's whole purpose is to remove that delay, so **these are fixed first, as step 0 of the phase.**
Each is filed as an issue.

1. **The node's lease can outlive the hub's death declaration.** The node starts its lease when the
   heartbeat *reply arrives*. The hub may declare death `FailureTimeoutMs` after it *received* that
   heartbeat. So the lease ends later than the hub's earliest declaration, by the hub's processing
   time (a Postgres round-trip with the Postgres membership store) plus return latency. Clock drift
   isn't budgeted, and both sides use wall-clock time. The node also never reads the hub's
   `AuthReply.FailureTimeoutMs`, so a node configured with a longer timeout keeps its lease well past
   the hub's declaration ([#214](https://github.com/tyler-loy/MelangeDB/issues/214)).
2. **Internal commits skip the lease check.** The guard returns early for `CommitOrigin.Internal`.
   Replica batches, border applies, and handoff markers (freeze, import, settle, release, abort)
   all still append to the shard's log after the lease has lapsed ([#215](https://github.com/tyler-loy/MelangeDB/issues/215)).
3. **An expired lease closes nothing.** The node keeps its engines open and holds `melange.lock`, so
   on lock-honouring storage a live but partitioned owner blocks the new owner's open indefinitely.
   On storage that doesn't honour the lock, nothing stops two nodes appending to one `melange.log`
   ([#216](https://github.com/tyler-loy/MelangeDB/issues/216)).
4. **The hub routes border batches, border resets, and forwarded events without checking that the
   sender owns the source shard.** A fenced owner's border publisher keeps feeding neighbours
   ([#218](https://github.com/tyler-loy/MelangeDB/issues/218)).
5. **An exception in the dead-node sweep crashes the hub.** It runs on a bare timer callback, so a
   Postgres error there is an unhandled exception on a thread-pool thread ([#217](https://github.com/tyler-loy/MelangeDB/issues/217)).
6. Smaller: a node marked dead revives silently on its next heartbeat, with no rejoin epoch; and
   the drain discards the origin's reported head LSN rather than checking the destination recovered
   to it.

## The rule that decides everything

> **A replica becomes writable only once the old owner can no longer acknowledge a write. Fencing
> must hold even when every timing assumption fails.**

"The old owner can't acknowledge" is a question of time *and* of storage. Leases answer the time
half. They cannot answer the storage half on their own, because of the paused writer:

> The old owner checks its lease, then the process pauses (GC, a VM migration, a stalled disk)
> past its lease end. The new owner is promoted and appends. The old owner resumes and completes
> its write. If it then acknowledges the client, that acknowledged write is lost or, worse, lands
> in the middle of the new owner's log.

So the fence has two parts, and both are needed:

- **A lease the old owner provably loses before the hub can declare it dead**, re-checked after
  durability and before any acknowledgement or egress.
- **A storage fence**, so that a write the paused owner completes afterwards cannot reach what the
  new owner reads.

## The shape

### 1. The lease, made sound

- **The lease starts when the heartbeat is sent,** read from a monotonic clock, and is shortened by
  a drift margin: `lease_end = send + T × (1 − ε)`. The hub receives that heartbeat no earlier than
  it was sent and declares death no earlier than `receipt + T`, so the lease always ends first.
- **The timeout comes from the hub**, through `AuthReply`. A node whose own setting disagrees logs
  it once and uses the hub's.
- **Every commit origin is fenced**, internal ones included. Recovery replay runs before the shard
  is published, so it is unaffected.
- **The lease is re-checked after durability and before the acknowledgement.** A commit that
  became durable after its lease lapsed reports "outcome unknown", not success. The same check
  gates every egress that tells another party something happened: border batches, events, the
  replica tail, handoff replies.
- **When the lease lapses, the node parks its shard engines** and releases the log lock. It
  reopens them if it re-registers and they are still assigned to it.

### 2. The storage fence: a log segment per term

The fencing token becomes a **term**, written into the log's file layout:

- An owner with term *t* appends only to `melange.log.t`.
- On promotion, the new owner with term *t+1* reads segment *t* up to its last valid frame (the
  *cut*), records the cut in a small manifest, and appends from then on to `melange.log.(t+1)`.
- A paused old owner that resumes writes into segment *t*, past a cut nobody reads beyond. Since
  that write was never acknowledged (the post-durability lease check refuses it), nothing is lost.

This works on any shared filesystem, whatever its lock semantics. `melange.lock` stays as a
same-host guard and a fast-path signal (below), not as the fence. Compaction and backup already
treat the log as files to copy; they learn the manifest.

The segments, the manifest, and the snapshot sit behind a **shard storage interface** from the
start, with the local or shared filesystem as its first driver. The interface costs little now,
and it is where an object-store driver would plug in later
([idea-bin](../idea-bin/object-storage-driver.md)). On S3 or Azure Blob, conditional writes would
make the fence storage-enforced.

The cost is a log-format change in `FileCommitLog`: a manifest, multiple segments, and recovery
across them. It is the largest single piece of the phase. *Decisions to settle* has the cheaper
alternative and what it gives up.

### 3. The warm projection

- **A replica is an engine in follower mode on a second node.** It owns no log. It tails the
  shard's log segments from shared storage, applies records to its own hot store, and refuses
  commits. Its hot store lives on the replica node's local scratch; the owner's `shard-k/hot`
  belongs to the owner.
- **It applies only up to the owner's published durable LSN.** That LSN rides the heartbeat's load
  sample, and the hub relays it to the replica. A record that was written but never made durable
  can vanish with the owner's machine, and a replica that had applied it would be ahead of the log
  it is about to own.
- **It tails the file rather than taking a network stream.** The log is already on shared storage,
  and shipping a hot shard's full log through the hub's star topology would put every commit
  through the hub twice. Tail lag doesn't affect correctness, because promotion replays the
  remainder from the authoritative segments under the new term. Lag only sets how much replay is
  left. Shared filesystems can be slow to show appends to an open handle (NFS caches attributes),
  so the tailer reopens per poll. The lag has to be measured, not assumed.
- **Bootstrap is ordinary recovery, read-only:** the snapshot file plus the tail. A replica whose
  cursor falls behind the log's base after a truncation re-bootstraps the same way. The snapshot
  it bootstraps from is written through the paged scan, which reads a block of rows at a time since
  [#212](https://github.com/tyler-loy/MelangeDB/issues/212): about 30–40 s for a 1M-row FASTER shard, down from 4–5.5 min.
- **Promotion:** the hub assigns the shard to the replica's node under term *t+1*. The replica cuts
  segment *t*, replays from its cursor to the cut, opens segment *t+1*, and publishes the shard.
  The expected cost is the unapplied tail: milliseconds when the replica was current.

The membership store gains one column, the replica node per shard. Placement keeps the replica off
the owner's node and moves it when its node dies, the way the shard itself moves.

### 4. Detection

The fence is what makes faster detection safe. With a sound lease, detection can shrink without
risking two writers; the cost of a false positive becomes a needless promotion, not data loss.

- **Exact deadlines instead of a half-timeout sweep:** the hub schedules each node's expiry at
  `LastSeen + T`, which removes the 0–0.5 T jitter ([#201](https://github.com/tyler-loy/MelangeDB/issues/201)).
- **Shorter defaults once HA is on.** For example heartbeat 250 ms and timeout 2–3 s, chosen from a
  measured false-positive rate and not guessed. #208 showed what a stalled heartbeat looks like; a
  node whose heartbeat shares a thread pool with a heavy shard can pause for seconds. The heartbeat
  loop may need its own thread before the timeout can come down.
- **A fast path for process death.** When the owner's link closes *and* the replica can take the
  shard's `melange.lock`, the owner's process is gone: the operating system dropped both. The hub
  can promote then without waiting out the lease. On storage that honours the lock that takes
  under a second. On storage that doesn't, the fast path is off and the lease decides.
- **Machine death and partitions take the lease path:** about T plus promotion, so 2–3 s at the
  suggested defaults rather than 12–19 s. One caveat to measure: on NFSv4 a dead client's locks
  persist until the server's lease expires (90 s by default). Under today's lock-as-fence design,
  that already means machine death on NFS blocks reassignment for up to 90 s. The term-segment
  fence doesn't depend on the lock, so it doesn't inherit that.

## What it costs

- **Memory.** A replicated shard's resident footprint is paid twice, on two nodes. That works
  against the elastic layer, which consolidates onto fewer nodes. A replica's projection can be
  paged where the owner's is resident, which trades promotion warmth for memory.
- **Shared-storage reads.** Each replica tails its shard's log continuously.
- **The log format.** Segments per term, a manifest, and migration from the single-file layout.
- **A third role in fencing.** Owner, replica, and moving. Every path that today asks "do I own
  this shard?" has to answer it for a replica too.

## Shapes considered

- **Consensus replication (Raft, Paxos).** Refused in phase 09 in favour of Postgres-backed
  membership, for reasons that still hold: shared storage already provides the durable log, and a
  quorum write path would put a network round trip into every commit. SpacetimeDB's Maincloud HA
  takes this road; this project deliberately doesn't.
- **Log shipping with a replica-owned log.** The owner streams records and the replica keeps its
  own copy. If replication is synchronous, every commit waits on a network round trip; if it is
  asynchronous, failover loses the tail. Shared storage gives a zero-loss log without either cost,
  so the replica keeps a projection and the log stays single.
- **A faster cold path only:** shorter timeouts plus the fence fixes, with no replica. That removes
  most of the window (detection), costs no memory, and leaves the reopen, which scales with the
  shard: 6–7 s at 1M rows on FASTER. It is the fallback if the replica's memory cost is refused,
  and the fence work is a prerequisite either way, so it ships first.
- **Serving reads from the replica.** Out of scope: subscriptions need the commit stream at the
  owner, and splitting reads from writes is another design.

## What this deliberately does not fix

- **Shared storage stays a single point of failure.** If the volume goes, every shard goes. HA of
  the storage is the platform's job: replicated volumes, cloud file services. An object-store
  driver would largely retire this ([idea-bin](../idea-bin/object-storage-driver.md)), and the
  storage interface above is what leaves room for it.
- **The hub stays a single point of failure.** Shard HA keeps shards writable while the hub is up.
  When the hub dies, membership freezes: no failover, no rebalance, and once nodes' leases lapse,
  every shard is fenced. Hub HA (a standby hub over the same Postgres) is a separate question, and a
  real one, because of that last effect. It is recorded here so no one assumes this phase covers it.
- **The cold path stays.** Shards without a replica, and replicas still bootstrapping, recover as
  today, only faster because of the detection work.

## Decisions to settle

Settled by the owner on 2026-11-21; each answer is recorded first, with the question kept below it
for the reasoning.

1. **The storage fence: term segments, or the lock?** **Settled: term segments.** The cheaper
   alternative keeps the single log file and makes `melange.lock` the fence. That requires shared
   storage that honours cross-host exclusive locks: a documented deployment requirement plus a
   startup probe where the hub has one node hold a lock and another try to take it. It saves the
   log-format change. It gives up storage-agnostic safety: the paused writer is only as fenced as
   the filesystem's lock is honest. On NFS it also inherits the up-to-90 s wait after machine death.
2. **Which shards get a replica?** **Settled: on by default** (`Cluster:ShardReplicas = 1`) whenever
   the cluster has two or more shard nodes, consistent with HA being the default, with `0` to opt
   out per deployment. The alternative is opt-in per strategy, so only the shards that need it pay
   for it.
3. **Default timing once HA is on.** **Settled: decided from a measurement** (heartbeat 250 ms with
   a 2–3 s timeout as the starting hypothesis, plus a false-positive soak under load) rather than
   picking numbers now. This includes whether the heartbeat loop gets a dedicated thread.
4. **Ordering.** **Settled:** step 0 (the fence fixes found above) ships first as its own change,
   then detection (#201 plus the fast path), then the replica. Each step stands alone, and steps 0–1
   alone cut the window to a few seconds with no memory cost.
5. **The step 0 findings.** **Settled: filed as issues**, the way phase 20's were:
   [#214](https://github.com/tyler-loy/MelangeDB/issues/214)–[#218](https://github.com/tyler-loy/MelangeDB/issues/218).
