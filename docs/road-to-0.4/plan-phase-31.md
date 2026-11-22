# Phase 31: throughput, the ceiling re-measured, then shared fan-out

**Status:** step 1 measured 2026-11-22; step 2 planned, its decisions open. Phase 31 is the last
phase of 0.4 (owner, 2026-11-21). The ideas and their order are in the
[0.4 README](README.md#ordering); this plan records what step 1 found and cuts step 2 into changes.

## Step 1: the ceiling, re-measured as a game runs

Measured on 2026-11-22; full tables in
[LOAD-TESTING.md](../LOAD-TESTING.md#re-measured-as-a-game-runs-phase-31-2026-11-22). The load tool
gained `--aoi R` (each player subscribes to the chunks within R of it, as 2R+1 `BETWEEN` strips per
table, rescoped on every step) and now runs Server GC by default (`--workstation-gc` reproduces the
old runs).

- **Most of the 200-player wall was workstation GC.** Same shape, Server GC on: p50 call-to-delta
  went from 5.4 s to 0.10–0.13 s, and delivered rows from 90k/s to 206–221k/s. The reference
  game's host is a Web SDK project and already runs Server GC; only the tool did not. The README
  now tells console-project hosts to opt in.
- **In the game's shape the knee is between 600 and 800 players** on this one machine: 16 shards of
  about 12 players at 200, each player seeing a 5x5-chunk square. At 200 players that shape
  delivers a tenth of the rows the ladder's shape does, at a p50 of 14 ms.
- **Fan-out under the write lock is still the wall, at about half the cost.** About 2–3 µs per
  subscriber per commit under Server GC, against 5.5 µs. One shard at 750 commits/s saturates at
  about 400 subscribers instead of 200. Above roughly 180–220k delivered rows a second, more
  players become queueing.
- **The driver on a second machine was not done.** There is one machine. Every number is a lower
  bound, and the bound is tighter at high player counts, where the driver is also busy.
- **Rescope through the gateway works**, which this step had to check before relying on it: a
  rescope is a `Subscribe` frame with an existing id, and the gateway replaces the frame it replays
  at each shard swap.

## Step 2: shared fan-out, still under the lock

Identical subscriptions are one query. Evaluate the group once per op, build its row ops once,
encode them once, and give each member the shared bytes under its own subscription id. The
consistency guarantee stays exactly as it is: deltas are still computed under the write lock,
against the pre-image, in LSN order.

### What a subscriber costs today, per commit, under the lock

Read from `SubscriptionEngine.Fanout` and `MelangeSocketConnection.EnqueueDelta`:

- the predicate and policy verdicts (`Matches`, `PolicyAdmits`, `VisibleColumns`): per subscriber;
- a `WireRowOp`, a `List<WireRowOp>`, two dictionary entries, a `SubscriptionUpdate`, and a
  `TransactionUpdateFrame`: allocated per subscriber;
- `Measure(frame)`, which walks the frame to size it for the backpressure ledger: per subscriber;
- an enqueue and a `SemaphoreSlim.Release` on the connection's send signal, which can wake a
  waiting send loop from inside the lock: per subscriber.

Then, outside the lock, every send loop serializes its own copy of the same rows.

The row bytes and decoded columns are already shared (`WireRowMemo`, `DecodedRow`). What is not
known is how the 2–3 µs divides among the four items above. That decides which of them step 2
builds first, so it is measured before anything is built.

### Changes

- **2a. Apportion the cost.** An in-process benchmark of `Fanout`: one table, N subscribers on
  counting sinks, a fixed commit stream, timed per commit. Then the same with each item stubbed
  out in turn. No product change; the numbers go in this plan.
- **2b. Subscription groups.** At registration, a subscription with no row policy and static wire
  columns joins the group keyed by (table, predicate kind, column, operand values, projection).
  The fan-out evaluates each group once per op and hands the resulting op list to every member.
  Subscriptions with a row or per-row column policy keep the per-subscriber path. A rescope moves
  a subscription between groups under the lock it already holds.
- **2c. Encode once.** A group's ops for one commit are serialized once into a shared segment.
  Each member's frame is a short header (LSN, subscription id) plus that segment, written by the
  send loop. Its length is known without walking it, so the backpressure ledger is still debited
  under the lock, where the drop-and-resync path needs it. **The bytes on the wire stay identical** to
  what the per-subscriber path writes; the conformance corpus and the live suite check that.
- **2d. Wake-ups.** If 2a shows the send signal matters: release it once per connection per commit
  instead of per frame, or after the lock drops.
- **2e. Re-measure** the sweep and the ladder as in step 1, interleaved against the step 1 build.

### Decisions to settle

1. **Wire format.** Leaning: no protocol change. 2c splices a header onto shared bytes and must be
   byte-identical to today's frames. A frame that names a group instead of a subscription would
   save more, and would be a protocol version and a client change in both runtimes, for a gain
   2a has not shown is needed.
2. **What may be grouped.** Leaning: only subscriptions with no row policy and static wire
   columns. Per-row policies are per subscriber by definition. Grouping them by policy identity
   is possible but is the kind of sharing where a mistake leaks a row to the wrong player.
3. **A startup signal for workstation GC.** The hosting guidance is in the README now. The engine
   could also log a warning at startup when the transport runs under workstation GC on a machine
   with many cores, since that is the single largest throughput effect step 1 found. Leaning: yes,
   one warning with an EventId, no setting.
4. **Where step 2 stops.** Leaning: build 2a–2c, then decide on conflation (step 3) from the
   re-measure. Conflation changes what a client sees (fewer, coalesced updates), so it is a
   protocol choice for the owner, not a tuning step.

## Steps 3 to 5

Unchanged from the [README](README.md#ordering): conflation for position-like tables, read-side
fan-out only if step 2 is not enough, and binary node-link frames. Step 1 did not reorder them. The
node link carried every handoff and border batch in these runs (up to 8,227 handoffs in two
minutes) without showing up as a limit, so binary frames stay last.
