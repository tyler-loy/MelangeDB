# Phase 18 — Truncation-floor observability

**Status: Shipped.**

**Goal:** "why is the log not truncating" — the disk-filling question — answerable in one look:
every truncation floor has a name, the governing floor is visible in metrics and logged at
truncation time, and a pinned log surfaces in the health endpoint before it surfaces as a full
disk.

**Depends on:** nothing; smallest phase of the 0.2 set. Phase 15 sharpened the need — backup
pins are the newest way to hold the log — but every holder listed below predates it.

## Why here

Truncation floors are the mechanism by which everything that still needs old records keeps them:
the Postgres applier's checkpoint, every live event subscriber's checkpoint, the `Resume`
retention window, cluster handoff markers (pending freezes, unsettled imports), the cluster
events cursor, and now backup pins. `AddTruncationFloor` registers an anonymous `Func<ulong?>`;
truncation takes the minimum and says nothing. Every one of these holders is *supposed* to pin
the log — briefly. The failure mode is one of them pinning it for days: a crashed event
subscriber that never checkpoints again, a stalled applier, a handoff marker orphaned by a bug.
Today the operator sees the symptom (log growth) with no path to the cause short of attaching a
debugger. The bus's checkpoint *expiry* (phase 06) exists precisely because an abandoned
subscriber must not pin truncation forever — this phase is the observability that should have
shipped around that mechanism: expiry handles abandonment, and naming handles everything
slower than abandonment.

## Deliverables

**Floors get names.** `AddTruncationFloor(string name, Func<ulong?> floor)` — an additive
overload; every registration in Core, Cluster, and Host updated to pass one (`postgres-applier`,
`event-bus`, `resume-window`, `backup-pin`, `shard-freeze`, `shard-import`, `cluster-events`).
Names are a small static set by construction — they name mechanisms, not instances — which is
what keeps the metric tag below out of the cardinality trap
([OBSERVABILITY.md](../OBSERVABILITY.md)'s standing rule).

**The governing floor is logged.** Every truncation logs, at information level, what was
truncated and which floor governed (name, its LSN, distance behind head). A truncation that
removes nothing *because* a floor pinned it logs the same shape — that is the interesting case,
and today it is perfectly silent.

**The gauge.** `melange.log.truncation_floor` tagged by floor name (per engine, like every
engine metric), plus the derived headline `melange.log.pinned_records` — head minus effective
floor, the number that grows when something is wrong. Dashboards alert on one number and drill
into the tag.

**The health check.** `melange-retention`: unhealthy when the pinned distance exceeds a
threshold (`HealthChecks:RetentionPinnedThreshold`, in records), naming the governing floor in
its description — EventId-grade information in health-endpoint form, the `melange-applier`
pattern. Healthy trivially when snapshots/truncation are not configured. The applier already has
its own lag check; this one exists because the applier is only one of seven holders and the
other six have nothing.

**Documentation.** The new rows in [OBSERVABILITY.md](../OBSERVABILITY.md) and
[CONFIGURATION.md](../CONFIGURATION.md); *truncation floor* is promoted from code comment to
[GLOSSARY.md](../GLOSSARY.md) noun; a short "the log is growing — who is holding it" runbook
section in the operations docs, written as the sequence of looks the operator actually performs.

## Out of scope

**A `/melange/status` JSON endpoint.** Tempting — one URL an operator curls — but it is a new
privileged read surface with the full gating baggage (`Enabled`/`OwnerRole`/assertion-flag
ladder, phase 15 walked it), duplicating what metrics + health checks already export through the
conventions every deployment has wired. Recorded as the natural next step *if* a real dashboard
need shows up that scraping cannot serve; metrics are the operator surface until then.
**Automatic floor eviction** — expiring an abandoned holder is policy the bus already owns for
its checkpoints (phase 06); generalizing eviction to arbitrary floors would let observability
grow teeth this phase deliberately does not have. Naming first; policy only with a named victim.

## Decisions to settle

### Settled: the threshold is in records, and the log line carries bytes

As the leaning. `HealthChecks:RetentionPinnedThreshold` counts records — cheap, exact, and the
unit `melange.applier.lag`'s threshold already established — and both truncation log lines carry
`LogBytes` beside `PinnedRecords`, since the file's length is known for free at the moment of the
decision. Time-behind-head stayed out: it needs per-LSN timestamps that truncated records no longer
have.

One thing the plan did not anticipate: the default has to sit **above
`Snapshots:IntervalTransactions`**, because the pinned distance reaches one snapshot interval in
ordinary operation — the head advances between snapshots while the floors are the reading taken at
the last one. The default is 1,000,000 against a 100,000-transaction interval, and the option's doc
says to raise both together.

### Settled: the unnamed overload survives

As the leaning. `AddTruncationFloor(Func<ulong?>)` delegates to the named registration under
`"unnamed"`. The cost is one constant; the benefit is that a third-party floor that never names
itself still appears in the report and the metric, which is itself the diagnosis.

## Shipped notes

- **The floor set is larger than the registration list.** The plan's seven names were the
  `AddTruncationFloor` call sites, but three more mechanisms bound truncation without ever
  registering: the snapshot LSN itself (the ceiling), each applier's checkpoint, and the Resume
  retention window, all applied inline in `TruncateLogCore`. All three are now named floors in the
  same report, because an operator asking "who is holding the log" does not care which of them was
  a registration. `snapshot` governing is what a healthy log looks like: nothing is holding
  anything back. Appliers report under their own applier name (`postgres`, `hot-store`) rather than
  the plan's illustrative `postgres-applier`, so `melange.applier.lag{applier="postgres"}` and
  `melange.log.truncation_floor{floor="postgres"}` name the same thing.

- **The floors are a cached reading, not a live query** — the one design decision this phase turned
  on. Evaluating floors from a metrics scrape is not merely wasteful, it is wrong three times over:
  providers run under the engine write lock by contract (`PinnedTruncationFloor` walks the pin list
  with no further locking), a scrape would race that state, and one registered floor — the
  cluster's borrowed-sidecar refresh — *writes a file* on evaluation, registered as a floor
  precisely so it runs when truncation is being decided. So `TruncateLogCore` stores the whole
  reading and the gauges pair it with the **live** head. That pairing is not a compromise: it is
  what makes `pinned_records` grow while a stuck holder stands still, which is the shape an
  operator alerts on. The cadence is self-correcting — a log that is growing is taking commits, and
  commits are what drive snapshots.

- **Absent beats zero.** Neither gauge publishes anything before the first truncation decision, and
  `melange-retention` reports healthy with an explicit "no truncation has been decided yet". A zero
  would have read as "healthy" on a log nothing has ever compacted.

- **The removed-nothing line is the same shape as the truncated line, on purpose.** From silence an
  operator cannot distinguish a log that is already compacted to its floor from one that has
  stopped compacting; the second fills the disk. `1510 LogTruncationPinned` carries the same
  fields as `1503`, so an alert keying on `FloorName` and `FloorLsn` works across both, and a
  stream of 1510 with one unchanging `FloorLsn` is the diagnosis without further work.

- **The resume window is scanned no harder than before.** Its loop still stops at the floor the
  other holders already set, so when it does not bind, its reading is the ceiling it scanned to —
  "permits removing at least this much". That keeps the tag present in the metric rather than
  flickering in and out, at no extra read cost.

- **Duplicate names resolve to the lowest reading in the metric**, while the report keeps every
  registration. Two shards' freeze markers, or several unnamed third-party floors, would otherwise
  produce a duplicated tag set where the last writer wins arbitrarily.

- Both `/melange/status` and automatic floor eviction stayed out of scope, unchanged from the plan.
