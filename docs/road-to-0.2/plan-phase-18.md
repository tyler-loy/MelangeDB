# Phase 18 — Truncation-floor observability

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

### Open: threshold in records, bytes, or time

The pinned quantity is naturally records (LSNs are dense); the operator's fear is bytes; the
truest signal is time-behind-head. Leaning: records for the check's threshold (cheap, exact,
already the unit of `melange.applier.lag`, whose threshold precedent this follows), with bytes
exposed on the gauge's log line since truncation already knows the file length. Time requires
per-LSN timestamps that truncated records no longer have; not worth inventing storage for.

### Open: does the unnamed overload survive

Leaning: keep `AddTruncationFloor(Func<ulong?>)` delegating to the named one with a
`"unnamed"` tag rather than breaking the public API pre-1.0 for a string. The pre-1.0 caveat
permits the break, but the cost of keeping it is one constant, and third-party floors that
never name themselves still show up — as `"unnamed"`, which is itself diagnostic.
