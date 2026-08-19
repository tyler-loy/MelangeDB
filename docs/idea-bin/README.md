# The idea bin

Things that might be worth building, kept where they can be argued with.

This is **not** a roadmap and **not** a deferral record. Those two exist already and mean specific
things:

- [ROADMAP.md](../ROADMAP.md) is what shipped and what is planned. Its **Known deferrals** section
  is a list of *decided refusals* — each one has reasoning behind it, and several are permanent.
- [road-to-0.1/](../road-to-0.1/) and [road-to-0.2/](../road-to-0.2/) are phase plans: written
  before the work, kept afterwards as the decision record.
- [design/](../design/) holds worked-through designs for things being built or explicitly re-deferred.

What lives here is the third category: **undecided**. Not refused, not scheduled, not designed. An
idea in this directory has been thought about enough to have a shape, and not enough to have a
verdict.

## The rule

Every entry carries three things, and an entry missing any of them should be deleted rather than
kept:

1. **A shape.** What the API or mechanism would actually be. "Area-of-interest subscriptions" is a
   wish; `SubscribeAroundAsync(x, z, radius)` desugaring to the existing range predicate is a shape.
2. **What it costs and what it touches.** Enough to tell a small idea from a subsystem.
3. **A reopening trigger.** The measurement, the named consumer, or the event that turns this from
   an idea into a phase. Without one, an idea sits here forever and the directory becomes a
   graveyard that nobody reads and nobody prunes.

The trigger is the load-bearing part. This project's habit is to record *why* something is not being
built — an idea with no stated trigger has not met that bar yet.

## What this directory must not do

It must not soften a recorded refusal. If `ROADMAP.md` says something is deferred with reasoning,
that reasoning stands until the trigger fires; a bin entry proposing a shape for it is a proposal
against a live decision, not a quiet reversal. Where the two overlap — currently
[shard-ha-warm-replica.md](shard-ha-warm-replica.md) — the entry links the deferral and the deferral
links the entry, and neither restates the other's argument.

## The bin

| Idea | Shape in one line | Trigger |
| --- | --- | --- |
| [area-of-interest-subscriptions.md](area-of-interest-subscriptions.md) | `SubscribeAroundAsync(x, z, radius)` over the existing range predicate | A second consumer reinventing the linearized-key pattern |
| [shard-ha-warm-replica.md](shard-ha-warm-replica.md) | A warm replica per hot shard, pre-warmed projection, takes the lock on fence | The reassignment window measures user-visible |
| [additional-client-languages.md](additional-client-languages.md) | A thin Python client — connect, SQL, call — not a peer of the C# one | A named consumer, once client conformance exists |
| [engine-integration-packages.md](engine-integration-packages.md) | Official Godot / Unity packages wrapping `DispatchMode.Manual` | Someone shipping a game hits a wrapper bug we could have owned |
| [client-interpolation-helpers.md](client-interpolation-helpers.md) | Buffer-and-interpolate helpers — the stated answer to "no UDP" | The interpolation answer gets challenged with a real jitter measurement |
| [read-side-fanout.md](read-side-fanout.md) | Move subscription fan-out off the write lock | Phase 20 shows subscriber count, not commit rate, is the crowded-shard limit |
| [dev-host-hot-migration.md](dev-host-hot-migration.md) | `dotnet watch` migrates in place instead of dropping clients | The reconnect loop costs more than the fix |

Six of these came from an outside review of the project, each claim checked against the code before
it was written down here; the seventh (additional client languages) came out of scoping the
TypeScript client. Two items from that review are **not** here because they are already recorded
refusals with reasoning in [ROADMAP.md](../ROADMAP.md): sharding the hub, and
interpolation-instead-of-UDP as a transport question. A third — a stock admin console — was added to
that deferrals list rather than to this bin, because it is decided.
