# Phase 23 — The TypeScript client, client conformance, and `melange generate`

**Status: Planned.**

**Goal:** a browser is a first-class MelangeDB client, and "a correct MelangeDB client" stops being
a thing that exists only as C# code. Three deliverables, and **two of them outlive the client**: a
language-neutral generation path, a written conformance contract with a suite behind it, and the
TypeScript client that is the first thing to be held to it.

**Depends on:** nothing new. `melange-schema.json` has been public API at format `1` since phase 12,
and it was designed for exactly this.

**Blocked this phase until it landed:** the not-default predicate,
[#122](https://github.com/tyler-loy/MelangeDB/issues/122), now shipped. Adding a query shape is
cheap while the SQL subset has one implementation and expensive once it has a written contract and
a second one — and the shape had a second, sharper deadline. Callers who could not express it were
denormalising a boolean column into a persistent schema, and columns do not un-denormalise: removing
one is a destructive migration, refused automatically and manual by design, so the rational move
once the workaround exists is to keep it forever. A predicate landing after that point would have
arrived and changed nothing. The rule this leaves behind: **a query-shape change is a pre-phase-23
decision or a no.**

**This is the largest phase in 0.3.** Say so plainly: it is a second implementation of the client
runtime in a language with no analyzer, no shared types, and no test suite to inherit.

## Why here

**Phase 03 bought this option and has carried it unexercised for twenty phases.** Queries are a SQL
subset rather than LINQ specifically because LINQ *"would block the eventual TypeScript client"* —
that plan accepted the cost of owning a parser to keep this door open. An option carried that long
should either be exercised or written off, and writing it off would mean conceding that the wire,
the manifest, and the query language were shaped for a client that never came.

**There is a named consumer.** The reference workload has three client trees — a Godot game, a
terrain-gen CLI, and an **admin web**. That third one is the only consumer in the project that
cannot use the C# bindings, and it is also the one that had a hand-built Postgres scrape worker
deleted out from under it when the port landed.

**And that consumer needs more than the socket.** The admin console's whole reason for existing is
aggregates — `date_trunc('hour', at)`, `COUNT(*)` — which
[REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md) is explicit that subscriptions cannot express.
Those run against `/melange/sql`, over HTTP, in owner mode. `MelangeClient` is websocket-only, so
this is not a gap between the two languages — **neither client library covers it today**, and a
TypeScript package that ships only the socket client would leave its own justifying consumer unable
to finish the job. See the decision below.

**The manifest is already the right shape.** Format-versioned, ordered deterministically, hashed
over content with the generator version deliberately excluded so the hash means *schema* and not
*build*. Public tables only, `[ServerOnly]` columns absent, enums by simple name with `MELANGE0019`
refusing ambiguity. A non-C# generator reads this file and links nothing — which was the stated
design intent, now tested for the first time.

## What a second runtime actually has to implement

The generator is the cheap half; this list is the phase. Every item is a rule the C# client
currently encodes and nothing states as a contract:

- **MessagePack framing**, with the channel tag every frame has carried since version one, across
  the 18 frame types in `Frames.cs`.
- **The `WireDescriptor`, sent on chunk 0 of the initial set and never again**, held for the life of
  the subscription. Lose it and nothing decodes. A subscription's shape cannot change while it
  lives; a schema change means a new epoch and a full re-establishment.
- **The column mask** — per-row, empty meaning every descriptor column is present (the case that
  matters and the one that costs one byte), bit `ordinal` at `mask[ordinal >> 3] & (1 << (ordinal &
  7))`, length `(columnCount + 7) / 8`.
- **Schema-ordered v1 row bytes**, decoded per `ColumnKind` positionally — no names on the wire.
- **The anchor boundary.** An initial set is consistent at `AnchorLsn` and the delta stream carries
  only LSNs greater than it. That single rule is what makes the boundary gap-free and
  duplicate-free, and getting it wrong produces a client that is subtly, intermittently wrong.
- **The merged, refcounted cache.** The server deliberately sends a row once *per subscription* and
  deduplicates nothing across a connection; collapsing that into one cached row and one event is the
  client's job. This is semantics, not an optimization.
- **Resume against a log epoch**, not a bare LSN — `ResumeFrame` carries `EpochId` and
  `LastAckedLsn`, and a resume is only accepted against the same epoch.
- **`DropAndResync` backpressure**, the default over unbounded buffering.
- **Reducer arguments** as a count plus self-describing tagged values — no schema needed to decode.
- **The subscription and resume parameter map**, which is the one thing on the wire that still rides
  as MessagePack *values* rather than as a MelangeDB encoding ([DESIGN.md](../DESIGN.md) §10's
  remaining wire half). One map per subscribe, not per row, which is why it has never been worth a
  break on its own — but this phase is when a second runtime has to implement it.
- **The encoded primary key** — the uniform order-preserving byte key — because `Find` is a lookup
  by encoded key, not by CLR value.
- **Identity is read from the connection, never derived.** `SHA256(issuer|subject)` belongs to the
  server alone; a client that parses its own JWT becomes a second implementation of the one part of
  the contract that must never disagree.

## Deliverables

**`melange generate --lang <lang> <manifest> -o <dir>`.** `ManifestParser` and `ClientModel` are
already language-neutral by construction — hand-rolled JSON, netstandard2.0, no dependencies — and
already sit behind a Roslyn-shaped front door. Lifting them behind a CLI verb is the whole change,
and it is what makes a third language a scoped task rather than a fresh argument.

**`docs/CLIENT-CONFORMANCE.md` — the written contract.** Every rule in the list above, stated as an
obligation with the failure it prevents. This document is the deliverable that matters most in five
years: it is the difference between "port the C# client and hope" and "implement this and run the
suite."

**A wire-level conformance suite.** Runnable against a client in any language, against a real server
over a real socket. The material exists but is C#-shaped and scattered — `HandshakeTests`,
`ProtocolTests`, `WireRowTests`, `ClientRowShapeTests`, `TypedBindingsTests`, `ColumnVisibilityTests`,
`FanoutSharingTests`. The work is turning what those assert into something a non-C# client can be
subjected to.

**The TypeScript client.** A runtime package (framing, connection, subscriptions, the merged
refcounted cache, resume, reducer calls) plus generated typed bindings from the manifest. Browser
first: `WebSocket`, no Node-only APIs in the core. The generated API mirrors the C# shape — the
manifest already fixes table names, column order, enum names, and reducer signatures, so divergence
would be a choice rather than a consequence.

**The C# client is held to the suite on day one.** It defines conformance, so it must pass —
and if writing the contract down reveals a place where the C# client is the odd one out, that is a
finding, not an exemption.

**Documentation.** A TypeScript section in [CLIENT-BINDINGS.md](../CLIENT-BINDINGS.md) alongside the
existing C# one, and the manifest's *"a non-C# client generator becomes possible by reading this
file"* claim upgraded from a design intent to a shipped fact with a link.

## Out of scope

**Any third language.** Python is scoped and declined in
[idea-bin/additional-client-languages.md](../idea-bin/additional-client-languages.md), with its
reopening trigger being a named consumer *once this phase's conformance work exists*. This phase pays
that cost once; it does not spend it twice.

**Reducers in TypeScript.** A guest ABI would reintroduce exactly the statics problem this project
exists to delete — the whole premise is your code in your process with DI. The TypeScript surface is
a client, permanently.

**Client interpolation helpers**, which are their own open question in
[idea-bin/client-interpolation-helpers.md](../idea-bin/client-interpolation-helpers.md) and are not
more urgent because the client is new.

**A Node-specific product.** Node will work as a consequence of the core being dependency-free; it
is not a target with its own packaging story, examples, or support promise in this phase.

**Redesigning the C# client.** Writing the contract down will produce opinions about it. Those
become issues.

## Decisions to settle

### Does C# generation move to the CLI too

The C# path is a Roslyn incremental generator, which is the right tool for C# — it reruns on edit,
integrates with the build, and emits diagnostics. Routing C# through the CLI as well would give one
code path for all languages at the cost of making C# worse.

**Leaning:** it does not move. The CLI and the generator share `ManifestParser` and `ClientModel`;
only the emitters differ, and `ClientEmitter` stays where it is. **Open:** whether the CLI should be
able to emit C# at all for non-MSBuild consumers, and if so, how the two C# paths are kept from
drifting — a byte-identical-output test between them is the obvious answer and also an ongoing tax.

### Whether the conformance suite is C# code or data

A suite authored as C# tests can only ever test a C# client, which defeats the purpose. A
data-driven suite — a recorded corpus of frames with expected decodes, plus a small control protocol
the client under test implements — can test anything, at the cost of inventing that harness protocol.

**Leaning:** data-driven, with the corpus generated from a live server so it cannot drift from the
real wire. **Open:** how much of the suite can be pure decode-corpus (cheap, covers framing, row
bytes, masks, argument encoding) versus how much needs a live connection (handshake, anchor
boundary, resume across an epoch, backpressure). The second set is the valuable half and the
expensive half, and the split decides whether this deliverable is a week or a month.

### How much of the merged cache the TypeScript client owns

The refcounted merge is real work and it is tempting to ship a thinner client that surfaces raw
per-subscription streams and lets the application deduplicate.

**Leaning:** implement it fully. The server's behaviour — send once per subscription, deduplicate
nothing — is only correct *because* the client merges; a thin client would push a correctness
obligation onto every application and would quietly not be a MelangeDB client by the contract this
phase is writing. **Open:** whether the merge is in the runtime package or in generated code; C# puts
the mechanics in `MelangeDB.Client` behind `IClientRowCodec<T>` and keeps emitted code thin, and
matching that is probably right.

### The dispatch-mode analogue

C# has `DispatchMode.Manual` because Godot and Unity forbid scene mutation off the main thread.
JavaScript has one thread and no such hazard, so the mode's original motivation does not transfer.

**Leaning:** default to immediate dispatch and offer a `frameTick()` anyway, for browser engines
that want cache mutation and events to land inside a `requestAnimationFrame` rather than at socket
arrival. The property worth preserving is not thread affinity but the one the C# pump actually
guarantees — that *what the cache says* and *what handlers have been told* advance on the same clock.
**Open:** whether that is worth a second dispatch path in a client that has no threading problem.

### Whether the TypeScript package covers `/melange/sql`

The socket client serves subscriptions and reducer calls. The admin console — this phase's named
consumer — also needs one-shot aggregate queries, which are HTTP and owner-mode
(`Sql:AdHocEnabled` gates the endpoint, `Sql:OwnerRole` is a claim, and owner mode refuses a caller
without it rather than silently downgrading).

**Leaning:** include a thin typed query helper, because the manifest already knows the column kinds
needed to type a result row, and because leaving it out means the phase's justifying consumer still
hand-rolls the part it actually came for. **Open:** whether that helper types results *at all* —
an aggregate's result shape is not in the manifest, since `COUNT(*)` is not a table — which may mean
the honest surface is an untyped row reader plus typed helpers only for `SELECT *`-shaped queries.
Worth noting the C# client does not cover this either, so whatever is decided here is a candidate
for it too rather than a TypeScript-only opinion.

### Whether the parameter map gets pinned or changed

`SubscribeFrame.Parameters` and `ResumeSubscription` still carry MessagePack values. Until now that
has cost nothing, because there has only ever been one client.

**Leaning:** pin it, do not change it. A wire break to save one map per subscribe is not worth it,
and protocol v2's break was justified by per-row cost that this does not have. What the phase *must*
do is write the encoding down in the conformance contract as a frozen decision with that reasoning
attached, so the next person to notice it finds an answer instead of an omission. **Open:** whether
the value set is closed — the conformance document has to enumerate exactly which MessagePack value
shapes a parameter may take, and that enumeration does not exist anywhere today.

### Release and versioning

[RELEASING.md](../RELEASING.md) is explicit: all packages ship together at one version, no
per-package versioning.

**Leaning:** the npm package carries the same version string and is published by the same release,
so a `0.3.0` server, `0.3.0` NuGet packages, and `0.3.0` on npm are one artifact set. **Open:**
whether npm publication belongs in the existing pipeline or a parallel one, and what the story is
when a release is C#-only — under one version scheme, an unchanged TypeScript client still gets a
version bump, which is either honest or noise depending on who is asked.
