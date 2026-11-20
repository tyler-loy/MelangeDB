# Phase 28 — Clients that outlive a deploy

**Status: Planned.**

**Goal:** a client built against schema *N* keeps working against a server running an additive
change to *N*: added columns at any position, added tables, added reducers. A client that loses its
connection gets it back without the application writing a retry loop. Both are about one thing:
**a deployed client surviving events it did not cause.**

**Depends on:** nothing hard. **Lands before [phase 23](../road-to-0.3/plan-phase-23.md)**, which
writes the client contract down for a second runtime. See [the 0.4 README](README.md#ordering).

## Why here

**A shipped game client cannot be redeployed in lockstep with its server.** The reference
workload's Godot client is a build on players' machines. Today every additive server deploy breaks
every subscription of every client not yet updated. `ClientRowShape.Verify` compares the server's
descriptor to the bindings by name, kind, *and position*, and throws `MelangeSchemaMismatchException`
on any difference, including an added column. [MIGRATION.md](../MIGRATION.md) is honest about it:
"regenerate bindings with the deploy." That is correct for a web app and wrong for a game.

**SpacetimeDB fixed half of this in 2.16.1** (#6091): old TypeScript and C# clients now skip
columns *appended* to a table, using server-provided row boundaries. Adding a column anywhere else
still breaks old clients. **MelangeDB can do strictly better, and cheaply**, because the descriptor
every subscription receives already carries each column's name and kind. Nothing about the wire
has to change for a client to map by name and skip what it does not know.

**And automatic reconnect** (SpacetimeDB 2.12): their C# and TypeScript SDKs keep a connection, its
cache, its subscriptions, and its callbacks usable across a drop, with exponential backoff, replay,
cache reconciliation, and a token provider for expiring credentials. `MelangeClient` already has the
hard half, `ReconnectAsync` resuming from the last acknowledged LSN under a named log epoch, but it
is a method the application must call, and every consumer writes the same backoff loop around it.

## Deliverables

**Schema tolerance**

- `ClientRowShape.Verify` becomes a **mapping** rather than an equality check. For each column the
  bindings know, it finds the server column by name and checks the kind. Server columns the
  bindings do not know are skipped. It throws only for a column the bindings need that the server
  no longer has, or has with a different kind. Renames stay loud, because a rename is a removal plus
  an addition.
- The typed row decoder takes the mapping and **skips unknown columns by kind**. Ordered row bytes
  are not self-delimiting without the descriptor, but with it every column's extent is known.
- Producer results, which carry rows in the same encoding, get the same treatment.
- **Reducer arguments**: establish what an old client calling a reducer whose signature gained a
  parameter sees today, and decide whether trailing parameters with defaults are accepted when
  absent. See the decision below.
- [CLIENT-BINDINGS.md](../CLIENT-BINDINGS.md) and [MIGRATION.md](../MIGRATION.md): what an old
  client survives, and what still requires regenerating, in one table.
- Tests: an old binding against an added middle column, an added trailing column, a reorder, an
  added table; and against a removed column, a re-kinded one, and a renamed one, which still throw
  naming the column.

**Automatic reconnect**

- An opt-in policy on `MelangeClientOptions` (`Reconnect`): exponential backoff with jitter, a cap,
  and a give-up bound. Each attempt goes through the existing `ReconnectAsync` resume path, so cache
  reconciliation is the code that already exists.
- A token provider (`Func<CancellationToken, ValueTask<string>>`) consulted on each attempt, for
  expiring credentials. The identity rule from phase 04 is unchanged: a refreshed token may never
  change identity.
- Events the application can show a "reconnecting…" state from, and a terminal one when the policy
  gives up.
- Every new option goes in [CONFIGURATION.md](../CONFIGURATION.md); reconnect attempts and outcomes
  go in [OBSERVABILITY.md](../OBSERVABILITY.md).

## Out of scope

- **Tolerating destructive changes.** A removed or re-kinded column the bindings use is a real
  incompatibility, and pretending otherwise is the decode-into-garbage failure protocol v2 removed.
- **Server-side version negotiation**, meaning the server keeping old shapes alive for old
  clients. That is API versioning, and it is a product decision about how long to support a build,
  not a database feature.
- **Reconnect across a log epoch change** (a restore or a clone) beyond what `Resume` does today:
  a full resync, loudly.

## Decisions to settle

### Map on the client, or project on the server

The alternative to client-side skipping is for generated bindings to subscribe with an explicit
column list, the bindings' own columns, so the server projects and old clients never see added
columns at all. It saves the bytes of unused columns.

**Leaning:** map on the client. Projection makes every typed subscription a projected one, which
changes the server's fan-out work per subscriber, and [CLIENT-BINDINGS.md](../CLIENT-BINDINGS.md)
deliberately keeps typed caches off projected subscriptions. Client-side mapping changes no wire
format and no server cost, and it is one rule the phase 23 conformance contract can state:
*decode by descriptor, bind by name.* Revisit only if the unused-column bytes show up in a
measurement.

### Whether an old client's reducer calls survive an added parameter

Arguments travel in the reducer-argument encoding, positionally. **Open:** whether the server
accepts a call that omits trailing parameters that declare defaults. It is the reducer-side mirror
of the row change, and the game case is real (a reducer gains an optional flag). But it makes a
reducer's signature evolution a compatibility surface that has to be documented and tested
forever. **Leaning:** in scope only if it falls out of the argument decoder cleanly. Otherwise
record a clear `ReducerResult` error naming the missing parameter, and defer.

### Whether the server should know a client is stale

A client that maps around added columns is working, but it is also running old bindings. A
`Hello`-time schema hash would let the server count stale clients
(`melange.connections.stale_bindings`) without refusing them. That is useful for deciding when an
old build can be cut off. **Leaning:** yes, as an observation only; it never gates a connection.
It adds a field to the handshake, so it belongs in the same protocol revision as anything else
phase 23 needs.

### Reconnect on by default

SpacetimeDB made it opt-in. **Leaning:** opt-in in this release, with a default policy that is a
single line to enable. Turning it on by default would change what `OnDisconnected` means for every
existing consumer, which is a breaking change to make deliberately in a later release, not a
default to slip in alongside the feature.
