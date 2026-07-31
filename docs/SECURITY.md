# Defensive surface

What a MelangeDB server can enforce against a client it does not trust. In a full-loot game a leaked row is
wallhack-grade intel, so this is a correctness concern, not a hardening checklist.

The audited reference workload hand-rolls three of the defenses below, which is the clearest possible signal
that they belong in the database.

## Already covered

| Defense | Where |
| --- | --- |
| Private tables — never syncable, no subscription may name one | DESIGN.md §2, phase 04 |
| Row-level policies, composing as a **union**, enforced on the initial set *and* every delta | phase 04 |
| Policies may read private tables (so admin bypass works) | phase 04 |
| Scheduled reducers are not client-callable — a client can't force a world tick | phase 05 |
| Subscription count and buffer caps, backpressure policy | phase 03 |
| Max inbound message size | phase 03 |
| **Audit trail, for free** — every log record carries caller identity, reducer name, arguments, and timestamp | phase 01 |

That last one is worth noticing: because the commit log records the reducer name and args as metadata
alongside every write set, MelangeDB has a complete, ordered, tamper-evident record of who did what without
anyone building an audit system. Investigating an exploit is a log replay.

## Gap 1 — Column-level visibility

**This is the real gap, and it's live in Vibe Shaft today.** `Creature` is `Public = true`, so every connected
client receives every column — including columns that are purely server-internal AI state:

```csharp
public byte State;          // 0 idle, 1 wander, 2 flee, 3 return
public ulong NextThinkAt;   // the AI tick skips this row until now passes it
public ulong LastDamagedAt; // hit invulnerability window
public ulong SpookedUntil;  // alert radius doubles until this passes
public float HomeX, HomeZ;  // territory anchor
```

A client reading these knows **exactly when a creature will next make a decision** (`NextThinkAt`), whether
its alert radius is currently doubled (`SpookedUntil`), when its invulnerability window ends
(`LastDamagedAt`), and where its territory is anchored. That is a complete AI oracle, shipped to every player
by design, because row policies filter *rows* and nothing filters *columns*.

SpacetimeDB's only workaround is splitting the table — and here that workaround is actively hostile to a
deliberate performance design. `Creature`'s comment explains that the row stores *a path, not a point*, so the
server writes only when a creature decides something ("a wandering deer is one update every few seconds, not
ten a second"). Splitting AI fields from path fields would double writes on the hottest table in the game.

### The design

Two mechanisms, because there are two distinct needs.

**Static server-only columns** — never leave the process, no policy evaluation, no per-row cost:

```csharp
[Table(Public = true)]
public partial struct Creature
{
    public float X, Y, Z;                              // synced
    [ServerOnly] public ulong NextThinkAt;             // never sent to any client
    [ServerOnly] public ulong SpookedUntil;
    [ServerOnly] public float HomeX, HomeZ;
}
```

**Policy-driven column masking** for the cases where visibility depends on who is asking:

```csharp
public sealed class PlayerStateColumns : IColumnPolicy<PlayerState>
{
    public ColumnMask VisibleTo(in PlayerState row, PolicyContext ctx) =>
        row.Identity == ctx.Caller
            ? ColumnMask.All
            : ColumnMask.All.Except(nameof(PlayerState.LastSeen));
}
```

### It reuses machinery we already need

Phase 03 already requires **partial rows** on the wire, because client subscriptions support column projection
(`SELECT skill_id, total_xp, level FROM player_skill WHERE ...`). Server-side column visibility is the same
mechanism applied from the other end: the client asks for a projection, the server imposes one.

The composition rule, which is easy to get backwards:

> **Rows compose as a UNION. Columns compose as an INTERSECTION.**
> A client sees a row if *any* row policy admits it, and sees a column only if *every* applicable rule admits
> it. A client requesting a `[ServerOnly]` column is an error, not a silently empty field.

**Shipped in phase 04**, with three guarantees sharper than originally stated:

- A predicate on a `[ServerOnly]` column is also an error — membership through hit-versus-miss is
  information too.
- A write touching **only** invisible columns emits **no frame at all**. An update frame with unchanged
  visible columns would still tell a client *when* `NextThinkAt` changed — the timing oracle without the
  value.
- `[ServerOnly]` exclusion has **no modes**: it holds for admins, and it holds in ad-hoc SQL's owner mode.
  Owner mode bypasses row and column *policies*; it does not reopen columns that never leave the process.

## Gap 2 — Reducer authorization

Vibe Shaft calls `RequireAdmin(ctx)` **24 times**, by hand, at the top of privileged reducers. Every one of
those call sites is a place where forgetting the line is a privilege escalation, and nothing detects the
omission.

Phase 04 put this out of scope. That was the wrong call — it's the same class of problem as row visibility, and
"each of 119 reducers polices itself" is not a defensive posture.

```csharp
[Reducer(Policy = typeof(AdminOnly))]
public void ClearCreatures(ReducerContext ctx) { /* no guard clause needed */ }
```

Declarative, resolved from DI (so it can read private tables), and — the point — **auditable**: a diagnostic
can list every client-callable reducer with no policy attached, which turns "did we forget one?" from a code
review question into a build artifact.

Default posture is worth deciding deliberately: deny-by-default is safer but makes every ordinary gameplay
reducer carry an annotation. **Decided and shipped in phase 04**: allow-by-default
(`Policies:DefaultReducerPosture`, with `Deny` as the opt-in) *plus* the unpoliced-reducer report
(`Policies:UnpolicedReducerReport` — warn at startup, or refuse to start), and the report is asserted in a
test so it cannot silently regress. Policies apply to client-originated calls only; in-process dispatch is
the host's own code. Denial happens before any transaction opens — no log record exists for a refused call.

## Gap 3 — Subscription cost limits

Nothing currently stops a client from subscribing to `SELECT * FROM terrain_chunk_data` with no predicate and
pulling ~24.6k compressed terrain blobs — the entire world — in one request. `Subscriptions:MaxPerConnection`
caps how *many* subscriptions exist, not what any one of them costs.

Needed:
- **Mandatory-predicate tables.** A table may declare that a subscription must constrain a given column, so
  unbounded terrain scans are rejected rather than served.
- **Bounded range width.** `WHERE chunk_id BETWEEN :lo AND :hi` with a maximum span, so a client can stream a
  ring around itself but not the map.
- **A row/byte ceiling per subscription**, with a clear error rather than an OOM.
- **Estimated cost rejected before execution**, since the damage is done by the time you're streaming.

This is a denial-of-service surface reachable by any authenticated client, including a guest.

## Gap 4 — Rate limiting as infrastructure

`PlayerRateLimit` + `RateLimit.cs` implement a token bucket **as table rows**: micro-token accounting, refill
on read, plus a movement plausibility check. It works, and it costs a row write on every gathered rock purely
for defense — write amplification on the hottest path, and log volume, to enforce something the transport layer
could enforce for free.

MelangeDB provides exactly this since phase 04: a token bucket per identity **per reducer**
(`RateLimit:*`, per-reducer overrides), applied to client-originated calls and rejected **before** a
transaction opens — asserted in tests by the log head not moving, not just by the error coming back.
Game-semantic checks like movement plausibility stay in the module — that's gameplay logic, not
infrastructure — but "no more than N calls/second" no longer requires schema.

## Gap 5 — Argument validation

Reducer arguments come from clients and are currently trusted. The framework can't validate semantics, but it
can and should reject the class of inputs that corrupts state regardless of game rules:

- **`NaN` / `±Infinity` floats.** A `NaN` position propagates through terrain lookups and chunk math and
  poisons rows that are then replicated to every client. This is the highest-value item in this section.
- **String length caps**, so a name field can't carry a megabyte.
- **Collection length caps** on array arguments.
- Integer range constraints where declared.

## Gap 6 — Identity abuse and session identity binding

**The IdP is the gate.** Every connection presents a valid token; MelangeDB mints no identities, guest or
otherwise — guests are IdP-issued tokens carrying a guest role. That places issuance abuse (unlimited fresh
identities rotating past every per-identity defense above) where it belongs: at the IdP, which is where
account-creation throttling lives anyway. What remains MelangeDB's side of the line:

- **A connection cap per identity** (`Auth:MaxConnectionsPerIdentity`) — a valid token still must not hold
  unlimited sockets, subscriptions, and rate-limit buckets.
- **Identity derivation includes the issuer.** `Identity` is a hash of the token's issuer *and* subject.
  Hashing the subject alone would let a subject from one token source collide into another source's identity —
  a full policy-layer bypass that triggers nothing.

Related, and easy to implement permissively by accident: **a connection is bound to one identity for its
lifetime.** `Reauthenticate` refreshes a token; it must not switch identity. Every initial set and delta on that
connection was computed under the current identity's row and column policies, so an in-place switch delivers
A's filtered rows to B — a leak that bypasses the entire policy layer without triggering any of it. A token
resolving to a different identity closes the connection instead.

All of this shipped with phase 04, plus the moderation surface it implies: `MelangeSessions` terminates every
live session for an identity and holds an in-memory revocation set checked on connect and on re-auth —
effective immediately, no restart. Session-level on purpose: it answers "the ban takes effect *now*, not in
55 minutes when the token expires"; the durable ban belongs at the IdP, which stops issuing the subject
tokens. Expiry is enforced mid-session too — a token past its lifetime plus `Auth:ReauthGraceSeconds`
without a successful `Reauthenticate` closes the connection.

## The cluster trust boundary (phase 09)

Clustering adds exactly one new credential and one new boundary, and both are stated rather than implied.

**The credential** is `Cluster:Secret`: one shared HMAC key behind (a) node-link mutual authentication — both
ends of every hub↔node TCP link prove possession over exchanged nonces at connect, so neither a rogue process
dialing the hub's node port nor something impersonating the hub gets past the handshake — and (b) the
**internal identity assertion**, the signed token the hub mints saying "this connection acts as identity X."
The gateway validates a client's IdP JWT exactly once (the IdP is still the gate), then vouches for the
identity to shard nodes with assertions; shard nodes never see IdP tokens, which is what makes hub-issued
guest identities and per-shard routing workable at all. Assertions expire (`Cluster:AssertionTtlSeconds`,
never outliving the client token), are constant-time verified, and are refused at the gateway itself — a
client cannot present one.

**The boundary this draws, stated as an assumption and not an accident:** *any holder of the cluster secret
can assert any identity.* Concretely, a compromised shard node can impersonate every player in its shards —
and, since it holds the secret, mint assertions for anyone. That is accepted: nodes are your infrastructure,
exactly like the host process that already runs reducers with full authority (see below). What follows from
it operationally: treat `Cluster:Secret` like a database password; keep node links and per-shard websocket
endpoints (`Cluster:PublicAddress`, `{path}/shard/{key}`) on an internal network — the gateway is the only
client-facing endpoint; and rotate the secret by restarting the cluster with a new one, which invalidates
every outstanding assertion at once.

Fencing tokens and leases are a *correctness* mechanism, not a security one: they stop a wrongly-suspected-dead
node from split-brain writes, but a node that ignores them is already inside the trust boundary above.

## Explicitly not defended against

Stating these plainly is part of the design, not an omission:

- **Untrusted server code.** The library model runs reducers with the host process's full authority. This is
  *your* code in *your* exe; there is no sandbox and no attempt at one (DESIGN.md §1).
- **Client-side cheating beyond server authority.** Aimbots, wallhacks built from legitimately-synced data,
  input automation. Column visibility narrows what a cheat can *know*; it cannot police what a client does
  with what it legitimately receives.
- **The shard-span contract.** Rows mutated in one transaction must resolve to one shard. MelangeDB detects
  violations in development (`Cluster:ShardSpanCheck`) but cannot prevent them statically.
- **Existence inference through query patterns.** Even with correct row policies, a client can probe with
  `WHERE x = :guess` and learn something from hit versus miss. Mitigated by mandatory predicates and rate
  limits, not eliminated.

## Where this work lands

| Gap | Phase |
| --- | --- |
| Column-level visibility (`[ServerOnly]`, `IColumnPolicy<T>`) | **04** — pulled in scope |
| Reducer authorization policies + unpoliced-reducer report | **04** — pulled in scope |
| Argument validation (NaN/Inf, length caps) | **02** — a generator concern |
| Subscription cost limits | **03** |
| Rate limiting | **04** |
| Identity/connection caps | **04** |
