# Phase 04 — Identity, auth, and row-level policies

**Goal:** clients authenticate, get a stable `Identity`, and see only the rows they're permitted to see —
with policies as DI-resolved C# objects rather than SQL strings.

**Depends on:** [03](plan-phase-03.md).

## Why here

Row-level security has to exist before anyone puts real data in the system; retrofitting it means auditing
every subscription. It also delivers a concrete, demonstrable advantage over SpacetimeDB (below), which makes
it worth doing early for its own sake.

## Deliverables

**Identity**
- JWT bearer validation using the host's existing ASP.NET Core authentication — no bespoke token system.
  `Identity` is a stable hash of the token subject.
- Signed **guest identities** for anonymous play, issued by the server and durable across reconnects.
- `ctx.Caller` (identity) and `ctx.ConnectionId` (this session) are distinct, and the distinction is
  documented — one identity may hold several connections.

**Connect tickets — because browsers cannot set WebSocket headers.** The obvious design, an
`Authorization: Bearer` header on the handshake, works from Godot and .NET and is *impossible* from the browser
WebSocket API, which permits no custom headers. That would leave the reference project's admin web console
unable to authenticate at all. The alternatives are all worse than a ticket: a token in the query string ends up
in access logs and proxy logs, and the `Sec-WebSocket-Protocol` smuggle is a hack that confuses every
intermediary.

So: `POST /melange/ticket` with the JWT over TLS returns a **single-use, ~30-second ticket** presented on the
socket URL. Header auth stays supported for clients that can do it, but the ticket is the path that works
everywhere. Tickets are single-use and short-lived so a leaked one is near-worthless.

**Mid-session re-authentication.** A game session runs for hours; a JWT lasts about one. Neither obvious option
is acceptable — dropping the connection at expiry is terrible game feel, and ignoring expiry after the handshake
means a revoked or expired credential keeps working indefinitely. Instead the client sends `Reauthenticate`
(frame defined in phase 03) with a fresh token before expiry, and the server enforces a configurable grace
window past expiry before closing the connection.

**Revocation.** Banning a cheater must take effect now, not in 55 minutes when their token expires. Needs an
explicit "terminate all sessions for this identity" operation, and a revocation check on re-auth. Without this,
moderation in a live game doesn't work.

**Row-level policies**
```csharp
public sealed class InventoryVisibility(IAdminRegistry admins) : IRowPolicy<InventoryItem>
{
    public bool IsVisibleTo(in InventoryItem row, PolicyContext ctx) =>
        row.OwnerId == ctx.Caller
        || row.ContainerKind is ContainerKind.WorldContainer or ContainerKind.Vehicle
        || admins.IsAdmin(ctx.Caller);        // reads a PRIVATE table — the point of this design
}
```
- **Multiple policies on one table compose as a UNION.** Load-bearing: a player must see their own inventory
  *plus* the contents of any open chest or cart. Intersection semantics make that unexpressible.
- Policies are enforced on the initial subscription set *and* on every delta. A row becoming invisible must
  emit a delete to that client; becoming visible must emit an insert.
- A table with no policy and `Public = true` is fully visible; `Public = false` is never visible. No implicit
  third state.

**The advantage to demonstrate.** In SpacetimeDB, an RLS rule that references a private table fails to
evaluate for ordinary clients and kills their *entire* subscription — a documented, painful footgun in the
reference workload's `Rls.cs` (gray screen, no spawn). A policy object runs in-process with no restricted
namespace, so admin bypass is a plain lookup. Ship a test that encodes exactly this scenario.

**Column-level visibility.** Row policies filter rows; nothing filters columns, and that gap is live in the
reference workload today. `Creature` is `Public = true` and ships `NextThinkAt`, `SpookedUntil`,
`LastDamagedAt`, and `HomeX/HomeZ` to every client — a complete AI oracle telling a cheater exactly when a
creature next thinks and whether its alert radius is doubled. See [SECURITY.md](SECURITY.md) for the full
argument.

```csharp
[ServerOnly] public ulong NextThinkAt;      // static: never leaves the process
```
```csharp
public sealed class PlayerStateColumns : IColumnPolicy<PlayerState>   // dynamic: depends on the asker
{
    public ColumnMask VisibleTo(in PlayerState row, PolicyContext ctx) =>
        row.Identity == ctx.Caller ? ColumnMask.All
                                   : ColumnMask.All.Except(nameof(PlayerState.LastSeen));
}
```

This reuses phase 03's partial-row wire format — client projection and server column policy are the same
mechanism applied from opposite ends. The composition rule is easy to invert, so state it in the XML docs:
**rows UNION, columns INTERSECT.** A client requesting a `[ServerOnly]` column gets an error, never a silently
empty field.

Splitting tables is *not* an acceptable substitute here: `Creature` deliberately stores a path rather than a
point so the server writes only when a creature decides something, and splitting AI fields out would double
writes on the hottest table in the game.

**Reducer authorization.** The reference module calls `RequireAdmin(ctx)` by hand **24 times**. Every one is a
site where omitting the line is a privilege escalation, and nothing detects the omission.

```csharp
[Reducer(Policy = typeof(AdminOnly))]
public void ClearCreatures(ReducerContext ctx) { /* no guard clause */ }
```

Policies resolve from DI (so they can read private tables). The deliverable that matters as much as the
attribute is a **report listing every client-callable reducer with no policy attached** — that turns "did we
forget one?" from a code-review question into a build artifact.

**Rate limiting.** Connection-level token bucket per identity, configurable per reducer, rejected before a
transaction opens. The reference workload implements this *as table rows* (`PlayerRateLimit` + a micro-token
bucket), paying a row write on every gathered rock purely for defense. Game-semantic checks like movement
plausibility stay in the module — that's gameplay — but "no more than N calls/second" shouldn't need schema.

**Identity and connection caps.** `AllowGuests` currently grants an identity to anyone who asks, so every
per-identity defense in this phase is bypassed by acquiring a new identity. Needs a connection cap per identity
and a guest-issuance limit.

## Out of scope

Role hierarchies, groups, delegation. Sandboxing server code — there is deliberately no sandbox (DESIGN.md §1).
Client-side cheat detection: column visibility narrows what a cheat can *know*, but cannot police what a client
does with data it legitimately receives.

## Decisions to settle

- **Cost of per-row policy evaluation.** A delegate per row on the delta path may be too slow for terrain-
  scale fan-out. Options: allow a policy to declare an index-backed *pre-filter* so most rows never reach the
  predicate, or cache per-(identity, table) visibility. Measure before choosing.
- **Policy-relevant state changes.** If `AdminIdentity` changes, cached visibility is stale. Either policies
  declare their dependencies, or admin changes force a resubscribe. The second is cruder and probably right.
- **Do policies apply to ad-hoc SQL (phase 08)?** They must, or RLS is trivially bypassable — but an owner/
  admin path needs to bypass them deliberately. Two explicit modes, no ambiguity.
- **Guest identity durability.** Cookie, local token file, or client-persisted key? Affects whether a guest
  keeps their character after a client restart.
- **Guest → authenticated upgrade.** A guest plays for two hours, then signs in. Do they keep their character?
  In a full-loot game identity *is* inventory, so this is a product decision with real weight, not a technical
  detail. Either support an identity-merge operation (and accept that merging is a fraud vector worth
  rate-limiting) or refuse it explicitly and tell players up front. Silence here means players lose characters
  and blame the game.
- **Reducer authorization default posture.** Deny-by-default is safer but makes every ordinary gameplay reducer
  carry an annotation. Allow-by-default plus the unpoliced-reducer report is probably the right trade — the
  omission becomes visible without being fatal — but decide it explicitly rather than by accident.
- **Cost of column masking on the delta path.** A per-row mask evaluation on terrain-scale fan-out may be too
  slow. `[ServerOnly]` is free (it's compile-time), so the question is only about `IColumnPolicy<T>`; consider
  restricting dynamic masks to tables that aren't high-fan-out.

## Done when

- A client with no token connects as a guest, gets an identity, and keeps it across a reconnect.
- A client with a JWT resolves to a stable identity across reconnects and across server restarts.
- A browser-style client with **no ability to set headers** authenticates successfully via the ticket flow.
- A ticket is rejected on second use, and after its TTL expires.
- A session whose token expires mid-connection survives if the client re-authenticates within the grace window,
  and is closed if it doesn't — both asserted, since only testing the happy path here is how the insecure
  variant ships.
- Revoking an identity terminates its live sessions and prevents re-auth, without restarting the server.
- Player A cannot see Player B's inventory rows, skills, or attributes — asserted at the protocol level, not
  just the API level.
- Both players see the contents of a shared world container: the union case, proven.
- An admin identity sees all rows via a policy that reads the private `AdminIdentity` table, and a
  non-admin's subscription is **unaffected** — the SpacetimeDB failure mode, shown fixed.
- Making a row invisible to a subscribed client emits a delete to that client and nothing to others.
- Subscribing to a private table errors; no policy can make a private table visible.
- A `[ServerOnly]` column never appears on the wire for any client, admin included — asserted by inspecting
  frames, not the client API.
- A client explicitly requesting a `[ServerOnly]` column gets an error rather than a null or default value.
- A column masked by policy for one caller and visible to another is correct in both directions, and changing
  the mask mid-subscription updates the client.
- Every client-callable reducer either has a policy or appears in the unpoliced-reducer report; the report is
  asserted in a test so it can't silently regress.
- A reducer over its rate limit is rejected **before** a transaction opens — verified by asserting no log
  record was appended, not just that an error came back.
- One identity cannot exceed the connection cap; guest issuance is limited.

## Risks

- **Silent over-exposure is the worst possible bug class here.** In a full-loot game, leaking inventory rows
  is wallhack-grade intel. Tests must assert at the wire level — an API-level test can pass while the
  protocol leaks.
- **Policies that read tables mutated in the same transaction** could observe torn state. Evaluate policies
  against a committed LSN, never against a partially applied write set.
