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
  `Identity` is a stable hash of the token's **issuer and subject**. Issuer included deliberately: hashing
  the subject alone would let a subject from one token source collide into another source's identity,
  which bypasses the entire policy layer without triggering any of it.
- **Every connection presents a valid token — the IdP is the gate.** MelangeDB mints no identities, guest
  or otherwise. Guest play is a token the IdP issues with a guest role claim (`Auth:GuestRole`); a guest
  is an ordinary identity that policies and caps can treat differently, nothing more. Guest issuance
  limits — who gets an anonymous token, and how fast — are the IdP's job, which is where account-creation
  throttling belongs anyway. For local dev without an IdP, `dotnet user-jwts` or a dev-only issuer in the
  host covers it; MelangeDB stays entirely out of the identity-minting business.
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

**The invariant that makes re-auth safe: `Reauthenticate` may refresh a token but must never change the
identity.** Every subscription's initial set and every delta on this connection was computed under the current
identity's row and column policies. Permitting a mid-session identity switch would mean rows filtered for
identity A arriving at identity B — a leak, in the phase whose entire job is preventing leaks. If the presented
token resolves to a different identity, the correct response is to close the connection and make the client
reconnect, not to re-evaluate state in place.

**Guest conversion needs no merge machinery.** A guest holds an IdP-issued token like anyone else. When they
sign up, account linking is the **identity provider's** job: if the IdP preserves the subject when linking a
guest id to a new account, the resulting JWT resolves to the *same* `Identity`, MelangeDB sees no change, and
the live session picks the new token up through `Reauthenticate`. Nothing merges because nothing moved. Since
identity hashes issuer *and* subject, the linked token must come from the **same issuer** — automatic when one
IdP handles both guest and full accounts, and one more reason guests belong on the IdP rather than on a second
token source.

This keeps MelangeDB out of the identity business, consistent with using the host's ASP.NET Core authentication
rather than inventing a parallel one. The obligation it creates is a **documented contract** rather than a
feature: *if you want guest progress to survive sign-up, your IdP must preserve the subject when linking.* An
integrator whose IdP mints a fresh subject instead will strand guest characters, and that consequence belongs in
their hands with the reason stated plainly — not solved by a merge operation inside the database.

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

**Connection caps.** With the IdP as the gate, identity-issuance abuse is throttled where accounts are made —
at the IdP. What remains MelangeDB's is `Auth:MaxConnectionsPerIdentity`: a valid token still must not hold
unlimited sockets, subscriptions, and rate-limit buckets.

## Out of scope

Role hierarchies, groups, delegation. Sandboxing server code — there is deliberately no sandbox (DESIGN.md §1).
Client-side cheat detection: column visibility narrows what a cheat can *know*, but cannot police what a client
does with data it legitimately receives.

## Decisions to settle

- ~~**Cost of per-row policy evaluation.**~~ **Settled: ship the simple delegate path, with the measurement
  recorded.** Measured on the dev machine (Debug build, `PolicyCostMeasurementTests`): a union of two row
  policies — one of them doing a `Find` into the private `AdminIdentity` table per row, the worst realistic
  shape — evaluates at **~520 ns/row (~1.9M rows/s)**, including the per-evaluation row deserialization. At
  that rate a 100k-row initial set pays ~50ms and a typical delta fan-out pays microseconds, which is nowhere
  near the top of the profile. No pre-filter declaration, no per-(identity, table) visibility cache — both
  add invalidation machinery whose bugs are silent leaks, the worst class this phase exists to prevent.
  Revisit under phase 10's load rig if it shows up hot.
- ~~**Policy-relevant state changes.**~~ **Settled: no caching, so *new* decisions apply immediately; already-
  delivered rows are corrected by resubscribe or termination — the crude option, deliberately.** Policies are
  evaluated per event against committed state, never cached, so an `AdminIdentity` change affects the very
  next delta and every later initial set on its own. What it does not do is retroactively re-filter rows a
  client already holds. The honest remedies ship today: the client resubscribes (a re-scope or reconnect), or
  the server calls `MelangeSessions.Terminate(identity)` after a privilege change — demotion and revocation
  usually coincide anyway. Dependency-tracking policies would turn every policy into an invalidation problem;
  cruder is right until a real workload proves otherwise.
- ~~**Do policies apply to ad-hoc SQL (phase 08)?**~~ **Settled: the two-mode contract ships now, not in 08.**
  `Sql:AdHocMode` — `PolicyEnforced` (default) applies row and column policies exactly as a subscription
  would; `Owner` deliberately bypasses them for operator tooling. There is no third mode. `[ServerOnly]`
  columns are excluded in **both** modes: "never leaves the process" has no modes, and owner mode bypasses
  policies, not physics. Every HTTP endpoint requires a valid JWT either way. What 08 still owns: per-caller
  authorization of who may use owner mode (today it is a deployment-level setting) and the aggregate/join
  surface.
- ~~**Guest token persistence in the client SDK.**~~ **Settled: shipped as `ITokenStore`.** Pluggable on
  `MelangeClientOptions`, with `InMemoryTokenStore` as the honest-for-tests default and `FileTokenStore`
  (atomic temp-file-and-rename writes — a torn guest token is a lost character) as the durable reference
  implementation. The client loads from the store when no token is configured and persists the accepted token
  on connect and after every successful `Reauthenticate`, so a guest conversion updates the stored credential
  automatically.
- ~~**Guest → authenticated upgrade.**~~ **Settled: not MelangeDB's concern.** Guest conversion is IdP-side
  account linking; if the subject is preserved the identity never changes. See the deliverable above.
- ~~**Reducer authorization default posture.**~~ **Settled: allow-by-default, paired with the report — and the
  report is asserted.** `Policies:DefaultReducerPosture` defaults to `Allow` because deny-by-default taxes
  every ordinary gameplay reducer with an annotation and pushes teams toward a blanket `AllowAll` policy that
  defeats the audit. The omission stays visible instead of fatal: `Policies:UnpolicedReducerReport` lists
  every client-callable reducer with no policy at startup (`Warn` default, `Fail` refuses to start), and a
  test asserts the report's exact contents so it cannot silently regress. `Deny` remains one config key away
  for teams that want it. Policies gate client-originated calls only; in-process dispatch is the host's own
  code.
- ~~**Cost of column masking on the delta path.**~~ **Settled: `IColumnPolicy<T>` ships unrestricted, with the
  measurement recorded.** Same rig as the row measurement: one mask evaluated and intersected costs
  **~470 ns/row (~2.1M rows/s)**. Tables with no column policy pay literally nothing on the delta path — the
  wire column set is precomputed at subscription compile time — and `[ServerOnly]` stays compile-time-free,
  so the per-row cost exists only where a dynamic mask was explicitly requested. A high-fan-out restriction
  would be policy for a problem the numbers say does not exist yet; phase 10's load rig re-opens this if
  terrain-scale fan-out disagrees.

## Done when

- A client presenting an IdP-issued guest-role token connects, resolves to a stable identity, and keeps it
  across reconnects; a client with **no** token is rejected — the IdP is the gate.
- A client with a JWT resolves to a stable identity across reconnects and across server restarts.
- Two tokens sharing a subject but from **different** issuers resolve to **different** identities — the
  collision test for hashing issuer and subject together.
- A browser-style client with **no ability to set headers** authenticates successfully via the ticket flow.
- A ticket is rejected on second use, and after its TTL expires.
- A session whose token expires mid-connection survives if the client re-authenticates within the grace window,
  and is closed if it doesn't — both asserted, since only testing the happy path here is how the insecure
  variant ships.
- `Reauthenticate` with a token resolving to a **different** identity closes the connection rather than switching
  in place. This is the leak test for the frame, so it gets written before the happy path.
- A guest token and a later account-linked token sharing an issuer and subject resolve to the same `Identity`,
  and the live session continues across the swap with its subscriptions intact.
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
- One identity cannot exceed the connection cap.

## Risks

- **Silent over-exposure is the worst possible bug class here.** In a full-loot game, leaking inventory rows
  is wallhack-grade intel. Tests must assert at the wire level — an API-level test can pass while the
  protocol leaks.
- **Policies that read tables mutated in the same transaction** could observe torn state. Evaluate policies
  against a committed LSN, never against a partially applied write set.
