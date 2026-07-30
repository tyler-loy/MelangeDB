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
- JWT bearer validation on the websocket handshake using the host's existing ASP.NET Core authentication —
  no bespoke token system. `Identity` is a stable hash of the token subject.
- Signed **guest identities** for anonymous play, issued by the server and durable across reconnects.
- `ctx.Caller` (identity) and `ctx.ConnectionId` (this session) are distinct, and the distinction is
  documented — one identity may hold several connections.

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

## Out of scope

Authorization of *reducer calls* beyond identity — per-reducer permission attributes can come later; a
reducer can check `ctx.Caller` itself for now. Role hierarchies, groups, delegation.

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

## Done when

- A client with no token connects as a guest, gets an identity, and keeps it across a reconnect.
- A client with a JWT resolves to a stable identity across reconnects and across server restarts.
- Player A cannot see Player B's inventory rows, skills, or attributes — asserted at the protocol level, not
  just the API level.
- Both players see the contents of a shared world container: the union case, proven.
- An admin identity sees all rows via a policy that reads the private `AdminIdentity` table, and a
  non-admin's subscription is **unaffected** — the SpacetimeDB failure mode, shown fixed.
- Making a row invisible to a subscribed client emits a delete to that client and nothing to others.
- Subscribing to a private table errors; no policy can make a private table visible.

## Risks

- **Silent over-exposure is the worst possible bug class here.** In a full-loot game, leaking inventory rows
  is wallhack-grade intel. Tests must assert at the wire level — an API-level test can pass while the
  protocol leaks.
- **Policies that read tables mutated in the same transaction** could observe torn state. Evaluate policies
  against a committed LSN, never against a partially applied write set.
