# Phase 29 — The agent surface

**Status: Built for a single node** (2026-11-21); cluster routing of reducer calls is the one deliverable left. See [MCP.md](../MCP.md) and the as-built notes at the end.

**Goal:** an opt-in package, `MelangeDB.Mcp`, that exposes a [Model Context
Protocol](https://modelcontextprotocol.io) endpoint from the developer's own host. A coding agent
pointed at a running MelangeDB can then read the schema, run owner-mode SQL, and read the published
signals, through the same authentication the host already uses. Reducer calls are a separate,
explicitly granted capability, not part of the default.

```csharp
builder.Services.AddMelangeDb(...)
    .AddMcp(mcp => mcp.AllowReducers("SpawnTestCreature", "ResetZone"));   // reads are the default; writes are named

app.MapMelangeMcp();   // /melange/mcp; answers 403 mcp_disabled until Mcp:Enabled is set
```

**Depends on:** nothing. It composes surfaces that shipped: the `/melange/schema` manifest
(phase 12), `/melange/sql` in owner mode (phase 08), and the span and metric register.

## Why here

**SpacetimeDB made this a first-class surface across four releases.** A standalone MCP endpoint
came in 2.7, a `spacetime mcp` stdio bridge for local agents in 2.8.1, agent plugins for Claude and
Codex in 2.8.1–2.8.2, and Maincloud support with leader routing in 2.10. That endpoint offers
health, schemas, SQL, and reducer invocation. The audience is real: an agent writing reducer code
against a live world wants to check what the table actually holds, and today it shells out to
`curl` against `/melange/sql` with a hand-built token, or guesses.

**MelangeDB's shape makes it unusually cheap.** The database is a library inside the developer's
host, so the endpoint inherits the host's auth, DI, and logging for free. And because every
capability it exposes is already a published, stable surface, the phase is an adapter rather than
new engine work.

## This is not the admin console

[ROADMAP.md](../ROADMAP.md) refuses a stock admin console permanently, and this phase has to be
consistent with that rather than a way around it. The refusal's own reasoning draws the line:
**"publishing the signals is the engine's job; prescribing the tool that draws them is not."** An
MCP endpoint is on the publishing side of that line. It is a protocol, like `/melange/sql` is, with
no opinions about who operates the world or how anything is drawn, and any MCP client can consume
it, an agent being one. What would cross the line is shipping the *client*: a UI, a dashboard, or an
opinionated agent prompt pack. Those stay out (see [Out of scope](#out-of-scope)).

## Deliverables

- **`MelangeDB.Mcp`**, a separate package so the core takes no dependency on an MCP SDK. Built on
  the official C# MCP SDK; which package and version is checked at phase start, not pinned here.
- **Read tools** (the default set):
  - `describe_schema`: the manifest, the same bytes `/melange/schema` serves.
  - `query`: SQL through the same path as `/melange/sql`, under the same `Sql:AdHocMode` rules, so
    a `PolicyEnforced` deployment answers an agent with exactly the rows that caller's policies
    allow.
  - `health`: head LSN, durable LSN, applier lag, and the governing truncation floor.
  - `signals`: a bounded read of the instruments [OBSERVABILITY.md](../OBSERVABILITY.md) already
    names.
- **Reducer calls, opt-in by name.** `AllowReducers(...)` lists the reducers an agent may call, and
  under which identity. There is no wildcard. A call is an ordinary reducer invocation: logged,
  traced, policed by phase 04's reducer policies, and recorded in the log like any other.
- **`melange mcp`**: a stdio bridge in the CLI, for local agents that expect a subprocess. It
  forwards to a running host's endpoint and authenticates the way the other `melange` verbs do.
- **Cluster routing**: a call reaching any node is answered for the whole cluster where the
  underlying surface already is (schema, owner SQL), and routed to the owning node where it is
  per-shard (reducer calls).
- **[THREAT-MODEL.md](../THREAT-MODEL.md)**: a section for the endpoint. Covers what an agent with
  the read set can learn (everything owner SQL can), what the reducer allowlist bounds and what it
  does not, and prompt-injected tool calls arriving through data the agent read from the world.
- **Gating, by the existing precedent.** `Mcp:Enabled` defaults to `false` and answers
  `403 mcp_disabled`, as `Sql:AdHocEnabled` does. `Mcp:OwnerRole` is its own role claim, as
  `Bulk:OwnerRole` and `Backup:OwnerRole` are, because "an agent may look" is a different
  capability from "a person may query". Both keys go in [CONFIGURATION.md](../CONFIGURATION.md).
- **Instrumentation**: a `melange.mcp.tool` span per call, with tool name and outcome, in
  [OBSERVABILITY.md](../OBSERVABILITY.md).

## Out of scope

- **An agent plugin, skill pack, or prompt library.** SpacetimeDB ships these. They are
  documentation with opinions about how an agent should work, which is the console argument again.
  If one is written, it lives outside this repo.
- **Write access through `query`.** `/melange/sql` reads, and `/melange/bulk` (the trusted-pipeline
  write path) is deliberately not exposed to agents at all; the only write path is a named reducer.
- **Tools over the commit log.** [Phase 21](../road-to-0.3/plan-phase-21.md)'s `melange inspect` is
  the obvious next tool (an LSN, the reducer that produced it, its write set), and should be added
  when phase 21 lands, not built ahead of it here.

## Decisions to settle

All three settled by the owner on 2026-11-21, each as leaned. The reasoning is kept below. The SDK,
checked at phase start: `ModelContextProtocol.AspNetCore` 2.4.0 (published 2026-11-07), the
official C# SDK, with its streamable HTTP transport hosted in the developer's pipeline.

### Whether reducer calls ship in the first release at all

They are what makes the endpoint useful for testing gameplay from an agent, and also the only part
that can change the world. **Settled: ship them** (owner, 2026-11-21), but only by name and only with an explicit
identity per reducer, so the default install is read-only and the grant is visible in code review.
The alternative, read-only in v1 with writes later, is defensible if the threat-model section
cannot bound prompt-injected calls to something the allowlist makes acceptable.

### Production

An endpoint that answers SQL is a production surface the moment it is enabled. `Mcp:Enabled`
defaulting to off covers a deployment that never opted in. **Open:** whether that is enough, or
whether reducer calls additionally refuse outside the Development environment unless an explicit
`Mcp:AllowReducersInProduction` is set. **Settled: the extra flag, for reducers only** (owner,
2026-11-21). Reads follow
the `/melange/sql` precedent exactly, and the failure the flag prevents is a write-capable agent
endpoint nobody remembers is live.

### Transport

MCP's HTTP transport, hosted in the developer's ASP.NET pipeline, is the natural fit for a library.
The stdio bridge covers agents that only speak stdio. **Settled: HTTP in the package, stdio only in
the CLI bridge, and nothing else** (owner, 2026-11-21).

## As built

- **`MelangeDB.Mcp`** over `ModelContextProtocol.AspNetCore` 2.4.0: `AddMelangeMcp(mcp => ...)`
  registers a stateless streamable-HTTP server and its tools; `MapMelangeMcp()` maps it at
  `{Transport:Path}/mcp`. Every mapped endpoint is wrapped by a gate (through an endpoint
  convention, since `MapMcp` endpoints take no endpoint filters): `Mcp:Enabled`, then the host's
  own token validation (the transport's authenticator, shared with `MapMelangeSocket`), then
  `Mcp:OwnerRole`.
- **One SQL implementation.** `/melange/sql`'s body moved into `AdHocSql` (gates, the row path,
  the aggregate path), which the HTTP endpoint and the `query` tool both call, so an agent gets the
  endpoint's exact answer and exact refusal. The HTTP behaviour is unchanged.
- **`call_reducer`** is one tool rather than one per reducer: the allowlist is in its description
  and its refusal, and `describe_schema` carries the parameters. It is registered only when the
  allowlist is non-empty. Unknown or non-standard names fail at map time.
- **`signals`** is a `MeterListener` over the `MelangeDB` meters, bounded at 1,000 series: gauges at
  their latest value, counters summed and histograms as count/sum/max since the endpoint started.
- **The stdio bridge** is `melange mcp <url>`: the SDK's client toward the host and its stdio server
  toward the agent, forwarding tool listings and calls and deciding nothing.
- **Not built: cluster routing for reducer calls.** Every tool answers for the node it reached, as
  the HTTP surfaces do. Routing a call to the owning shard needs the hub's shard-execute path to
  carry the caller's claims and client-call source, which it does not today (it passes neither,
  so a routed call would skip the reducer's policy). That change is the follow-up.
- **Tests** (in the transport suite, over the real HTTP transport with the SDK's own client): the
  three gates, every read tool, the SQL refusal, the allowlist, the production flag, a bad
  allowlist failing at startup, and the stdio bridge end to end over in-memory pipes.

