# The agent endpoint (MCP)

`MelangeDB.Mcp` puts a [Model Context Protocol](https://modelcontextprotocol.io) endpoint in your
own host, so a coding agent pointed at a running world can read its schema, query it, check its
health and metrics, and, only where you allow it, call named reducers. It is an adapter over
surfaces MelangeDB already publishes, with the host's own authentication in front of it. It ships
no UI and no agent prompts: it is a protocol, and any MCP client can use it.

```
dotnet add package MelangeDB.Mcp
```

```csharp
builder.Services.AddMelangeDb(melange => melange.AddTablesFrom(typeof(Creature).Assembly) /* ... */);
builder.Services.AddMelangeMcp(mcp => mcp
    .AllowReducers("SpawnTestCreature")                                // runs as the agent's own identity
    .AllowReducer("ResetZone", Identity.Hash("agent/zone-reset")));    // always runs as this identity

var app = builder.Build();
app.UseWebSockets();
app.MapMelangeSocket();
app.MapMelangeMcp();       // {Transport:Path}/mcp, by default /melange/mcp
```

```json
{ "MelangeDb": { "Mcp": { "Enabled": true } } }
```

## Who gets in

Every request passes three gates before the MCP server sees it:

1. **`Mcp:Enabled`** (default `false`). Off answers `403 mcp_disabled`, the `Sql:AdHocEnabled`
   precedent. It is read per request, so turning it off takes effect at once.
2. **A valid bearer token**, validated exactly as the socket and `/melange/sql` validate one
   (`Auth:*`). Missing or invalid answers `401`.
3. **The `Mcp:OwnerRole` claim** (default `melange-agent`). Without it the answer is
   `403 owner_required`. It is its own role, as `Bulk:OwnerRole` and `Backup:OwnerRole` are:
   "an agent may look" is a different capability from "a person may query". Issue it from your IdP
   to the tokens your agents use, and to nothing else.

The endpoint is stateless streamable HTTP: each call is one authenticated POST.

## The tools

| Tool | What it does |
| --- | --- |
| `describe_schema` | The schema manifest, the same document `/melange/schema` serves: tables, columns, keys, indexes, visibility, and every client-callable reducer with its parameters. |
| `query` | One ad-hoc SQL query under **exactly** the rules `/melange/sql` applies to the same caller: `Sql:AdHocEnabled`, `Sql:AdHocMode`, row and column policies in policy-enforced mode, the row and byte ceilings, and the cluster's `partitioned_elsewhere` refusal. Holding `Mcp:OwnerRole` does not grant owner SQL; that is still `Sql:OwnerRole`. Returns `{"columns":[...],"rows":[[...]]}`, with 64-bit integers, timestamps and `COUNT`s as decimal strings, as `/melange/sql` writes them. |
| `health` | Head, durable and base LSN, each applier's lag, and the governing truncation floor with every floor's position, on the node that answered. |
| `signals` | A bounded read (at most 1,000 series) of the instruments [OBSERVABILITY.md](OBSERVABILITY.md) names: gauges at their current value, counters summed and histograms as count/sum/max since the endpoint started. Filter by name prefix. |
| `call_reducer` | Present only when you named reducers. Calls one of them; see below. |

Arguments that are not plain JSON use the same convention as `/melange/call`:
`{"$identity":"<hex>"}`, `{"$bytes":"<base64>"}`, `{"$timestamp":<unix micros>}`,
`{"$interval":<micros>}`. A refusal comes back as a tool error whose text starts with the wire
code the HTTP surface would have answered with (`sql_disabled: ...`, `rejected: ...`).

## Reducer calls

Reading is the default; writing is named. `AllowReducers(...)` and `AllowReducer(name, runAs)` list
the reducers an agent may call, in code, where the grant is visible in review. There is no
wildcard, and a name that is not a declared standard reducer fails at startup.

- **A call is an ordinary client call.** Arguments are validated against the declared types, the
  reducer's policy runs, `RateLimit:*` applies, and the commit is logged and traced like any
  other. The tool returns `{"ok":true,"lsn":N}`.
- **Who it runs as.** `AllowReducers` runs each call as the identity in the agent's token, with
  that token's claims, so the reducer's policy judges the agent. `AllowReducer(name, runAs)` always
  runs as `runAs`, with no claims: use it for a fixed test identity.
- **Outside Development, reducer calls are off** unless `Mcp:AllowReducersInProduction` is set.
  Reads are unaffected. The failure this prevents is a write-capable agent endpoint nobody
  remembers is live.

## Local agents that speak stdio: `melange mcp`

An agent that only launches MCP servers as subprocesses uses the CLI's bridge:

```
melange mcp https://game.example.com --token <jwt>     # or MELANGE_TOKEN in the environment
```

It forwards every tool listing and call to the host's endpoint (adding `/melange/mcp` when the URL
names no path) and decides nothing itself: the host's gates, allowlist and policies apply as they
would over HTTP. Stdout carries only the protocol; diagnostics go to stderr.

## Clusters

Map the endpoint on the hub. The read tools answer for the node they reached, as the HTTP surfaces
do: `query` reads the hub's Global and Replicated tables and refuses Partitioned ones with
`partitioned_elsewhere`; `health` and `signals` describe the hub.

**A reducer call names its shard.** On a cluster hub `call_reducer` requires `shard`, and the hub
sends the call to the node that owns it. It arrives there as the client call it is: the agent's
identity, its token's claims and guest flag travel with it, so argument validation, the
reducer's policy and `RateLimit:*` apply on that node, and a refusal comes back with its own code
(`denied:`, `rate_limited:`, `outcome_unknown:` when the link to the node failed mid-call). The
`lsn` returned is that shard's. A shard that does not exist is refused (`invalid_args:`) and never
created. A call without `shard` is refused rather than run on the hub's engine, which is not
where a shard's reducers commit; hub-site reducers are therefore not callable from the agent
endpoint on a cluster. On a single node `shard` is ignored.

## Threat model

What an agent with the read tools can learn, what the allowlist bounds, and how prompt injection
through world data fits in: [THREAT-MODEL.md](THREAT-MODEL.md#the-agent-endpoint-phase-29).
