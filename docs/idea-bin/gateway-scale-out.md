# More than one gateway

**Shape:** run the gateway (`MapMelangeGateway`) in processes of its own, several behind a load
balancer, each reaching the hub and the shard nodes over the network, instead of only inside the
hub process.

**Status:** undecided. Recorded during road-to-0.4 phase 31, when the game's shape reached 1,600
players on one machine and every one of them was connected through the single gateway.

## What exists today

The gateway lives in the hub process and reaches the hub's state directly, not over a link. A
`GatewayConnection` calls, in process:

- `HubRuntime.MintAssertion` to authenticate each upstream session;
- `HubRuntime.ResolveShard` to find a shard's owner;
- the hub engine's committed view, to resolve a session's shard from the application's map;
- `HubRuntime.Handoffs` and `HubRuntime.ShardMoves`, the notifiers that tell it a player is
  moving, when to hold calls, and when to swap.

Phase 31 made the gateway cheaper per player: one link set per shard node instead of a socket per
player per node, batched link messages, and fewer copies per relayed message. It did not change
that there is one of it.

## Why it might be needed

All client sockets terminate in one process, and every delta to every player passes through it:
680,000 rows a second at 1,600 players on one machine, with the gateway sharing that process with
the hub. Past some population the gateway's CPU, memory and socket count are the ceiling however
many shard nodes there are, and the hub's own work (the session map, sagas, heartbeats) competes
with it.

## What it would take

- **The hub's services over the wire.** Assertion minting, shard resolution and the session map
  become requests on the node link (or a gateway link like it), cached where staleness is safe.
- **Handoff notifications as messages.** A saga today calls each gateway's observer synchronously
  (`NotifyDestinationAuthoritative` must return before the release is requested, so the origin is
  muted first). Across processes that ordering becomes a request the saga awaits from every
  gateway holding the player, which is a protocol and a failure mode (a gateway that does not
  answer) the in-process call does not have.
- **Client placement.** A load balancer in front; nothing in the protocol ties a client to a
  gateway, so any gateway can take any client.

**Cost:** a subsystem. The notifier ordering is the hard part and the reason it is not small.

## Reopening trigger

A measurement, on separate machines for the driver and the cluster, where the gateway process is
the saturated resource (CPU or relay throughput) while shard nodes and the hub's own work have
headroom. Until then, the single gateway is not the measured limit.
