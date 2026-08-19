# Area-of-interest subscriptions

**Shape:** a subscription helper that takes a position and a radius instead of a key range.

```csharp
await conn.Db.Creature.SubscribeAroundAsync(x, z, radius: 64);
```

**Status:** undecided. Nothing is refused here and nothing is scheduled.

## What exists today

Every piece this would be built from is shipped. The spatial strategy draws the grid (phase 10),
range predicates are one of the four supported SQL shapes, and the typed bindings already emit a
range helper:

```csharp
var sub = await conn.Db.Creature.ChunkId.SubscribeRangeAsync(0, 10);
// ... WHERE ChunkId BETWEEN :lo AND :hi
```

The reference workload's terrain streaming is exactly this shape —
`SELECT * FROM terrain_chunk_data WHERE chunk_id BETWEEN :lo AND :hi` is in
[DESIGN.md](../DESIGN.md) as the canonical range example.

## The argument for

"Entities near me" is the single most common interest query a game client writes, and every client
currently invents it: linearize a 2-D position onto a key, decide how many chunks of slop the radius
needs, re-subscribe on movement, and pick a hysteresis policy so walking a boundary doesn't thrash
subscriptions. None of that is hard. All of it is folklore, and each client gets to be wrong about
it independently.

This would be **sugar, not mechanism** — same single-table deltas, same wire, same range predicate
underneath. That is what makes it cheap and also what makes it easy to keep deferring.

## The argument against, and the real open question

Sugar over a shipped primitive is the weakest kind of feature until the desugaring is unambiguous,
and here it isn't. A radius is a circle; the underlying predicate is a key range over a linearized
grid. The helper has to choose:

- **The linearization.** Row-major, Morton, or the strategy's own grain. If the helper picks and the
  strategy disagrees, the helper is wrong in a way the user cannot see.
- **The slop.** A circle inscribed in chunks over-fetches; one that clips under-fetches. Either is
  defensible; silently picking is not.
- **The re-subscribe policy on movement.** This is the part clients actually get wrong, and it is
  the part a helper would most need to own — which makes it stateful, which makes it more than sugar.

So the honest version of this idea is *either* a thin helper that documents its slop and leaves
movement to the caller, *or* a stateful interest tracker that is a genuinely larger feature. Picking
between those is the work, and it should not be picked in the abstract.

A prerequisite either way: the helper needs the shard strategy's linearization to be a first-class
readable thing rather than a convention shared by hand between server and client.

## Reopening trigger

**A second consumer independently reinventing the pattern.** The reference workload has one
(terrain streaming). One instance is a use case; two is a missing feature, and the second one also
tells us which of the two shapes above is the right one — because we will have two real examples to
generalize from instead of one and a guess.
