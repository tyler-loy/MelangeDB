# Dev host that migrates in place on `dotnet watch`

**Shape:** in a development host, a schema change detected on rebuild applies to the running data
directory without dropping connected clients — the inner loop becomes "save, world updates" rather
than "save, restart, reconnect, re-navigate."

**Status:** undecided. This is a papercut, filed as one.

## What exists today

The hard half is done. Phase 16 shipped hot-tier schema migration: a shape sidecar, additive changes
replaying by name-mapped rebuild, destructive changes refused loudly with the pending DDL printed.
Phase 08 settled the same rule for the relational tier. [MIGRATION.md](../MIGRATION.md) describes
both, and the rule is one rule across the two tiers.

So a schema change against an existing log already works. What does not exist is the *loop*: a
rebuild restarts the host, which drops every client, and each one reconnects from scratch.

## The argument for

This is the cost that developers pay most often and think about least. A game developer changing one
column re-runs the whole path back to the state they were testing — reconnect, re-auth, re-subscribe,
walk back to where the bug was. The migration itself is instant; the re-navigation is not.

The reference workload is developed on MelangeDB daily, which means this cost is being paid daily by
the project's own most important consumer.

## The argument against

**Scope creep toward hot reload.** Migrating data in place is well-defined; keeping a live host's
*code* current across a rebuild is .NET hot reload's problem, and reducer bodies are exactly the kind
of code hot reload handles worst. A dev host that migrates the data but still needs a process restart
to pick up a changed reducer has solved the smaller half of the annoyance.

**Dev-only surface earns its keep slowly.** Anything that behaves differently in development than in
production is a place where a bug can hide until deployment, and this would be a fairly large such
place — a second startup path, exercised constantly by developers and never by production.

**The client side is not free either.** "Without dropping clients" means the connected clients hold
bindings generated from the *old* manifest. Phase 16 was explicit that a stale client is a handshake
refusal; making the dev loop seamless means deciding what a mid-session schema bump does to a client
that is already connected, which is a protocol question, not a convenience feature.

That last point is probably the real content of this idea, and it is why this is not obviously small.

## Reopening trigger

**The reconnect loop costs more than the fix** — measured crudely and honestly: developers on the
reference workload timing what a schema-change iteration actually costs them over a week. If it is
seconds, this stays here forever. If it is minutes many times a day, it is a phase, and it should be
scoped around the client-side question above rather than around `dotnet watch`.
