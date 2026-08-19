# Client interpolation helpers

**Shape:** buffer-and-interpolate helpers in the client — hold incoming row deltas a fixed delay
behind the wire, interpolate positional columns between the last two known states, and expose the
smoothed value to the game.

**Status:** undecided.

## Why this is here at all

The project has a permanent refusal — **no unreliable/UDP transport** — and it is recorded in
[ROADMAP.md](../ROADMAP.md) with reasoning: a reducer is a transaction, and a client must know
whether it committed. The supported answer to the motion-smoothness problem that UDP is usually
reached for is *rate limiting plus client-side interpolation*.

That answer is currently **stated but not shipped**. Every client implements it or does without.

This entry is not a reopening of the transport question. It is the observation that the project owes
a little more than a sentence to the alternative it named.

## The argument for

An answer to a known objection is stronger when it is a function call than when it is a paragraph.
"Use interpolation" puts the burden on each client to get buffer depth, extrapolation-on-gap, and
snap-vs-blend thresholds right, and those are the settings that decide whether the refusal *feels*
correct to a player. The refusal is sound; the experience of it is currently unowned.

It is also small and self-contained. It touches no wire format, no server, and no consistency
property — a helper over rows the client already has.

## The argument against

It is game logic, and the line between "database client" and "game framework" is one this project
has otherwise held. Interpolation policy is genuinely game-specific: what to do on a gap
(extrapolate, hold, snap), how far behind live to sit, and which columns are even interpolatable are
decisions a shooter and a farming sim answer differently. A helper that picks defaults picks a genre.

There is a narrower version worth considering if this is ever built: ship the **buffer** — a
delay-line over deltas with a known depth, which is the mechanical and genre-neutral half — and
leave the interpolation curve to the caller. That keeps the helper on the database side of the line.

## Reopening trigger

**The interpolation answer gets challenged with a real jitter measurement** — someone building on
this reports that rate limiting plus their own interpolation does not produce acceptable motion, with
numbers. That is the point at which the stated answer needs to be either supported with code or
revisited as an argument, and either way the measurement is what tells us which.
