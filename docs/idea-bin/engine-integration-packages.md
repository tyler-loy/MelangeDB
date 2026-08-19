# Official Godot / Unity packages

**Shape:** thin, supported packages that wrap the existing client for each engine's lifecycle —
`FrameTick` wired to `_Process` / `Update`, the dispatch mode preset correctly, and the
main-thread rules enforced by construction rather than by documentation.

**Status:** undecided.

## What exists today

The mechanism is complete. `DispatchMode.Manual` queues whole data frames and applies them inside
`client.FrameTick(maxFrames)`, so every event fires on the thread that calls it — handlers may touch
a scene tree directly, with no `CallDeferred` anywhere. Frames drain whole, so a tick budget never
tears a transaction. [CLIENT-BINDINGS.md](../CLIENT-BINDINGS.md) documents the threading model in
full, including reducer continuations and the `SynchronizationContext` behaviour under Godot.

The reference workload's Godot client runs on exactly this today, without a package.

## The argument for

The mode is correct and the rules around it are sharp enough to cut. Two are already written down as
footguns rather than as compile errors:

- **Await, never block.** In Manual mode, `SubscribeAsync` / `ReconnectAsync` / `UnsubscribeAsync`
  complete only as ticks apply their frames. Blocking the ticking thread on one deadlocks the
  application, because the completion is waiting for the tick the block is preventing.
- **Discarded synchronization context.** `Task.Run(...)` or `ConfigureAwait(false)` around a reducer
  await drops the main-thread resumption a Godot user is relying on.

A package cannot make either impossible, but it can make the right shape the default one: a node or
component that owns the tick, presets the dispatch mode, and offers an API where the deadlocking
call is not the convenient one.

## The argument against

This is packaging, not capability, and packaging has a maintenance tail per engine — versions,
plugin formats, marketplace listings, and the expectation of support that a published package
creates. The whole point of `DispatchMode.Manual` was to make the client host-agnostic so this layer
stays thin and optional; owning it converts an engine-neutral library into one with two named engine
dependencies.

It also has an ordering problem: Unity has no consumer here at all, and Godot's one consumer is
already working without it. A package written from one working integration is a generalization from
a single example.

**If it happens: wrap, do not redesign.** The client's threading contract is settled and correct.
Anything that reopens it in the name of engine ergonomics is a different, worse idea.

## Reopening trigger

**Someone shipping a game hits a bug in their own wrapper that a package would have owned** — a
Manual-mode deadlock or a lost synchronization context in real integration code. That is the
evidence that the footguns are being stepped on rather than merely documented, and it comes with a
second real integration to generalize from.
