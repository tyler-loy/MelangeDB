# Additional client languages

**Shape:** a **thin** Python client — connect, query the SQL endpoint, call a reducer — generated
from the same `melange-schema.json` manifest. Deliberately not a peer of the C# client.

**Status:** undecided. The TypeScript client is a planned phase; Python is not, and this entry
records why the two are different decisions rather than one decision about "more languages."

## What the second language actually costs

Scoping the TypeScript client made the sharing profile concrete. The generator splits into four
parts and they do not share equally:

| Part | Marginal cost per additional language |
| --- | --- |
| Manifest parse and client model (`ManifestParser`, `ClientModel`) | **Zero.** Hand-rolled JSON, netstandard2.0, no dependencies — already language-neutral by construction. |
| Emitter templates (`ClientEmitter`) | Small. This is the only C#-bound piece of generation. |
| **The client runtime** | **Most of the work.** Protocol v2 row decoding, the refcounted merged cache, subscription lifecycle, `Resume` and log epochs, `DropAndResync` backpressure, connect tickets. |
| Conformance | Currently C#-only (`ClientRowShapeTests`, `WireRowTests`, `TypedBindingsTests`). |

The conclusion that matters: **a second language is not a rider on the first.** Codegen sharing
saves roughly the cheap quarter; the runtime is re-implemented each time. Treating "and Python too"
as an increment of the TypeScript phase would be planning against a cost model that isn't real.

## Why TypeScript is a phase and Python is not

The reference workload has three consumer trees — a Godot game (C#), an admin web, and a terrain-gen
CLI (C#). TypeScript has a **named consumer**; Python has none anywhere in this project.

That distinction is not bureaucratic. Phase 11's lesson was that the live port found five defects
the entire test suite had missed — the recovery regression, the client identity gap, the
transient-rejection shape, the reducer-error mismapping, the silent shape adoption. A client library
in a language nobody is using is surface that cannot be validated that way, and shipping it would
assert a parity nothing has tested.

## Why "thin," if it ever happens

Python's plausible role here is not a game client. It is ops, load-testing, data work, and agents
driving a world — a **read-and-call** profile: run a SQL query, call a reducer, maybe consume a raw
subscription stream. That profile does not want the refcounted merged cache, which is the expensive
half of the runtime and the half that exists to keep a game's scene graph in sync.

Building a full peer of the C# client for that consumer would be building the wrong thing carefully.
If Python lands, it should be scoped as the thin client from the start and say so in its own name.

## What makes this cheap later

Two deliverables belong to the TypeScript phase whether or not a third language ever happens, and
they are what turns this entry from an argument into a scoped task:

1. **`melange generate --lang <x>`** — lift the parser and model behind a CLI verb so an emitter no
   longer requires a Roslyn host.
2. **A written client-conformance definition**, backed by a wire-level suite runnable against any
   language. This is the house pattern: protocol v2 built its safety net before it removed the old
   format.

With conformance defined, a Python client is a scoped phase or a credible community port. Without
it, every new language is a fresh argument from first principles.

## Reopening trigger

**A named consumer, once client conformance exists.** Both halves are required — a consumer without
conformance means an unvalidated second runtime, and conformance without a consumer means
maintaining a library for nobody.
