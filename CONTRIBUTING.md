# Contributing to MelangeDB

Thanks for looking. MelangeDB is alpha and pre-1.0, so the most useful contributions right now are
bug reports with a reproduction, and questions where the documentation didn't answer something it
should have.

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Getting set up

You need the [.NET 10 SDK](https://dotnet.microsoft.com/download). The exact version the repo pins
is in `global.json`.

```
git clone https://github.com/tyler-loy/MelangeDB.git
cd MelangeDB
dotnet build
dotnet test
```

Some suites use [Testcontainers](https://testcontainers.com) and need a running Docker daemon —
Postgres for the relational tier, and containers for parts of the cluster suite. Without Docker
those suites self-skip locally. **CI treats a skip as a broken environment, not a pass**, so a green
local run with skips is not the same bar as a green CI run.

## The test bar

This repo holds a deliberately high bar, and it is worth knowing before you open a PR:

- **Warnings are errors**, in every project, in both configurations.
- **The suite runs in both Release and Debug.** Some races only reproduce in one — that is why both
  run rather than just Release.
- **Zero skips.** CI fails if any suite reports a skipped test.
- **The cluster and load suites run locally, not in CI.** They are multi-node integration machinery
  calibrated against real hardware; on shared 2-vCPU runners they fail for reasons that indict the
  runner rather than the code. They still *build* in CI. If you touch `MelangeDB.Cluster`,
  `tools/MelangeDB.LoadTest`, or the handoff paths, run them locally before pushing:

```
dotnet test tests/MelangeDB.Cluster.Tests
dotnet test tests/MelangeDB.LoadTest.Tests
```

- Individual stress tests inside the CI-run suites opt out with `[Trait("Category", "Stress")]`.
  They are excluded in CI and run locally by default.

For anything touching concurrency, recovery, or handoff, the house habit is to run the affected
suite in a contended loop rather than once — a single green run does not say much about a race.

## Documentation conventions

Three rules apply to every change, not just to documentation changes. They exist because the
alternative is folklore:

- **Every configuration item goes in [docs/CONFIGURATION.md](docs/CONFIGURATION.md)** in the *same*
  change that introduces it, with its real default verified against the code. A setting that isn't
  listed there doesn't exist as far as users are concerned.
- **Every new noun goes in [docs/GLOSSARY.md](docs/GLOSSARY.md)** when the change introducing it
  lands. Vocabulary drift is how a design becomes unexplainable.
- **Every change instruments what it adds**, recorded in
  [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md). Span and metric names are public API — once a
  dashboard depends on `melange.applier.lag`, renaming it is a breaking change.

## Pull requests

- Branch from `main`. Branch names in this repo read `area/short-description` — `fix/pk-range-scan`,
  `server/bulk-owner-gate`, `client/frame-tick-pump`.
- Commit subjects read `Area: what changed, in lowercase` — for example
  `Server: a primary-key range walks keys to the range, not rows through it`. Describe the behaviour
  that changed, not the files you touched.
- Keep a PR to one concern. If you are stacking PRs, **merge them top-down or retarget as you go** —
  merging a stack bottom-up into `main` rewrites the branches above it.
- Say in the PR description what you ran, including whether you ran the local-only suites.

## Design changes

MelangeDB has a lot of deliberately-settled decisions, and many of the things that look like
omissions are recorded refusals: joins in subscriptions, an unreliable/UDP transport, a sandbox for
reducer code, exactly-once event delivery. Before proposing one of these, check
[docs/DESIGN.md](docs/DESIGN.md) and [docs/ROADMAP.md](docs/ROADMAP.md) — the reasoning is written
down, and the useful conversation starts from disagreeing with the reasoning rather than from the
proposal.

For the full reasoning behind a specific decision, [docs/road-to-0.1/](docs/road-to-0.1/) holds the
twelve phase plans the project was built from. That's where to look when something appears arbitrary
— a fair number of those are refusals with an argument attached, and several record the measurement
that settled them.

If you want to change a settled decision, open an issue describing the workload that the current
decision fails. Measurements beat arguments here, and a new measurement is what reopens one.

## Security

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md) for how to report
one privately.

For the *threat model* — what a MelangeDB server can enforce against a client it doesn't trust, and
what it deliberately doesn't — see [docs/THREAT-MODEL.md](docs/THREAT-MODEL.md).
