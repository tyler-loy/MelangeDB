# Releasing

How MelangeDB packages are versioned, where they are published, and how a consumer restores them.

## What ships

Every library under `src/` is a NuGet package — eleven of them:

`MelangeDB.Abstractions`, `MelangeDB.Core`, `MelangeDB.Protocol`, `MelangeDB.Client`,
`MelangeDB.Server`, `MelangeDB.CodeGen`, `MelangeDB.Storage.Faster`, `MelangeDB.Storage.Postgres`,
`MelangeDB.Cluster`, `MelangeDB.OpenTelemetry`, `MelangeDB.Cli`.

Tests, samples, and the load-test tool are never packed (`IsPackable=false`). Shared package metadata
lives in `Directory.Build.props` at the repo root; a project's `.csproj` only adds what is specific to
it.

Three packages are not plain class libraries:

- **`MelangeDB.CodeGen`** ships as a Roslyn analyzer: the assembly lands in `analyzers/dotnet/cs`,
  there is no `lib/` output, and the package declares no dependencies (Roslyn comes from the
  consuming compiler). It is marked a development dependency, so referencing it gets
  `PrivateAssets="all"` by default — the generator runs at the consumer's compile time and never
  flows further.
- **`MelangeDB.Cli`** ships as a .NET tool (`dotnet tool install --global MelangeDB.Cli`), providing
  the `melange` command: `schema` exports a schema manifest, and `backup` / `backup verify` /
  `restore` are the operational verbs over the `.mbak` archive (docs/BACKUP.md). See
  [CLIENT-BINDINGS.md](CLIENT-BINDINGS.md).
- **Symbols are embedded** in every assembly rather than shipped as separate `.snupkg` packages, so
  there is no symbol server to be reachable and nothing to publish twice. SourceLink is on (it ships
  in the .NET SDK), so stepping into MelangeDB from a consumer resolves sources from GitHub.

## Versioning

Simple and boring, on purpose:

- **`VersionPrefix` in `Directory.Build.props` is the single source of truth** for the next
  release. Read it there rather than here — a version repeated in prose is a version that goes
  stale the first time someone releases without noticing the second copy.
- **Publishing a GitHub Release publishes that version** to nuget.org. The publish job refuses a
  release whose tag does not match `VersionPrefix` exactly — bump the prefix in the same PR that
  prepares the release, then tag the merge commit.
- After releasing, bump `VersionPrefix` to the next version.
- **Pushes to `main` do not publish to nuget.org.** They pack
  `<VersionPrefix>-ci.<run-number>`, upload it as a workflow artifact, and push it to
  this repository's **GitHub Packages** feed. A version on nuget.org can never be deleted, only
  unlisted, and a prerelease per commit to `main` is not a stream worth making permanent; a
  GitHub Packages version can be deleted, so the per-commit stream lives there instead. See
  [Consuming prereleases](#consuming-prereleases) — restoring from that feed needs
  authentication even though the repository is public.

All eleven packages always publish together at the same version. There is no per-package versioning —
a MelangeDB version is one coherent set.

**Pre-1.0 means the public API may break in any release.** That is stated in the README, the
changelog, and here, because a consumer pinning `0.x` deserves to know it before rather than after.

What 1.0 requires is deliberately undecided, with the reasoning and the trigger for deciding it
recorded in [ROADMAP.md](ROADMAP.md#the-10-question-deliberately-open). Note for whoever revisits
it: "all packages publish together at one version" is a rule written when every package was a NuGet
package, and a second ecosystem tests it — see
[road-to-0.3 phase 23](road-to-0.3/plan-phase-23.md).

## Cutting a release

1. Open a PR that bumps `VersionPrefix` in `Directory.Build.props` and moves the `CHANGELOG.md`
   `[Unreleased]` section under the new version heading. Merge it.
2. Tag the merge commit `v<VersionPrefix>` and push the tag. **Nothing publishes yet** — a tag on
   its own does nothing.
3. Draft a GitHub Release against that tag, write the notes, and publish it. That starts the run.
4. The `test` job runs first. When it passes, the `publish` job parks awaiting approval of the
   `release` environment. Approve it, and the packages go to nuget.org.
5. Bump `VersionPrefix` to the next version so `main` prereleases sort above the release. **The
   bump changes two committed files**, because a manifest records the generator version that wrote
   it and their staleness guards compare byte-for-byte on purpose. Re-export both in the same PR,
   or CI fails on the bump alone:

   ```
   dotnet build samples/MelangeDB.Sample.Worker -c Debug
   dotnet build tests/MelangeDB.Transport.Tests -c Debug
   dotnet run --project src/MelangeDB.Cli -- schema \
     samples/MelangeDB.Sample.Worker/bin/Debug/net10.0/MelangeDB.Sample.Worker.dll \
     -o samples/MelangeDB.Sample.Worker/melange-schema.json
   dotnet run --project src/MelangeDB.Cli -- schema \
     tests/MelangeDB.Transport.Tests/bin/Debug/net10.0/MelangeDB.Transport.Tests.dll \
     -o tests/MelangeDB.Transport.Tests/melange-schema.json
   ```

   The generator-code snapshots under `tests/MelangeDB.CodeGen.Tests/Snapshots/` need no such
   treatment — they pin the version to `0.0.0.0` before comparing, because a snapshot of generated
   code asserts the code, not which build emitted it.

Two deliberate gates sit between a typo and a permanent nuget.org version: a pushed tag publishes
nothing by itself, and the publish job waits on a reviewer even after the release exists. That
asymmetry is on purpose — a version on nuget.org can never be deleted, only unlisted.

## Publishing

Two jobs in `.github/workflows/ci.yml` handle packages, and the split matters:

- **`pack`** runs on `main` — a push, or a manual dispatch from that branch — packs the `-ci.N`
  prerelease, uploads it as a workflow artifact, and pushes it to **GitHub Packages**. It has **no**
  `environment`, so ordinary pushes never wait on a reviewer, and it authenticates with the built-in
  `GITHUB_TOKEN` — there is no secret to configure. The only setting it needs is
  `permissions: packages: write` on the job.
- **`publish`** runs **only** on `release: [published]`, is gated behind the `release` environment,
  and pushes to **nuget.org** via Trusted Publishing. Both jobs need the `test` job green first,
  and both push with `--skip-duplicate` so re-running a workflow is idempotent — which matters
  for `pack`, because a re-run reuses `github.run_number` and therefore repacks a version it has
  already pushed.

The two feeds carry different things on purpose: **nuget.org gets releases, GitHub Packages gets
every commit to `main`.** Nothing reaches nuget.org without a published GitHub Release and a
reviewer.

A package is filed against a repository by the `<RepositoryUrl>` baked into the nupkg — set once in
`Directory.Build.props`. Push a package whose `RepositoryUrl` names a different repository and the
feed rejects it rather than quietly filing it somewhere else, so that property is load-bearing for
more than the "Source" link on the listing.

### Trusted Publishing

Publishing uses [nuget.org Trusted Publishing](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing),
so **there is no long-lived API key stored anywhere**. The job requests a signed OIDC token from
GitHub, nuget.org validates it against a policy naming this repository, and exchanges it for an API
key valid for one hour. Nothing to rotate, nothing to leak.

The moving parts:

- **`permissions: id-token: write`** on the `publish` job. Without it GitHub issues no OIDC token
  and the login step fails.
- **`NuGet/login@v1`** runs immediately before the push, not at the top of the job — the exchanged
  key expires in an hour, and one token buys exactly one key.
- **`NUGET_USER`**, a repository **secret** (Settings → Secrets and variables → Actions) holding the
  nuget.org **profile name** — not an email address. It isn't really a credential, but the workflow
  reads `secrets.NUGET_USER`, so setting it as a *variable* instead leaves it empty; the job fails
  with an explicit message rather than a confusing login error if that happens.

The policy on nuget.org is bound to three things, and each one is a way to break publishing:

| Policy field | Value here | Breaks if |
| --- | --- | --- |
| Repository owner | `tyler-loy` | The repo moves to an org |
| Repository | `MelangeDB` | The repo is renamed |
| Workflow file | `ci.yml` | The file is renamed, or the publish job moves to another workflow file |
| Environment (optional) | `release` | The job's `environment:` is renamed or removed |

The workflow-file binding is the easy one to trip over: **the publish job must stay in `ci.yml`**, or
the policy has to be updated to match. The field is the bare file name — not the
`.github/workflows/` path.

**Setting the Environment field is worth doing.** The `publish` job declares `environment: release`,
and naming that in the policy makes nuget.org reject an OIDC token minted from any other context —
so the environment stops being only a GitHub-side approval prompt and becomes something nuget.org
enforces too. Leave the field empty and any workflow run from this repo's `ci.yml` satisfies the
policy, approval gate or not.

Create the environment under Settings → Environments → `release`, and add yourself as a required
reviewer. Without a reviewer configured the environment exists but gates nothing.

Two more things worth knowing:

- **A policy created against a private repo starts "temporarily active" for 7 days.** nuget.org needs
  the GitHub repository and owner IDs to pin the policy against resurrection attacks, and it only
  learns them from a successful publish. If no publish happens inside that window the policy goes
  inactive — restartable at any time, but worth knowing before the first release. Publishing once
  after the repo is public makes it permanently active.
- **Reserve the `MelangeDB.*` package ID prefix** (Account → Reserved package ID prefixes) if you
  haven't. That's separate from trusted publishing, and it's what stops anyone else from publishing a
  `MelangeDB.Something` that looks official.

## Consuming the packages

Once published, nothing special is required — nuget.org is a default source in every SDK install:

```
dotnet add package MelangeDB.Core
dotnet add package MelangeDB.Server
dotnet add package MelangeDB.Storage.Faster
dotnet add package MelangeDB.CodeGen
```

Or as a `PackageReference`:

```xml
<ItemGroup>
  <PackageReference Include="MelangeDB.Core" Version="0.2.0" />
  <PackageReference Include="MelangeDB.Server" Version="0.2.0" />
  <PackageReference Include="MelangeDB.Storage.Faster" Version="0.2.0" />
  <PackageReference Include="MelangeDB.CodeGen" Version="0.2.0" />
</ItemGroup>
```

`MelangeDB.CodeGen` is the source generator; a `PackageReference` to it is all it takes — the
generator runs during the consumer's build exactly as the in-repo `ProjectReference ...
OutputItemType="Analyzer"` form does.

The `melange` CLI installs as a tool:

```
dotnet tool install --global MelangeDB.Cli
```

### Consuming prereleases

Every push to `main` publishes `<VersionPrefix>-ci.<run-number>` to GitHub Packages. That feed is
not a default source, and — this is the part that surprises people — **restoring from it requires
authentication even though this repository is public.** GitHub Packages has never supported
anonymous NuGet restore; a public package is publicly *visible*, not publicly *restorable*.

Add the source with a classic personal access token carrying the `read:packages` scope. Put it in a
`nuget.config` beside the consuming solution rather than in the global one, so the credential's
blast radius is one repository:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="melangedb-ci" value="https://nuget.pkg.github.com/tyler-loy/index.json" />
  </packageSources>
  <packageSourceCredentials>
    <melangedb-ci>
      <add key="Username" value="YOUR_GITHUB_USERNAME" />
      <add key="ClearTextPassword" value="%GITHUB_PACKAGES_TOKEN%" />
    </melangedb-ci>
  </packageSourceCredentials>
</configuration>
```

`%GITHUB_PACKAGES_TOKEN%` is expanded from the environment, so the token itself stays out of the
file and out of version control. Then reference an explicit prerelease version — a floating range
would drag in a new build of `main` on every restore, which is not what you want holding a
dependency steady:

```xml
<PackageReference Include="MelangeDB.Core" Version="0.1.1-ci.42" />
```

All eleven packages are pushed together at the same `-ci.N`, so pin them to one number. Prereleases
are **not** a supported surface: they are whatever `main` was at that commit, they can be deleted,
and the API may change between two consecutive ones. Use them to try a fix before a release exists,
not to run anything you care about.

## CI

`.github/workflows/ci.yml` is the whole pipeline. Its `test` job runs on every PR, every push to
`main`, every published release, and on demand — **not** on a pushed tag, which triggers nothing at
all, so don't wait for a check mark to appear on one: restore, Release build (warnings-as-errors),
then the test suite in
**both** Release and Debug. Skipped tests fail the run — the Postgres suite self-skips when
Docker is missing, and CI treats a skip as a broken environment, not a pass. `ubuntu-latest` has
Docker preinstalled, which is what the Testcontainers suites use.

The cluster and load suites build in CI but do not execute there; they are calibrated against real
hardware and must be run locally. See [CONTRIBUTING.md](../CONTRIBUTING.md) for that bar.

**Recovering a lost run on `main`.** Actions → CI → *Run workflow*, with `main` selected. Use it
when a run on `main` never produced a result and `Re-run jobs` will not clear it — an outage, a
runner shortage, a run orphaned mid-re-run. Dispatching from `main` runs `test` and `pack`, so it
produces the `-ci.N` prerelease the lost run owed; dispatching from a topic branch runs `test` only.
Nothing about dispatch can reach nuget.org — `publish` fires on `release: [published]` and nothing
else. This trigger exists because without it the only recovery was an empty commit on `main`,
which is how the 2026-08-06 outage was going to have to be settled (#55).

CI sets `MELANGE_TEST_TIME_SCALE=4` (a test-infra knob, not a MelangeDB option): the integration
suites' wait-helper deadlines multiply by it so shared slow vCPUs get proportionally more wall
clock. Assertions are unchanged — the awaited conditions still have to come true. Unset (the
default, scale 1) on real hardware.
