# Releasing

How MelangeDB packages are versioned, where they are published, and how a consumer restores them.

## What ships

Every library under `src/` is a NuGet package — ten of them:

`MelangeDB.Abstractions`, `MelangeDB.Core`, `MelangeDB.Protocol`, `MelangeDB.Client`,
`MelangeDB.Server`, `MelangeDB.CodeGen`, `MelangeDB.Storage.Faster`, `MelangeDB.Storage.Postgres`,
`MelangeDB.Cluster`, `MelangeDB.OpenTelemetry`.

Tests, samples, and tools are never packed (`IsPackable=false`). Shared package metadata lives in
`Directory.Build.props` at the repo root; a project's `.csproj` only adds what is specific to it.

Two packages are not plain class libraries:

- **`MelangeDB.CodeGen`** ships as a Roslyn analyzer: the assembly lands in `analyzers/dotnet/cs`,
  there is no `lib/` output, and the package declares no dependencies (Roslyn comes from the
  consuming compiler). It is marked a development dependency, so referencing it gets
  `PrivateAssets="all"` by default — the generator runs at the consumer's compile time and never
  flows further.
- **Symbols are embedded** in every assembly rather than shipped as `.snupkg` symbol packages,
  because GitHub Packages has no symbol server. SourceLink is on (it ships in the .NET SDK), so
  stepping into MelangeDB from a consumer resolves sources from GitHub.

## Versioning

Simple and boring, on purpose:

- **`VersionPrefix` in `Directory.Build.props` is the single source of truth** for the next
  release (currently `0.1.0`).
- **Every push to `main` publishes a prerelease**: `<VersionPrefix>-ci.<run-number>+<short-sha>`,
  e.g. `0.1.0-ci.42+a1b2c3d`. The run number makes prereleases sort correctly; the sha is build
  metadata for tracing a package back to its commit.
- **A tag `v<VersionPrefix>` publishes the stable version**, e.g. tag `v0.1.0` publishes `0.1.0`.
  The publish job refuses a tag that does not match `VersionPrefix` exactly — bump the prefix
  in the same PR that prepares the release, then tag the merge commit.
- After tagging, bump `VersionPrefix` to the next version so `main` prereleases immediately start
  sorting above the release they follow.

All ten packages always publish together at the same version. There is no per-package versioning —
a MelangeDB version is one coherent set.

## The feed

Packages publish to the repo owner's GitHub Packages NuGet feed:

```
https://nuget.pkg.github.com/tyler-loy/index.json
```

Publishing is the `publish` job in `.github/workflows/ci.yml`: on every push to `main`
(prerelease) and on `v*` tags (stable), it packs all ten projects and pushes with
`--skip-duplicate`, so re-running a workflow is idempotent. It runs **only after the test job
passes** — a broken main never publishes — and authenticates with the built-in `GITHUB_TOKEN`
under `permissions: packages: write`, so no PAT is stored anywhere for publishing.

**Visibility follows the repository.** The repo is currently private, so the packages are private:
only accounts with read access to the repo can restore them. If the repo ever goes public, every
package version published from it becomes public too — there is no per-package override on
repo-scoped GitHub Packages. The options at that point are: accept public packages, move private
feeds to a separate private repo or an organization-level feed, or switch to a different registry.

## Consuming the packages

GitHub's NuGet feed requires authentication even for public packages, so every consumer needs a
personal access token (classic) with the **`read:packages`** scope.

1. Create the PAT at <https://github.com/settings/tokens> (classic token, `read:packages`; it must
   also be SSO-authorized if your org enforces SSO).
2. Add the source to a `nuget.config` next to your solution — credentials go in environment
   variables or your user-level config, **never committed**:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <add key="melangedb" value="https://nuget.pkg.github.com/tyler-loy/index.json" />
  </packageSources>
  <packageSourceCredentials>
    <melangedb>
      <add key="Username" value="%GITHUB_USERNAME%" />
      <add key="ClearTextPassword" value="%GITHUB_TOKEN%" />
    </melangedb>
  </packageSourceCredentials>
</configuration>
```

With `GITHUB_USERNAME` and `GITHUB_TOKEN` set in the environment, `dotnet restore` works; in a
consuming repo's own GitHub Actions, `GITHUB_TOKEN` works as the password only if that repo has
been granted access to the packages, otherwise use a PAT there too.

3. Reference the packages. A game server host wants:

```xml
<ItemGroup>
  <PackageReference Include="MelangeDB.Core" Version="0.1.0-ci.*" />
  <PackageReference Include="MelangeDB.Server" Version="0.1.0-ci.*" />
  <PackageReference Include="MelangeDB.Storage.Faster" Version="0.1.0-ci.*" />
  <PackageReference Include="MelangeDB.CodeGen" Version="0.1.0-ci.*" />
</ItemGroup>
```

`MelangeDB.CodeGen` is the source generator; a `PackageReference` to it is all it takes — the
generator runs during the consumer's build exactly as the in-repo `ProjectReference ...
OutputItemType="Analyzer"` form does.

## CI

`.github/workflows/ci.yml` is the whole pipeline. Its `test` job runs on every PR, push to
`main`, and `v*` tag: restore, Release build (warnings-as-errors), then the full test suite in
**both** Release and Debug. Skipped tests fail the run — the Postgres suite self-skips when
Docker is missing, and CI treats a skip as a broken environment, not a pass. `ubuntu-latest` has
Docker preinstalled, which is what the Testcontainers suites use. The `publish` job (above) is
gated behind it with `needs: test` and runs only on push events, never for pull requests.

CI sets `MELANGE_TEST_TIME_SCALE=4` (a test-infra knob, not a MelangeDB option): the integration
suites' wait-helper deadlines multiply by it so shared slow vCPUs get proportionally more wall
clock. Assertions are unchanged — the awaited conditions still have to come true. Unset (the
default, scale 1) on real hardware.
