# Security policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Report privately through GitHub's
[private vulnerability reporting](https://github.com/tyler-loy/MelangeDB/security/advisories/new)
on this repository. If that is unavailable to you, email **tyler@loy.ninja** with `MelangeDB
security` in the subject.

Please include as much of the following as you can:

- The type of issue — row or column leakage, authentication bypass, denial of service, cluster
  trust-boundary violation, and so on.
- The affected component (`MelangeDB.Server`, `MelangeDB.Cluster`, the code generator, …) and
  version or commit.
- A reproduction — a failing test is ideal, a description of the steps is fine.
- What an attacker gets out of it.

You will get an acknowledgement within a few days. MelangeDB is a pre-1.0 project maintained by one
person, so please treat any timeline as best-effort rather than a guarantee.

## Supported versions

MelangeDB is **alpha and pre-1.0**. Only `main` is supported: fixes land there, and there are no
backports to earlier prereleases. Do not run this in production yet.

## Scope

In scope — anything that breaks a guarantee MelangeDB makes:

- A client observing rows or columns that its row/column policies should have hidden, including via
  the wire protocol rather than the client API.
- Authentication or authorization bypass: connect tickets, mid-session re-authentication, identity
  collision, owner-mode SQL, the bulk endpoint, or reducer policies.
- A client forcing server behaviour it shouldn't reach — invoking scheduled or lifecycle reducers,
  escaping subscription cost limits, or bypassing rate limits.
- Violations of the cluster trust boundary described in [docs/THREAT-MODEL.md](docs/THREAT-MODEL.md).
- Memory-safety or state-corruption bugs reachable from client input, including reducer argument
  decoding.

Out of scope — these are documented design decisions, not vulnerabilities:

- **Reducer code is not sandboxed.** It runs with the host process's full authority, by design; it
  is your code in your executable. See [docs/DESIGN.md](docs/DESIGN.md) §1.
- **A client cannot be stopped from misusing data it legitimately received.** Column policies narrow
  what a cheat can know; they cannot police what a client does with what it is permitted to see.
- **MelangeDB mints no identities.** Abuse of account or guest-token issuance is your identity
  provider's concern.
- Anything requiring the cluster secret, an owner-role claim, or filesystem access to the commit
  log — all of these are inside the trust boundary by definition.

If you are unsure whether something is in scope, report it privately anyway.
