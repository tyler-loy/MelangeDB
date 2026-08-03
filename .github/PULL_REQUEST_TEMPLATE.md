<!-- Subjects in this repo read "Area: what changed, in lowercase" — describe the behaviour that
     changed, not the files touched. -->

## What changed

<!-- And why. If it settles a decision that was open, say which one. -->

## Tests run

- [ ] `dotnet test` green in **Release**
- [ ] `dotnet test` green in **Debug**
- [ ] Cluster and load suites (`MelangeDB.Cluster.Tests`, `MelangeDB.LoadTest.Tests`) — these do not
      run in CI; required if this touches clustering, handoff, or the load rig
- [ ] Stress-trait tests, if this touches concurrency, recovery, or handoff

## Documentation

- [ ] New configuration items are in `docs/CONFIGURATION.md` with defaults verified against the code
- [ ] New nouns are in `docs/GLOSSARY.md`
- [ ] New spans/metrics are in `docs/OBSERVABILITY.md`
- [ ] `CHANGELOG.md` updated, if this is user-visible
- [ ] Not applicable

## Anything reviewers should look at first

<!-- The part you are least sure about is the most useful thing to name here. -->
