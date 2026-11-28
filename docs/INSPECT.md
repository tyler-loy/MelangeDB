# Inspecting a world's history

The commit log is a full record of every commit: when it happened, which identity called which
reducer with which arguments, what it wrote, and the events it published. `melange inspect` reads
that record. Point it at a data directory or a `.mbak` archive and it shows the log without a
debugger, without the application's assemblies, and without changing anything.

```
melange inspect log    <source> [--from <lsn>] [--to <lsn>] [--reducer <name>] [--caller <hex>] [--table <name>] [--diff] [--json]
melange inspect record <source> --at-lsn <n> [--diff] [--manifest melange-schema.json] [--json]
melange inspect row    <source> --table <name> (--key <value> | --key-hex <hex>) [--from <lsn>] [--to <lsn>] [--json]
melange inspect at     <source> --at-lsn <n> [--table <name> [--key <value> | --key-hex <hex>] [--all]] [--json]
```

Three questions bring people here, and each has a verb:

1. **What happened at this LSN?** `inspect record --at-lsn <n>` shows one commit in full. `inspect
   log` finds the LSN when you know only a time, a reducer or a caller.
2. **What happened to this row?** `inspect row --table <t> --key <k>` lists every commit that
   touched it, with who did it.
3. **What did the world look like then?** `inspect at --at-lsn <n>` rebuilds it.

## A walkthrough

A player reports that a deer teleported across the map around 09:14 UTC. Start with the commits in
that minute that wrote the `Creature` table. The facts come first, then one line per commit:

```
$ melange inspect log ./data/log --table Creature --from 41100
...
  LSN  TIME (UTC)                   CALLER            WRITES  EVENTS  REDUCER
41187  2026-11-27 09:13:57.103112Z  0000000000000000      40       0  TickCreatures
41201  2026-11-27 09:13:58.004127Z  560827fa6cb1578f       3       1  Attack
41244  2026-11-27 09:14:01.512930Z  560827fa6cb1578f       1       0  DebugMoveCreature
```

`DebugMoveCreature` from a player identity is suspicious. Ask what happened to the deer:

```
$ melange inspect row ./data/log --table Creature --key 77
row       Creature  Id = 77  (encoded 0x000000000000004d)
history   3 commits touched it
...
LSN 41244  2026-11-27 09:14:01.512930Z  update  by DebugMoveCreature  caller 560827fa6cb1578ff5ae...
      X  1880.5   (was 212.25)
      Y  -640     (was 96)
```

Then look at the call itself, with its arguments named from the schema manifest:

```
$ melange inspect record ./data/log --at-lsn 41244 --manifest melange-schema.json --diff
...
reducer   DebugMoveCreature
arguments 3
  [0] id = 77
  [1] x = 1880.5
  [2] y = -640
```

The full caller identity is on the `caller` line, and `inspect log --caller 560827fa` lists
everything else that identity did. To see the area as it stood just before, `inspect at --at-lsn
41243 --table Creature` prints every creature as of that commit.

If the answer is older than the log reaches, every verb says so and names the floor. Point the
same commands at the newest backup taken before it: `melange inspect row world.mbak ...` reads an
archive exactly as it reads a directory. When you know what went wrong, `melange restore --at-lsn`
(the LSN just before the mistake) and `melange clone` are where a finding leads; see
[BACKUP.md](BACKUP.md).

## Sources

`<source>` is one engine's data directory (the directory `CommitLog:Path` names, or one shard's
directory), a segment file inside one, or a `.mbak` archive. An archive from a cluster holds several
engines (`hub`, `shard-1`, ...); `--engine <key>` picks one. Each engine has its own log and its own
LSNs, so the inspector reads one at a time.

## What it is safe to point at

**Anything, including a running server's directory.** The inspector never writes and takes no
lock. It opens every file for reading with write and delete sharing, so a live engine holding
`melange.lock`, or another cluster node owning the shard, is never in its way, and it is never in
theirs. Its view is of the moment it opened: it reads to the head it found then and no further,
and after its answer it says what moved under it while it read:

- **the engine appended past the head**: open the directory again to see the newer records;
- **the log was truncated**: the records shown were read before it;
- **the shard changed owner**: a new owner sealed the live segment, so the answer is of the log
  before the move.

A partial record after the head is expected in a live directory (an append still being written)
and after a crash (a torn tail). It is counted in the facts, never shown, and never refused. A CRC
failure with intact records after it is real corruption, and the inspector refuses the source, as
recovery would.

It has no network surface. Reading the files is the authorization: the inspector reads past every
row and column policy, private tables included, so who may read a data directory or an archive is
the same question as who may run it. [THREAT-MODEL.md](THREAT-MODEL.md#the-inspector-phase-21)
states both properties.

## The facts come first

Every verb opens with the source's facts, because investigations fail at their edges:

```
source    /srv/world/log (data directory)
epoch     6c3e1a2b-...
lineage   descends from epoch 1a2b3c4d-... up to LSN 41200: history at or below it is inherited
records   LSN 41201..52310 (11110 records)
floor     LSN 41200 (truncation base)
snapshot  LSN 50000 (2026-11-27 09:14:02.118000Z)
owner     term 2, after 1 sealed segment from earlier owners
note      read without a lock, to LSN 52310; an engine running against this directory may be ahead of it
```

- **epoch** is what an LSN means. A restore mints a new epoch that continues numbering from the
  restored head, so LSN 41000 in two epochs can be two different commits.
- **lineage** names the epochs a restored log descends from and the fork point below which their
  history is this log's too. **clone** says the world is a clone, and of what.
- **records** is the range the source holds, and **floor** is the LSN at or below which it holds
  nothing. For a directory that is the truncation base. An archive holds its snapshot and only the
  log above it, so its floor is its snapshot's LSN.
- **owner** appears once a shard has changed owner. The log is then several segments, one per
  ownership term, each sealed at a cut. Records a previous owner wrote past its cut were never
  history, and the inspector never shows them.

Asking for an LSN the source does not hold gets the reason, not an empty answer. Below the floor
that is a truncation: [OBSERVABILITY.md](OBSERVABILITY.md#runbook-the-log-is-growing--who-is-holding-it) names every truncation floor,
so the next question (which holder let the record go) has an answer there, and a backup taken
before the truncation still holds the record.

## `inspect log`: who called what, and when

One line per commit: its LSN, its time (UTC), the first 16 hex digits of the caller's identity, how
many rows it wrote, how many events it published, and the reducer.

```
  LSN  TIME (UTC)                   CALLER            WRITES  EVENTS  REDUCER
41201  2026-11-27 09:13:58.004127Z  560827fa6cb1578f       3       1  Attack
41202  2026-11-27 09:13:58.020316Z  0000000000000000      12       0  TickCreatures
```

- `--from <lsn>` and `--to <lsn>` bound the window, inclusively. A tick window is an LSN range with
  the time column beside it.
- `--reducer <name>` keeps commits by one reducer (exact, case-sensitive).
- `--caller <hex>` keeps commits whose caller identity starts with these hex digits.
- `--table <name>` keeps commits that wrote at least one row of that table.

A schema migration's marker record shows as `__melange/shape (schema migration marker)`: the place
in the log where a new shape began to govern the rows.

## `inspect record`: one commit in full

```
LSN 41201  2026-11-27 09:13:58.004127Z  (412 bytes on disk)
caller    560827fa6cb1578ff5aebc94f0064979633c9622f9d4e0bf9c461b92ce4a9b0c
reducer   Attack
arguments 2
  [0] target = 77
  [1] weapon = "axe"
writes    3
  update Creature  Id = 77
      Id      77
      Health  12
  ...
events    1
  CreatureHit  depth 0  9 bytes  0x...  (an event payload is the application's bytes; no shape records it)
```

**Rows decode from the shape sidecar, under the shape that governed the record's LSN.** Row bytes
are positional: they mean nothing without the column list they were written under.
`melange.shape` beside the log (and inside every archive) keeps that list as a history, so a
record written before a migration decodes with the columns it had then, not today's. A directory
without the sidecar shows rows and keys as bytes, and says so.

**Arguments decode from their own bytes.** Each argument carries its type, so the values come out
without the application: an integer is `int64` or `uint64` whatever the parameter declared, and an
enum is its integer. What the bytes do not carry is the parameter's name. `--manifest
melange-schema.json` (the file `melange schema` writes) supplies names for the reducers a client
can call. Scheduled reducers are not in the manifest, so their arguments stay unnamed.

**A write shows what the commit wrote, not what it changed from.** The log records each row's new
value and each delete's key. It has no before-images. `--diff` rebuilds them: see
[below](#--diff-what-each-write-replaced).

**Event payloads are shown as bytes.** An event's type and depth are structural; its payload is the
application's own encoding, with nothing on disk that describes it.

**Archive-tier writes are marked.** An archive-tier table's rows ride the log like any other write
and then live only in Postgres. The inspector shows them for as long as the log holds them.

## `inspect row`: who mutated this row, and when

Every commit in the window whose write set touched one key, oldest first, with the reducer and the
caller that did it. The first version shows every column, and each later one shows only what
changed from the version before:

```
row       Creature  Id = 77  (encoded 0x000000000000004d)
history   3 commits touched it

LSN 40112  2026-11-27 09:02:11.410022Z  insert  by SpawnCreature  caller 0000000000000000...
      Id      77
      X       12.5
      Health  30
LSN 41201  2026-11-27 09:13:58.004127Z  update  by Attack  caller 560827fa6cb1578f...
      Health  12   (was 30)
LSN 41377  2026-11-27 09:15:40.920553Z  delete  by Attack  caller 560827fa6cb1578f...
      (deleted)
```

- `--table <name>` names the table, case-sensitive, as the shape sidecar records it.
- `--key <value>` is the primary key, parsed by the key column's kind: a number, a string, `true`
  or `false`, hex for bytes and identities (64 hex digits), and microseconds or an ISO 8601 time
  for a timestamp. A key printed by any verb is accepted back as written.
- `--key-hex <hex>` is the encoded key a write set carries (`--json` prints it as `key.hex`), for a
  source with no shape sidecar, or a table no shape names.

It is a scan of write sets rather than an index lookup: it reads every record in the window, so
`--from` and `--to` are what make it fast on a long log. The history begins at the source's floor.
An archive's snapshot sits exactly at its floor, so for an archive the row's version in the
snapshot is printed first, as the state the history starts from. An archive-tier table's history
stops where the log was truncated: older versions are in Postgres, not here.

## `inspect at`: the world as of a commit

```
world     as of LSN 41201 (2026-11-27 09:13:58.004127Z)
built     from the snapshot at LSN 41000, then 201 records replayed
this is   raw rows decoded from the recorded shapes. No indexes, no SQL, no row policies, no residency: a row absent here is absent from the table.
left out  AuditLog: archive-tier, so their rows live in Postgres, not here

tables
  Creature  1840 rows
  Player     212 rows
```

The world is rebuilt in memory, forward: the snapshot replayed to the LSN when the snapshot is at
or before it, or the whole log replayed from empty while it still reaches back to the first
commit. Nothing else can be built. A state before the snapshot, with the log truncated below it,
would have to be run backwards, and the log does not hold what a write replaced. The inspector
says so, naming the snapshot's LSN and the truncation base, and a backup taken before that LSN is
the remedy.

- With no `--table`, it lists every table and its row count.
- `--table <name>` prints the table's rows in primary-key order, each with the LSN that last wrote
  it. It stops at 10,000 rows and says how many it left out; `--all` prints every row.
- `--table <name> --key <value>` (or `--key-hex`) prints one row, never guarded, or says that no
  row has the key as of that LSN.

**What it is not.** It is the rows the log and the snapshot hold, decoded: correct names, kinds and
values. There are no indexes (they are projections rebuilt from declarations, and the inspector
has none), no SQL, no row policies and no residency. A row missing here is missing from the
table, not just unreachable by an index. Each row decodes under the shape that governed the LSN
that wrote it, so after a migration one table can show older rows with fewer columns. That is how
the bytes are stored, until the engine rewrites them.

**Archive-tier tables are never materialized.** Their rows live in Postgres, never in a snapshot
or an archive, so a projection could only ever hold a fragment of them. `left out` names them, and
asking for one by `--table` says to query Postgres.

The replay is the cost: proportional to the snapshot's rows plus the records between the snapshot
and the LSN. The row guard is about what lands on a terminal, not about the work.

## `--diff`: what each write replaced

`record --diff` and `log --diff` show each write beside the value it replaced:

```
  update Creature  Id = 77
      Id      77
      Health  12   (was 30)
  delete Item  Id = 9
      Owner   (was 77)
```

An insert says that no row had the key before; a delete lists the columns it removed. `log --diff`
prints every matching record in full this way, so it is `record` over a window (and `--manifest`
names arguments there too). It is one replay carried forward across the window, standing just
before its first record, never a replay per record. It needs what `at` needs, a world it can build
just before the window, and it is refused, for the same reason, where `at` would be. It is
opt-in because it is slow, and that is what keeps `inspect log` fast.

## `--json`

Every verb takes `--json` and writes one JSON document: `source` (the facts), the answer
(`records` for `log`, `record` for `record`, `row`, `snapshot` and `history` for `row`, `world`
with `rows` or `row` for `at`), `notes`, and `changedWhileRead`. Under `--diff`, each write carries
`before`: the row it replaced, or `null`. Integers up to 32 bits
and floats are JSON numbers. 64-bit integers are strings, because a JSON number past 2^53 loses
digits in most parsers and AutoInc ids live up there. Bytes are hex, identities are hex, and
timestamps are ISO 8601. An argument is `{ "name"?, "type", "value" }`.

## Exit codes

`0` answered; `1` the source could not be read, or does not hold what was asked for (the reason is
on stderr); `2` the command line was wrong.
