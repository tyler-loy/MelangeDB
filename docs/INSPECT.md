# Inspecting a world's history

The commit log is a full record of every commit: when it happened, which identity called which
reducer with which arguments, what it wrote, and the events it published. `melange inspect` reads
that record. Point it at a data directory or a `.mbak` archive and it shows the log without a
debugger, without the application's assemblies, and without changing anything.

```
melange inspect log    <source> [--from <lsn>] [--to <lsn>] [--reducer <name>] [--caller <hex>] [--table <name>] [--json]
melange inspect record <source> --at-lsn <n> [--manifest melange-schema.json] [--json]
```

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
row policy, private tables included, so who may read a data directory or an archive is the same
question as who may run it.

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
value and each delete's key. It has no before-images.

**Event payloads are shown as bytes.** An event's type and depth are structural; its payload is the
application's own encoding, with nothing on disk that describes it.

**Archive-tier writes are marked.** An archive-tier table's rows ride the log like any other write
and then live only in Postgres. The inspector shows them for as long as the log holds them.

## `--json`

Every verb takes `--json` and writes one JSON document: `source` (the facts), the answer
(`records` for `log`, `record` for `record`), `notes`, and `changedWhileRead`. Integers up to 32 bits
and floats are JSON numbers. 64-bit integers are strings, because a JSON number past 2^53 loses
digits in most parsers and AutoInc ids live up there. Bytes are hex, identities are hex, and
timestamps are ISO 8601. An argument is `{ "name"?, "type", "value" }`.

## Exit codes

`0` answered; `1` the source could not be read, or does not hold what was asked for (the reason is
on stderr); `2` the command line was wrong.
