; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
MELANGE0001 | MelangeDB | Error | Table must declare exactly one [PrimaryKey] column
MELANGE0002 | MelangeDB | Error | [AutoInc] requires a long or ulong column
MELANGE0003 | MelangeDB | Warning | [Unique] on a Partitioned table cannot be enforced across shards
MELANGE0004 | MelangeDB | Error | Reducer parameter type is not serializable
MELANGE0005 | MelangeDB | Warning | Reducer bodies must read time from ctx.Timestamp
MELANGE0006 | MelangeDB | Warning | Reducer bodies must draw randomness from ctx.Random
MELANGE0007 | MelangeDB | Warning | [ServerOnly] declares subscription visibility the table does not have
MELANGE0008 | MelangeDB | Error | Reducers are synchronous
MELANGE0009 | MelangeDB | Error | Reducer signature is invalid
MELANGE0010 | MelangeDB | Warning | Reducer bodies must perform no I/O
MELANGE0011 | MelangeDB | Error | Column type is not supported
MELANGE0012 | MelangeDB | Error | Column type cannot serve as a key
MELANGE0013 | MelangeDB | Error | Table names must be unique within a compilation
MELANGE0014 | MelangeDB | Error | Scheduled table names a reducer that does not exist
MELANGE0015 | MelangeDB | Error | Scheduled reducer signature is invalid
MELANGE0016 | MelangeDB | Error | ScheduleAt column placement is invalid
MELANGE0017 | MelangeDB | Warning | Full scan over a table that is not Resident
MELANGE0018 | MelangeDB | Error | ShardBy must not be the primary key
MELANGE0019 | MelangeDB | Error | Client-visible enums must have unique names
MELANGE0020 | MelangeDB | Error | Schema manifest is invalid
MELANGE0021 | MelangeDB | Error | One project generates from one schema manifest
MELANGE0022 | MelangeDB | Error | Scheduled tables are always Placement.Local
MELANGE0023 | MelangeDB | Warning | Read-modify-write inside a snapshot-isolated reducer
MELANGE0024 | MelangeDB | Error | Archive tables cannot be Public
MELANGE0025 | MelangeDB | Error | [Unique] on an archive table cannot be checked
MELANGE0026 | MelangeDB | Error | Archive table declares something only a hot table can honour
MELANGE0027 | MelangeDB | Error | KeepKeys applies only to archive tables
MELANGE0028 | MelangeDB | Error | Producer signature is invalid
MELANGE0029 | MelangeDB | Error | Producer result type is not serializable
MELANGE0030 | MelangeDB | Error | Producer names must be unique across producers and reducers
MELANGE0031 | MelangeDB | Error | Producer result types must have unique client names
