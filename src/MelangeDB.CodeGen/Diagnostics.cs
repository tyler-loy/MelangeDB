using Microsoft.CodeAnalysis;

namespace MelangeDB.CodeGen;

/// <summary>
/// Every diagnostic MelangeDB reports at compile time. Ids are stable public API: MELANGE0001
/// through MELANGE0017, never renumbered, each with a fires-test and a compiles-clean test.
/// </summary>
public static class Diagnostics
{
    private const string Category = "MelangeDB";

    public static readonly DiagnosticDescriptor NoPrimaryKey = new(
        "MELANGE0001",
        "Table must declare exactly one [PrimaryKey] column",
        "Table '{0}' declares {1} [PrimaryKey] column(s); exactly one is required — the primary key is a row's identity",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AutoIncNotInteger = new(
        "MELANGE0002",
        "[AutoInc] requires a long or ulong column",
        "Table '{0}': [AutoInc] column '{1}' is {2}; AutoInc ids are 64-bit originator-prefixed values and require long or ulong",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UniqueOnPartitionedTable = new(
        "MELANGE0003",
        "[Unique] on a Partitioned table cannot be enforced across shards",
        "Table '{0}': [Unique] column '{1}' declares a single-writer guarantee, but the table's Placement is Partitioned — " +
        "one writer per shard means no node can see every row (see docs/CLUSTERING.md). Use Placement.Global or Placement.Replicated, or drop [Unique].",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UnserializableReducerParameter = new(
        "MELANGE0004",
        "Reducer parameter type is not serializable",
        "Reducer '{0}': parameter '{1}' of type {2} cannot be decoded from a client call. " +
        "Supported: bool, integers, floats, string, byte[], Identity, Timestamp, enums, and single-dimension arrays of these.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AmbientTimeInReducer = new(
        "MELANGE0005",
        "Reducer bodies must read time from ctx.Timestamp",
        "Reducer '{0}' reads {1}; use ctx.Timestamp so the transaction stays deterministic and replayable",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AmbientRandomInReducer = new(
        "MELANGE0006",
        "Reducer bodies must draw randomness from ctx.Random",
        "Reducer '{0}' constructs a new Random; use ctx.Random, which is seeded per commit, so the transaction stays deterministic and replayable",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ServerOnlyOnPrivateTable = new(
        "MELANGE0007",
        "[ServerOnly] declares subscription visibility the table does not have",
        "Table '{0}': column '{1}' is [ServerOnly], which masks a column on a subscription-visible table — but the table is not Public, " +
        "so no client ever sees any of it. Mark the table Public = true or remove [ServerOnly].",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AsyncReducer = new(
        "MELANGE0008",
        "Reducers are synchronous",
        "Reducer '{0}' is async. A reducer body is a synchronous critical section — the transaction commits by one atomic log append, " +
        "and awaiting invites exactly the I/O the design forbids. Do the asynchronous work outside and pass its result in as arguments.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor InvalidReducerSignature = new(
        "MELANGE0009",
        "Reducer signature is invalid",
        "Reducer '{0}' {1}. A reducer is an ordinary instance method: void return, ReducerContext first, then its client-visible parameters.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor IoTypeInReducer = new(
        "MELANGE0010",
        "Reducer bodies must perform no I/O",
        "Reducer '{0}' uses {1}, which performs I/O or blocks. A reducer runs inside the transaction's critical section; " +
        "move the I/O outside the reducer and pass its result in as arguments.",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UnsupportedColumnType = new(
        "MELANGE0011",
        "Column type is not supported",
        "Table '{0}': column '{1}' has unsupported type {2}. Supported: bool, integers, floats, string, byte[], Identity, Timestamp, enums.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor DuplicateTableName = new(
        "MELANGE0013",
        "Table names must be unique within a compilation",
        "Table '{0}' (struct {1}) collides with another table in this compilation. " +
        "The TableId derives from the table name and generated type names derive from the struct name, so both must be unique; rename one of them.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor KeyColumnNotEncodable = new(
        "MELANGE0012",
        "Column type cannot serve as a key",
        "Table '{0}': column '{1}' of type {2} cannot be a primary key or index — the type has no order-preserving byte encoding",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ScheduledReducerMissing = new(
        "MELANGE0014",
        "Scheduled table names a reducer that does not exist",
        "Table '{0}' declares Scheduled = \"{1}\", but no reducer named '{1}' exists in this compilation. " +
        "Declare [Reducer] public void {1}(ReducerContext ctx, {0} timer) on a reducer class.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ScheduledReducerSignature = new(
        "MELANGE0015",
        "Scheduled reducer signature is invalid",
        "Reducer '{0}' {1}",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ScheduleAtColumnMisplaced = new(
        "MELANGE0016",
        "ScheduleAt column placement is invalid",
        "Table '{0}' {1}",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ShardByIsPrimaryKey = new(
        "MELANGE0018",
        "ShardBy must not be the primary key",
        "Table '{0}': ShardBy = \"{1}\" names the [PrimaryKey] column. Handoff re-homes a row by rewriting its ShardBy " +
        "column while the stored row key — the encoded primary key — stays fixed, so a primary-key shard column would " +
        "silently diverge from its key on the first transfer. Give the shard id its own column.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AmbiguousClientEnumName = new(
        "MELANGE0019",
        "Client-visible enums must have unique names",
        "Enum name '{0}' is used by more than one enum on the client-visible surface. The schema manifest carries enums " +
        "by simple name — that is the name the generated client bindings declare — so two public-facing enums cannot share one. Rename one of them.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor InvalidManifest = new(
        "MELANGE0020",
        "Schema manifest is invalid",
        "The schema manifest at '{0}' cannot be read: {1}. Re-export it with `melange schema` (the MelangeDB.Cli tool) from the module build (or its dev server); no bindings were generated.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor MultipleManifests = new(
        "MELANGE0021",
        "One project generates from one schema manifest",
        "This compilation carries several melange-schema.json AdditionalFiles ({0}). The bindings share one MelangeDB.Types namespace and one connection wrapper, so one project binds one module; split consumers of different modules into separate projects.",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UnindexedScanOnPagedTable = new(
        "MELANGE0017",
        "Full scan over a table that is not Resident",
        "Iter() scans every row of table '{0}', which is not declared Resident — on a paging store that is I/O per page, " +
        "not a memory walk. If the table is small, bounded, and scan-heavy, declare Residency.Resident; if this site " +
        "looks up by a column, add [Index] and use Filter; if it only checks existence, use Any(), Count, or First(). " +
        "An operator can also pin it per deployment via MelangeDb:Residency:{0}.",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);
}
