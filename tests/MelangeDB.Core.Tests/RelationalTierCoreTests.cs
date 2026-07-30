using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>One column of every key-encodable kind, for the decode round-trip.</summary>
[Table]
public partial struct KeyKinds
{
    [PrimaryKey]
    public long Signed;

    public ulong Unsigned;

    public int Int;

    public short Short;

    public byte Byte;

    public bool Flag;

    public string Name;

    public Identity Who;

    public Timestamp At;

    public ChunkKind Kind;
}

/// <summary>
/// The engine-side halves of the relational tier contract, none of which need Postgres:
/// relational tables project into the hot store too (decision 4a — reducer reads, uniqueness,
/// and the overlay all work through the ordinary paths); decoupled appliers are tracked but never
/// driven by the pipeline; and the boxed key decode round-trips every key-encodable kind.
/// </summary>
public class RelationalTierCoreTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void Relational_reads_are_read_your_writes_in_the_same_transaction()
    {
        _harness.Invoke("Register", ctx =>
        {
            var inserted = ctx.Db.Insert(new Registration { Email = "same-tx@example.com", CreatedAt = ctx.Timestamp });

            // The overlay path: the row written above is visible before commit.
            var found = ctx.Db.Find<Registration>(inserted.Id);
            Assert.NotNull(found);
            Assert.Equal("same-tx@example.com", found.Value.Email);
        });

        // And after commit the relational table reads like any table — it also projects into the
        // hot store; Postgres is an additional projection, not the read path for reducers.
        _harness.Invoke("Verify", ctx => Assert.Single(ctx.Db.Scan<Registration>()));
    }

    [Fact]
    public void Decoupled_appliers_are_tracked_but_never_driven_by_the_pipeline()
    {
        var decoupled = new ManualApplier("decoupled");
        _harness.Engine.Appliers.RegisterDecoupled(decoupled);

        _harness.Invoke("Write", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1] }));
        _harness.Invoke("Write", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 2, Data = [2] }));

        // The commit path never called Apply, and shutdown catch-up must not either.
        Assert.Empty(decoupled.Applied);
        _harness.Engine.Checkpoint();
        Assert.Empty(decoupled.Applied);

        // But it is a full citizen for lag reporting…
        Assert.Contains(_harness.Engine.Appliers.Lags(), l => l.Applier == "decoupled" && l.Lag == 2);

        // …and the pipeline refuses to pretend it can drive it.
        Assert.Throws<InvalidOperationException>(() => _harness.Engine.Appliers.Pause("decoupled"));
        Assert.Throws<InvalidOperationException>(() => _harness.Engine.Appliers.Resume("decoupled"));
    }

    [Fact]
    public void Key_decode_round_trips_every_key_encodable_kind()
    {
        var registry = SchemaRegistry.FromTypes(typeof(KeyKinds));
        var schema = registry.Get(typeof(KeyKinds));
        foreach (var (column, value) in new (string, object)[]
        {
            ("Signed", -42L),
            ("Unsigned", ulong.MaxValue - 1),
            ("Int", int.MinValue + 5),
            ("Short", (short)-7),
            ("Byte", (byte)200),
            ("Flag", true),
            ("Name", "melange"),
            ("Who", Identity.Hash("decode")),
            ("At", new Timestamp(1_753_800_000_000_000)),
            ("Kind", ChunkKind.Ore),
        })
        {
            var columnSchema = schema.Column(column);
            var encoded = KeyCodec.Encode(columnSchema, value);
            Assert.Equal(value, KeyCodec.Decode(columnSchema, encoded));
        }
    }

    private sealed class ManualApplier(string name) : ILogApplier
    {
        public List<CommitRecord> Applied { get; } = [];

        public string Name { get; } = name;

        public ulong AppliedLsn { get; private set; }

        public void Apply(CommitRecord record)
        {
            Applied.Add(record);
            AppliedLsn = record.Lsn;
        }
    }
}
