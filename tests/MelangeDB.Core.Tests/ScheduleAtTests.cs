using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The ScheduleAt column type and the timer-table schema rules: one discriminated shape, one wire
/// format shared by the generated codec and the reflection serializer, and a Scheduled table that
/// is private and Local whatever its attribute claims.
/// </summary>
public class ScheduleAtTests
{
    [Fact]
    public void Instants_and_intervals_round_trip_and_default_is_an_epoch_instant()
    {
        var instant = ScheduleAt.Instant(new Timestamp(1_234_567));
        Assert.False(instant.IsInterval);
        Assert.Equal(1_234_567, instant.DueAt.UnixTimeMicroseconds);

        var interval = ScheduleAt.Interval(TimeSpan.FromSeconds(5));
        Assert.True(interval.IsInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), interval.Every);

        Assert.Equal(instant, ScheduleAt.FromMicroseconds(interval: false, 1_234_567));
        Assert.NotEqual(instant, ScheduleAt.FromMicroseconds(interval: true, 1_234_567));

        ScheduleAt fromTimestamp = new Timestamp(9);
        Assert.False(fromTimestamp.IsInterval);
        ScheduleAt fromSpan = TimeSpan.FromMinutes(1);
        Assert.True(fromSpan.IsInterval);

        Assert.Throws<ArgumentOutOfRangeException>(() => ScheduleAt.Interval(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => ScheduleAt.Interval(TimeSpan.FromSeconds(-1)));

        var defaulted = default(ScheduleAt);
        Assert.False(defaulted.IsInterval);
        Assert.Equal(0, defaulted.DueAt.UnixTimeMicroseconds);
    }

    [Fact]
    public void Generated_codec_and_reflection_serializer_agree_on_timer_rows_byte_for_byte()
    {
        var generated = EngineHarness.GeneratedRegistry(typeof(DecayTimer)).Get(typeof(DecayTimer));
        var reflected = SchemaRegistry.FromTypes(typeof(DecayTimer)).Get(typeof(DecayTimer));
        var codec = Assert.IsType<RowCodec<DecayTimer>>(generated.Codec, exactMatch: false);

        var row = new DecayTimer { Id = 7, ScheduledAt = ScheduleAt.Interval(TimeSpan.FromSeconds(30)), Target = "flora" };
        var fromCodec = codec.Serialize(in row);
        var fromReflection = RowSerializer.Serialize(reflected, row);
        Assert.Equal(fromReflection, fromCodec);

        var decodedByCodec = codec.Deserialize(fromReflection);
        Assert.Equal(row, decodedByCodec);
        var decodedByReflection = (DecayTimer)RowSerializer.Deserialize(reflected, fromCodec);
        Assert.Equal(row, decodedByReflection);

        var oneShot = row with { ScheduledAt = ScheduleAt.Instant(new Timestamp(42)) };
        Assert.Equal(oneShot, codec.Deserialize(codec.Serialize(in oneShot)));
    }

    [Fact]
    public void A_scheduled_table_is_implicitly_private_and_local_on_both_schema_paths()
    {
        foreach (var schema in new[]
        {
            EngineHarness.GeneratedRegistry(typeof(DecayTimer)).Get(typeof(DecayTimer)),
            SchemaRegistry.FromTypes(typeof(DecayTimer)).Get(typeof(DecayTimer)),
        })
        {
            Assert.False(schema.IsPublic);
            Assert.Equal(Placement.Local, schema.Placement);
            Assert.Equal(nameof(DecayReducers.Decay), schema.Scheduled);
        }
    }

    [Fact]
    public void Schema_construction_enforces_the_ScheduleAt_placement_rules()
    {
        var columns = new[]
        {
            Column("Id", ColumnKind.UInt64, typeof(ulong), primaryKey: true),
            Column("When", ColumnKind.ScheduleAt, typeof(ScheduleAt)),
        };

        // A ScheduleAt column outside a Scheduled table is meaningless — and would otherwise leak
        // onto the wire, which has no encoding for it.
        var unscheduled = Assert.Throws<NotSupportedException>(() =>
            new TableSchema(typeof(DecayTimer), "Unscheduled", columns));
        Assert.Contains("only valid on a table declaring Scheduled", unscheduled.Message);

        // A Scheduled table without exactly one ScheduleAt column has no schedule to read.
        var bare = Assert.Throws<NotSupportedException>(() => new TableSchema(
            typeof(DecayTimer),
            "Bare",
            [Column("Id", ColumnKind.UInt64, typeof(ulong), primaryKey: true)],
            scheduled: "Tick"));
        Assert.Contains("exactly one ScheduleAt column", bare.Message);

        // Declaring it Public and Partitioned changes nothing: timer rows are scheduling data.
        var forced = new TableSchema(
            typeof(DecayTimer),
            "Forced",
            columns,
            isPublic: true,
            placement: Placement.Partitioned,
            scheduled: "Tick");
        Assert.False(forced.IsPublic);
        Assert.Equal(Placement.Local, forced.Placement);

        // And a ScheduleAt column can never key or index anything.
        Assert.False(SchemaKeyCodec.IsKeyEncodable(ColumnKind.ScheduleAt));
    }

    private static ColumnSchema Column(string name, ColumnKind kind, Type clrType, bool primaryKey = false) => new()
    {
        Name = name,
        ClrType = clrType,
        Kind = kind,
        IsPrimaryKey = primaryKey,
        GetValue = static _ => null,
        SetValue = static (_, _) => { },
    };
}
