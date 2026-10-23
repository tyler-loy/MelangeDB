using System.Reflection;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The code the generator emits for <c>[Producer]</c> methods (road-to-0.3 phase 26), run for
/// real: arguments decode with defaults filled in, the body's value comes back through the async
/// adapter, and results encode as one tagged value that the reducer-argument reader — the format
/// they share — reads back field by field.
/// </summary>
public class ProducerCodegenTests
{
    private static readonly ValidationOptions Limits = new();

    private static readonly IReadOnlyDictionary<string, ProducerDescriptor> Producers =
        ((IMelangeModel)Activator.CreateInstance(
            typeof(NoteProducers).Assembly.GetCustomAttribute<MelangeGeneratedModelAttribute>()!.ModelType)!)
        .Producers()
        .ToDictionary(static p => p.Name);

    [Fact]
    public void The_generated_model_registers_every_producer_under_its_public_name()
    {
        Assert.Equal(["Answer", "Echo", "NoteIds", "Nothing", "Page"], Producers.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(typeof(NoteProducers), Producers["Page"].ProducerClass);
        Assert.Equal(ReducerSite.Hub, Producers["Answer"].ExecutionSite);
    }

    [Fact]
    public async Task A_scalar_result_encodes_as_one_tagged_value()
    {
        var reader = Decode(await CallAsync("Answer"));

        Assert.Equal(42, reader.ReadInt32());
        reader.End();
    }

    [Fact]
    public async Task Defaulted_parameters_may_be_left_off_and_the_nested_result_reads_back_in_declaration_order()
    {
        var bytes = await CallAsync("Page", ReducerArguments.Encode(5), new StubDb(
            new Note { Id = 1, Text = "a", Score = 1.5, Author = Identity.Hash("x") },
            new Note { Id = 2, Text = "b", Score = 2.0, Author = Identity.Hash("y") }));

        var reader = Decode(bytes);
        Assert.Equal(4, reader.BeginArray()); // NotePage: Lines, Next, At, Summary
        Assert.Equal(2, reader.BeginArray());
        Assert.Equal(7, reader.BeginArray()); // NoteLine, in record parameter order
        Assert.Equal(1UL, reader.ReadUInt64());
        Assert.Equal("a", reader.ReadString());
        Assert.Equal(1.5, reader.ReadFloat64());
        Assert.Equal(Identity.Hash("x"), reader.ReadIdentity());
        Assert.Equal((byte)Mood.Calm, reader.ReadUInt8()); // the default the call left off
        Assert.Equal(1, reader.ReadInt32()); // Rank
        Assert.Null(reader.ReadByteArray()); // Blob
        Assert.Equal(7, reader.BeginArray());
        Assert.Equal(2UL, reader.ReadUInt64());
        Assert.Equal("b", reader.ReadString());
        Assert.Equal(2.0, reader.ReadFloat64());
        Assert.Equal(Identity.Hash("y"), reader.ReadIdentity());
        Assert.Equal((byte)Mood.Calm, reader.ReadUInt8());
        Assert.Equal(-1, reader.BeginArray()); // a null int? rides as Null, which reads as an absent array
        Assert.Null(reader.ReadByteArray());
        Assert.Null(reader.ReadString()); // Next: fewer rows than the page size
        Assert.Equal(new Timestamp(1_000), reader.ReadTimestamp());
        Assert.Equal(2, reader.BeginArray()); // NoteSummary: the field, then the property
        Assert.Equal(2, reader.ReadInt32());
        Assert.Equal(3L, reader.ReadInt64());
        reader.End();
    }

    [Fact]
    public async Task Supplied_arguments_override_the_defaults()
    {
        var bytes = await CallAsync("Page", ReducerArguments.Encode(1, "a", Mood.Loud), new StubDb(
            new Note { Id = 1, Text = "a" },
            new Note { Id = 2, Text = "b" },
            new Note { Id = 3, Text = "c" }));

        var reader = Decode(bytes);
        Assert.Equal(4, reader.BeginArray());
        Assert.Equal(1, reader.BeginArray());
        Assert.Equal(7, reader.BeginArray());
        Assert.Equal(2UL, reader.ReadUInt64()); // after "a", one row
        Assert.Equal("b", reader.ReadString());
        _ = reader.ReadFloat64();
        _ = reader.ReadIdentity();
        Assert.Equal((byte)Mood.Loud, reader.ReadUInt8());
    }

    [Fact]
    public async Task Arrays_round_trip_and_a_null_result_is_the_null_tag()
    {
        var echo = Decode(await CallAsync("Echo", ReducerArguments.Encode((object)new[] { "x", "y" })));
        Assert.Equal(2, echo.BeginArray());
        Assert.Equal("x", echo.ReadString());
        Assert.Equal("y", echo.ReadString());
        echo.End();

        var nothing = Decode(await CallAsync("Nothing"));
        Assert.Equal(-1, nothing.BeginArray());
        nothing.End();
    }

    [Fact]
    public async Task A_lazy_query_result_is_materialized_by_the_encoder()
    {
        var reader = Decode(await CallAsync("NoteIds", db: new StubDb(new Note { Id = 7 }, new Note { Id = 9 })));

        Assert.Equal(2, reader.BeginArray());
        Assert.Equal(7UL, reader.ReadUInt64());
        Assert.Equal(9UL, reader.ReadUInt64());
    }

    [Fact]
    public void Validation_refuses_too_many_arguments_and_too_few_required_ones()
    {
        Assert.Throws<ReducerArgumentException>(() => Validate("Page", ReducerArguments.Encode(1, "a", Mood.Calm, 4)));
        Assert.Throws<ReducerArgumentException>(() => Validate("Echo", ReducerArguments.Encode()));
        Validate("Page", ReducerArguments.Encode());
    }

    [Fact]
    public async Task An_encoder_stops_at_the_response_cap_rather_than_building_the_whole_answer()
    {
        var words = Enumerable.Range(0, 1000).Select(static i => $"word-{i}").ToArray();
        var value = await Producers["Echo"].Invoke(new NoteProducers(), Context(new StubDb()), ReducerArguments.Encode((object)words), Limits);

        var writer = new ProducerResultWriter(maxBytes: 100);
        var failure = Assert.Throws<ProducerResultTooLargeException>(() => Producers["Echo"].Encode(writer, value));
        Assert.Equal(100, failure.Limit);
        Assert.True(writer.Length <= 100);
    }

    private static void Validate(string producer, byte[] arguments)
    {
        var reader = new ReducerArgsReader(arguments, Limits);
        Producers[producer].Validate(ref reader);
    }

    private static async Task<byte[]> CallAsync(string producer, byte[]? arguments = null, IDbView? db = null)
    {
        var descriptor = Producers[producer];
        var value = await descriptor.Invoke(new NoteProducers(), Context(db ?? new StubDb()), arguments ?? [], Limits);
        var writer = new ProducerResultWriter();
        descriptor.Encode(writer, value);
        return writer.Written.ToArray();
    }

    private static ProducerContext Context(IDbView db) =>
        new(Identity.Hash("caller"), ConnectionId.None, new Timestamp(1_000), lsn: 1, db, CancellationToken.None);

    /// <summary>A result is one value with no count header; prefixing one lets the argument reader read it.</summary>
    private static ReducerArgsReader Decode(byte[] result) =>
        new(new byte[] { 1, 0 }.Concat(result).ToArray(), new ValidationOptions { RejectNonFiniteFloats = false });

    /// <summary>A view holding a few <see cref="Note"/> rows: enough for producers that scan.</summary>
    private sealed class StubDb(params Note[] notes) : IDbView
    {
        public TRow Insert<TRow>(TRow row)
            where TRow : struct => throw new NotSupportedException();

        public void Update<TRow>(TRow row)
            where TRow : struct => throw new NotSupportedException();

        public bool Delete<TRow>(object primaryKey)
            where TRow : struct => throw new NotSupportedException();

        public TRow? Find<TRow>(object primaryKey)
            where TRow : struct => throw new NotSupportedException();

        public IEnumerable<TRow> Scan<TRow>()
            where TRow : struct => notes.Cast<TRow>();

        public IEnumerable<TRow> Filter<TRow>(string column, object value)
            where TRow : struct => throw new NotSupportedException();

        public IEnumerable<TRow> FilterRange<TRow>(string column, object low, object high)
            where TRow : struct => throw new NotSupportedException();
    }
}
