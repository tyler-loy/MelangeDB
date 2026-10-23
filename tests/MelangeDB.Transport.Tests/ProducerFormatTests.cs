using MelangeDB.Core;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The producer formats, held together: the frames round-trip, the handshake settles on the highest
/// version both sides speak, and what the server's <see cref="ProducerResultWriter"/> writes the
/// client's <see cref="ProducerResultReader"/> reads.
/// </summary>
public class ProducerFormatTests
{
    private readonly MessagePackFrameSerializer _serializer = new();

    [Fact]
    public void Producer_frames_round_trip()
    {
        var call = new CallProducerFrame(7, "History", [1, 2, 3], "00-trace") { Channel = MelangeChannels.Calls };
        var back = Assert.IsType<CallProducerFrame>(_serializer.Deserialize(_serializer.Serialize(call)));
        Assert.Equal((7u, "History", "00-trace", MelangeChannels.Calls), (back.RequestId, back.Producer, back.TraceParent, back.Channel));
        Assert.Equal(call.Arguments, back.Arguments);

        var ok = new ProducerResultFrame(7, true, 42, new byte[] { 9, 8 }, null, null);
        var okBack = Assert.IsType<ProducerResultFrame>(_serializer.Deserialize(_serializer.Serialize(ok)));
        Assert.Equal((true, 42UL), (okBack.Ok, okBack.Lsn));
        Assert.Equal(new byte[] { 9, 8 }, okBack.Payload.ToArray());

        var failed = new ProducerResultFrame(8, false, 0, ReadOnlyMemory<byte>.Empty, MelangeErrorCodes.Timeout, "slow");
        var failedBack = Assert.IsType<ProducerResultFrame>(_serializer.Deserialize(_serializer.Serialize(failed)));
        Assert.Equal((false, MelangeErrorCodes.Timeout, "slow"), (failedBack.Ok, failedBack.ErrorCode, failedBack.Message));
        Assert.True(failedBack.Payload.IsEmpty);
    }

    [Theory]
    [InlineData(2, 3, 3)]
    [InlineData(2, 2, 2)]
    [InlineData(3, 9, 3)]
    [InlineData(1, 2, 2)]
    public void The_handshake_settles_on_the_highest_common_version(int min, int max, int expected) =>
        Assert.Equal(expected, MessagePackFrameSerializer.Negotiate(min, max));

    [Theory]
    [InlineData(1, 1)]
    [InlineData(4, 5)]
    public void A_range_with_no_common_version_is_refused(int min, int max) =>
        Assert.Null(MessagePackFrameSerializer.Negotiate(min, max));

    [Fact]
    public void What_the_server_writes_the_client_reads()
    {
        var writer = new ProducerResultWriter();
        writer.WriteArrayHeader(10);
        writer.WriteBool(true);
        writer.WriteInt64(-5);
        writer.WriteUInt64(ulong.MaxValue);
        writer.WriteFloat64(1.25);
        writer.WriteString("héllo");
        writer.WriteString(null);
        writer.WriteBytes([1, 2]);
        writer.WriteIdentity(Identity.Hash("x"));
        writer.WriteTimestamp(new Timestamp(123));
        writer.WriteNull();

        var reader = new ProducerResultReader(writer.Written.Span);
        Assert.Equal(10, reader.BeginArray());
        Assert.True(reader.ReadBool());
        Assert.Equal(-5, reader.ReadInt32());
        Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
        Assert.Equal(1.25f, reader.ReadFloat32());
        Assert.Equal("héllo", reader.ReadString());
        Assert.Null(reader.ReadString());
        Assert.Equal(new byte[] { 1, 2 }, reader.ReadBytes());
        Assert.Equal(Identity.Hash("x"), reader.ReadIdentity());
        Assert.Equal(new Timestamp(123), reader.ReadTimestamp());
        Assert.Equal(-1, reader.BeginArray());
        reader.End();
    }

    [Fact]
    public void A_shape_mismatch_is_reported_rather_than_misread()
    {
        var writer = new ProducerResultWriter();
        writer.WriteString("not a number");

        var failure = Assert.Throws<ProducerResultFormatException>(() => new ProducerResultReader(writer.Written.Span).ReadInt64());
        Assert.Contains("regenerate", failure.Message, StringComparison.Ordinal);
    }
}
