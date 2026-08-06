using System.Text;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The commit-log payload is written into a pooled buffer by a span writer rather than through a
/// <c>MemoryStream</c> and a <c>BinaryWriter</c>. That is an allocation change and must not be a
/// <b>format</b> change: every log any earlier build wrote has to keep reading, and a log that
/// stopped reading would take the world with it.
/// <para>
/// So the assertion is byte equality against a reference encoder built the old way, right here in
/// the test, rather than a round-trip through this codec's own reader. A round-trip passes happily
/// when the writer and the reader are wrong in the same direction — which is exactly the mistake
/// available when one person changes both.
/// </para>
/// </summary>
public class LogPayloadFormatTests
{
    [Fact]
    public void A_payload_is_byte_identical_to_the_previous_encoder()
    {
        foreach (var request in Requests())
            Assert.Equal(ReferenceEncode(7UL, request), Encode(7UL, request));
    }

    [Fact]
    public void A_payload_round_trips_through_the_reader()
    {
        var request = new CommitRequest(
            new Timestamp(1_700_000_000_000_000),
            Identity.Hash("caller"),
            "Move",
            new byte[] { 9, 8, 7 },
            [
                new RowOp(RowOpKind.Insert, new TableId(3), Key(1), new byte[] { 1, 2, 3 }),
                new RowOp(RowOpKind.Delete, new TableId(3), Key(2), ReadOnlyMemory<byte>.Empty),
            ],
            [new EventRecord("Moved", 1, new byte[] { 4, 5 })]);

        var payload = Encode(11UL, request);
        var record = LogRecordCodec.ReadPayload(payload, payload.Length + 8);

        Assert.Equal(11UL, record.Lsn);
        Assert.Equal("Move", record.ReducerName);
        Assert.Equal(new byte[] { 9, 8, 7 }, record.Arguments.ToArray());
        Assert.Equal(2, record.WriteSet.Count);
        Assert.Equal(RowOpKind.Delete, record.WriteSet[1].Kind);
        Assert.True(record.WriteSet[1].Row.IsEmpty);
        Assert.Equal("Moved", Assert.Single(record.Events).EventType);
    }

    [Fact]
    public void A_payload_larger_than_the_first_rented_buffer_is_written_whole()
    {
        // The growth path. A rented buffer that runs out has to copy what it holds into a larger
        // one; getting that wrong truncates or duplicates a record, and neither shows up until a
        // recovery that matters.
        var big = new byte[64 * 1024];
        Random.Shared.NextBytes(big);
        var request = new CommitRequest(
            new Timestamp(3),
            Identity.Hash("bulk"),
            "Bulk",
            ReadOnlyMemory<byte>.Empty,
            [.. Enumerable.Range(0, 40).Select(i => new RowOp(RowOpKind.Insert, new TableId(1), Key(i), big))]);

        var payload = Encode(2UL, request);

        Assert.Equal(ReferenceEncode(2UL, request), payload);
        var record = LogRecordCodec.ReadPayload(payload, payload.Length + 8);
        Assert.Equal(40, record.WriteSet.Count);
        Assert.Equal(big, record.WriteSet[39].Row.ToArray());
    }

    [Fact]
    public void A_multibyte_reducer_name_is_length_prefixed_in_bytes_not_characters()
    {
        // The length prefix counts UTF-8 bytes. Encoding straight into the buffer rather than into
        // an intermediate array is where a character count could quietly creep in, and a name whose
        // two counts differ is the only input that would catch it.
        var request = new CommitRequest(
            new Timestamp(1),
            Identity.Hash("caller"),
            "Ωμέγα-移動",
            ReadOnlyMemory<byte>.Empty,
            []);

        var payload = Encode(1UL, request);

        Assert.Equal(ReferenceEncode(1UL, request), payload);
        Assert.Equal("Ωμέγα-移動", LogRecordCodec.ReadPayload(payload, payload.Length + 8).ReducerName);
    }

    private static IEnumerable<CommitRequest> Requests()
    {
        yield return new CommitRequest(new Timestamp(0), default, "Empty", ReadOnlyMemory<byte>.Empty, []);
        yield return new CommitRequest(
            new Timestamp(-1),
            Identity.Hash("negative-clock"),
            "Tick",
            new byte[] { 0xFF },
            [new RowOp(RowOpKind.Update, new TableId(uint.MaxValue), Key(0), new byte[] { 0 })]);
        yield return new CommitRequest(
            new Timestamp(long.MaxValue),
            Identity.Hash("caller"),
            "Mixed",
            new byte[] { 1, 2, 3, 4 },
            [
                new RowOp(RowOpKind.Insert, new TableId(1), Key(1), new byte[] { 10 }),
                new RowOp(RowOpKind.Delete, new TableId(2), Key(2), ReadOnlyMemory<byte>.Empty),
                new RowOp(RowOpKind.Update, new TableId(3), Key(3), new byte[] { 20, 30 }),
            ],
            [
                new EventRecord("A", 0, ReadOnlyMemory<byte>.Empty),
                new EventRecord("B.Nested", byte.MaxValue, new byte[] { 7 }),
            ]);
    }

    private static byte[] Encode(ulong lsn, in CommitRequest request)
    {
        var length = LogRecordCodec.WritePayload(lsn, request, out var buffer);
        var payload = buffer.AsSpan(0, length).ToArray();
        LogRecordCodec.Release(buffer);
        return payload;
    }

    /// <summary>
    /// The previous encoder, transcribed. It stays in the test rather than in the codec so that
    /// changing the codec cannot change what it is being compared against.
    /// </summary>
    private static byte[] ReferenceEncode(ulong lsn, in CommitRequest request)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(LogRecordCodec.RecordFormatVersion);
        writer.Write(lsn);
        writer.Write(request.Timestamp.UnixTimeMicroseconds);
        Span<byte> identity = stackalloc byte[Identity.Size];
        request.Caller.WriteTo(identity);
        writer.Write(identity);
        var name = Encoding.UTF8.GetBytes(request.ReducerName);
        writer.Write((ushort)name.Length);
        writer.Write(name);
        writer.Write(request.Arguments.Length);
        writer.Write(request.Arguments.Span);
        writer.Write(request.WriteSet.Count);
        foreach (var op in request.WriteSet)
        {
            writer.Write((byte)op.Kind);
            writer.Write(op.Table.Value);
            writer.Write((ushort)op.Key.Length);
            writer.Write(op.Key.Span);
            if (op.Kind != RowOpKind.Delete)
            {
                writer.Write(op.Row.Length);
                writer.Write(op.Row.Span);
            }
        }

        var events = request.Events ?? [];
        writer.Write(events.Count);
        foreach (var evt in events)
        {
            var typeName = Encoding.UTF8.GetBytes(evt.EventType);
            writer.Write((ushort)typeName.Length);
            writer.Write(typeName);
            writer.Write(evt.Depth);
            writer.Write(evt.Payload.Length);
            writer.Write(evt.Payload.Span);
        }

        writer.Flush();
        return stream.ToArray();
    }

    private static RowKey Key(int value)
    {
        Span<byte> buffer = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return new RowKey(buffer);
    }
}
