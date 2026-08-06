using System.Text;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>An event published straight from the engine tests, with no host in sight.</summary>
public sealed record CorePing(string Tag, int Value);

/// <summary>
/// The commit-record format change: version 2 appends a domain-event section, and version-1
/// records — every log written before this phase — read back unchanged with no events. Plus the
/// engine half of the outbox: events ride the record, publish-only transactions commit, and a
/// rolled-back publish leaves zero trace.
/// </summary>
public class EventRecordFormatTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void A_published_event_lands_in_the_commit_record()
    {
        _harness.Invoke("Ping", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" });
            ctx.Publish(new CorePing("hello", 42));
        });

        var record = _harness.Engine.Log.ReadFrom(1).Single();
        Assert.Equal((ushort)2, record.FormatVersion);
        Assert.Single(record.WriteSet);
        var evt = Assert.Single(record.Events);
        Assert.Equal(typeof(CorePing).FullName, evt.EventType);
        Assert.Equal(0, evt.Depth);
        var payload = Encoding.UTF8.GetString(evt.Payload.Span);
        Assert.Contains("hello", payload);
        Assert.Contains("42", payload);
    }

    [Fact]
    public void A_publish_only_transaction_commits_a_record_with_an_empty_write_set()
    {
        _harness.Invoke("PingOnly", ctx => ctx.Publish(new CorePing("no-rows", 1)));

        Assert.Equal(1UL, _harness.Engine.Log.HeadLsn);
        var record = _harness.Engine.Log.ReadFrom(1).Single();
        Assert.Empty(record.WriteSet);
        Assert.Single(record.Events);
    }

    [Fact]
    public void A_rolled_back_publish_leaves_zero_trace()
    {
        Assert.Throws<RejectedException>(() => _harness.Invoke("PingAndThrow", ctx =>
        {
            ctx.Publish(new CorePing("ghost", 0));
            throw new RejectedException("aborted");
        }));

        Assert.Equal(0UL, _harness.Engine.Log.HeadLsn);
        Assert.Empty(_harness.Engine.Log.ReadFrom(1));
    }

    [Fact]
    public void Events_survive_a_restart_and_replay_from_the_log()
    {
        _harness.Invoke("Ping", ctx => ctx.Publish(new CorePing("durable", 7)));
        _harness.Restart();

        var evt = Assert.Single(_harness.Engine.Log.ReadFrom(1).Single().Events);
        Assert.Equal(typeof(CorePing).FullName, evt.EventType);
        Assert.Contains("durable", Encoding.UTF8.GetString(evt.Payload.Span));
    }

    [Fact]
    public void A_version_1_payload_reads_back_with_no_events()
    {
        // A phase-01..05 record, byte for byte: no event section existed. The new codec must read
        // it unchanged — extending the record payload broke no existing log.
        var caller = Identity.Hash("v1-writer");
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write((ushort)1);
        writer.Write(9UL);
        writer.Write(1_234_567L);
        Span<byte> identity = stackalloc byte[Identity.Size];
        caller.WriteTo(identity);
        writer.Write(identity);
        var name = Encoding.UTF8.GetBytes("OldReducer");
        writer.Write((ushort)name.Length);
        writer.Write(name);
        writer.Write(0); // no arguments
        writer.Write(0); // no row ops
        writer.Flush();
        var payload = stream.ToArray();

        var record = LogRecordCodec.ReadPayload(payload, payload.Length + 8);
        Assert.Equal((ushort)1, record.FormatVersion);
        Assert.Equal(9UL, record.Lsn);
        Assert.Equal(caller, record.Caller);
        Assert.Equal("OldReducer", record.ReducerName);
        Assert.Empty(record.WriteSet);
        Assert.Empty(record.Events);
    }

    [Fact]
    public void Version_2_payloads_round_trip_events_through_the_codec()
    {
        var request = new CommitRequest(
            new Timestamp(555),
            Identity.Hash("emitter"),
            "Emit",
            ReadOnlyMemory<byte>.Empty,
            [],
            [new EventRecord("Some.Event", 2, new byte[] { 1, 2, 3 }), new EventRecord("Other.Event", 0, Array.Empty<byte>())]);

        var length = LogRecordCodec.WritePayload(4UL, request, out var buffer);
        var payload = buffer.AsSpan(0, length).ToArray();
        LogRecordCodec.Release(buffer);
        var record = LogRecordCodec.ReadPayload(payload, payload.Length + 8);

        Assert.Equal((ushort)2, record.FormatVersion);
        Assert.Equal(2, record.Events.Count);
        Assert.Equal("Some.Event", record.Events[0].EventType);
        Assert.Equal(2, record.Events[0].Depth);
        Assert.Equal(new byte[] { 1, 2, 3 }, record.Events[0].Payload.ToArray());
        Assert.Equal("Other.Event", record.Events[1].EventType);
        Assert.Equal(0, record.Events[1].Depth);
        Assert.True(record.Events[1].Payload.IsEmpty);
    }
}
