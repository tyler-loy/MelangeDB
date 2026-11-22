using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading.Channels;

namespace MelangeDB.Cluster;

/// <summary>
/// One websocket between a gateway and a shard node carrying many player sessions, each a
/// <see cref="MuxSocket"/> (road-to-0.4 phase 31). Per-player upstream sockets cost a local port
/// each, and every handoff closed one and opened another; at 1,200 players that held most of a
/// host's ephemeral ports in TIME_WAIT and made connects at swap time take seconds. Over a link,
/// opening a session is one message.
/// <para>
/// Wire format: one binary websocket message per envelope, <c>[kind:1][session:4 LE][payload]</c>.
/// <c>Open</c> carries the shard key (8 bytes LE) and is sent by the gateway, which numbers its
/// sessions. <c>Data</c> carries a flags byte (bit 0: end of message) and a chunk of at most
/// <see cref="MaxChunk"/> bytes. <c>Close</c> carries a status (2 bytes LE) and a UTF-8
/// description; <c>Abort</c> carries nothing and reads as a dropped connection. <c>Credit</c>
/// carries 4 bytes LE: payload bytes the receiver has consumed, which the sender may send again.
/// </para>
/// <para>
/// Flow control is per session and per direction: at most <see cref="Window"/> payload bytes in
/// flight. A slow reader therefore backs up only its own session's sender, never the link, so one
/// slow client cannot stall the others sharing it. Nothing here authenticates: a session proves its
/// identity the way a per-player socket did, with its own Hello.
/// </para>
/// </summary>
internal sealed class MuxLink : IAsyncDisposable
{
    /// <summary>Unacknowledged payload bytes one direction of one session may have in flight.</summary>
    public const int Window = 256 * 1024;

    /// <summary>The largest data chunk; a longer message is cut so one session cannot hold the link.</summary>
    public const int MaxChunk = 16 * 1024;

    internal enum Kind : byte
    {
        Open = 1,
        Data = 2,
        Close = 3,
        Abort = 4,
        Credit = 5,
    }

    private const int HeaderBytes = 5;

    private readonly WebSocket _socket;
    private readonly Func<MuxSocket, ulong, Task>? _accept;
    private readonly ConcurrentDictionary<uint, MuxSocket> _sessions = new();
    private readonly Channel<byte[]> _outbound = Channel.CreateUnbounded<byte[]>(
        new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _closed = new();
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _nextSession;
    private int _ended;
    private Task? _reader;
    private Task? _writer;

    private MuxLink(WebSocket socket, Func<MuxSocket, ulong, Task>? accept)
    {
        _socket = socket;
        _accept = accept;
    }

    /// <summary>The gateway's end: opens sessions, accepts none.</summary>
    public static MuxLink Client(WebSocket socket) => new(socket, accept: null);

    /// <summary>The node's end: <paramref name="accept"/> runs each session the gateway opens, for its shard.</summary>
    public static MuxLink Server(WebSocket socket, Func<MuxSocket, ulong, Task> accept) => new(socket, accept);

    public bool IsAlive => Volatile.Read(ref _ended) == 0 && _socket.State == WebSocketState.Open;

    /// <summary>Completes when the link has ended and every session on it has been dropped.</summary>
    public Task Completion => _completion.Task;

    /// <summary>Sessions currently open on this link.</summary>
    public int SessionCount => _sessions.Count;

    public void Start()
    {
        _reader = Task.Run(ReadLoopAsync);
        _writer = Task.Run(WriteLoopAsync);
    }

    /// <summary>
    /// Opens a session to <paramref name="shard"/> on the node. The node answers by running a
    /// connection on it, or by closing it (the node does not own the shard); either way the
    /// caller learns through the socket, as it would from a websocket handshake.
    /// </summary>
    public MuxSocket Open(ulong shard)
    {
        if (_accept is not null)
            throw new InvalidOperationException("The node's end of a link accepts sessions; the gateway opens them.");
        var id = unchecked((uint)Interlocked.Increment(ref _nextSession));
        var session = new MuxSocket(this, id);
        _sessions[id] = session;
        var open = Envelope(Kind.Open, id, 8);
        BinaryPrimitives.WriteUInt64LittleEndian(open.AsSpan(HeaderBytes), shard);
        if (!TryEnqueue(open))
            session.LinkLost();
        return session;
    }

    internal bool SendData(uint id, ReadOnlySpan<byte> chunk, bool endOfMessage)
    {
        var envelope = Envelope(Kind.Data, id, 1 + chunk.Length);
        envelope[HeaderBytes] = endOfMessage ? (byte)1 : (byte)0;
        chunk.CopyTo(envelope.AsSpan(HeaderBytes + 1));
        return TryEnqueue(envelope);
    }

    internal void SendClose(uint id, WebSocketCloseStatus status, string? description)
    {
        var text = System.Text.Encoding.UTF8.GetBytes(description ?? string.Empty);
        var envelope = Envelope(Kind.Close, id, 2 + text.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(envelope.AsSpan(HeaderBytes), (ushort)status);
        text.CopyTo(envelope.AsSpan(HeaderBytes + 2));
        TryEnqueue(envelope);
    }

    internal void SendAbort(uint id) => TryEnqueue(Envelope(Kind.Abort, id, 0));

    internal void SendCredit(uint id, int bytes)
    {
        var envelope = Envelope(Kind.Credit, id, 4);
        BinaryPrimitives.WriteInt32LittleEndian(envelope.AsSpan(HeaderBytes), bytes);
        TryEnqueue(envelope);
    }

    /// <summary>A session that is finished on both sides, or aborted, leaves the link's table.</summary>
    internal void Forget(uint id) => _sessions.TryRemove(id, out _);

    private static byte[] Envelope(Kind kind, uint id, int payloadBytes)
    {
        var envelope = new byte[HeaderBytes + payloadBytes];
        envelope[0] = (byte)kind;
        BinaryPrimitives.WriteUInt32LittleEndian(envelope.AsSpan(1), id);
        return envelope;
    }

    private bool TryEnqueue(byte[] envelope) => Volatile.Read(ref _ended) == 0 && _outbound.Writer.TryWrite(envelope);

    private async Task WriteLoopAsync()
    {
        var ct = _closed.Token;
        try
        {
            await foreach (var envelope in _outbound.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                await _socket.SendAsync(envelope, WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The link died (or is being disposed); the read loop or End() drops the sessions.
        }
        finally
        {
            End();
        }
    }

    private async Task ReadLoopAsync()
    {
        var ct = _closed.Token;
        var buffer = new byte[MaxChunk + HeaderBytes + 64];
        using var message = new MemoryStream();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                message.SetLength(0);
                WebSocketReceiveResult result;
                do
                {
                    result = await _socket.ReceiveAsync(buffer, ct).ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                        return;
                    message.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                Dispatch(message.GetBuffer().AsSpan(0, (int)message.Length));
            }
        }
        catch (Exception)
        {
            // The link died; every session on it ends below.
        }
        finally
        {
            End();
        }
    }

    private void Dispatch(ReadOnlySpan<byte> envelope)
    {
        if (envelope.Length < HeaderBytes)
            throw new InvalidDataException("A link envelope shorter than its header.");
        var kind = (Kind)envelope[0];
        var id = BinaryPrimitives.ReadUInt32LittleEndian(envelope[1..]);
        var payload = envelope[HeaderBytes..];
        switch (kind)
        {
            case Kind.Open when _accept is not null:
            {
                var shard = BinaryPrimitives.ReadUInt64LittleEndian(payload);
                var session = new MuxSocket(this, id);
                _sessions[id] = session;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await _accept(session, shard).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        session.Abort();
                    }
                });
                break;
            }

            case Kind.Data:
                if (_sessions.TryGetValue(id, out var target))
                    target.Deliver(payload[1..].ToArray(), endOfMessage: (payload[0] & 1) != 0);
                break;
            case Kind.Close:
                if (_sessions.TryGetValue(id, out var closing))
                {
                    closing.RemoteClosed(
                        (WebSocketCloseStatus)BinaryPrimitives.ReadUInt16LittleEndian(payload),
                        System.Text.Encoding.UTF8.GetString(payload[2..]));
                }

                break;
            case Kind.Abort:
                if (_sessions.TryRemove(id, out var aborted))
                    aborted.LinkLost();
                break;
            case Kind.Credit:
                if (_sessions.TryGetValue(id, out var credited))
                    credited.Credit(BinaryPrimitives.ReadInt32LittleEndian(payload));
                break;
            default:
                throw new InvalidDataException($"Unknown link envelope kind {(byte)kind}.");
        }
    }

    /// <summary>Ends the link once: every session on it reads as a dropped connection.</summary>
    private void End()
    {
        if (Interlocked.Exchange(ref _ended, 1) != 0)
            return;
        _closed.Cancel();
        _outbound.Writer.TryComplete();
        foreach (var id in _sessions.Keys)
        {
            if (_sessions.TryRemove(id, out var session))
                session.LinkLost();
        }

        _socket.Abort();
        _completion.TrySetResult();
    }

    public async ValueTask DisposeAsync()
    {
        End();
        foreach (var loop in new[] { _reader, _writer })
        {
            if (loop is null)
                continue;
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        _socket.Dispose();
        _closed.Dispose();
    }
}
