using System.Net.WebSockets;
using System.Threading.Channels;

namespace MelangeDB.Cluster;

/// <summary>
/// One player session carried on a <see cref="MuxLink"/>, presented as a <see cref="WebSocket"/>
/// so the code on either end (the gateway's <see cref="UpstreamSession"/>, the node's
/// <c>MelangeSocketConnection</c>) runs unchanged on top of it. It keeps a websocket's contract
/// where those callers rely on it: messages arrive whole and in order, possibly across several
/// receives; a close handshake ends in <see cref="WebSocketState.Closed"/>; an abort, or the
/// link's death, reads as <see cref="WebSocketError.ConnectionClosedPrematurely"/>.
/// <para>
/// Sends wait for credit: at most <see cref="MuxLink.Window"/> bytes unread by the peer. That is
/// what TCP did for a per-player socket, so a node's slow-consumer handling still sees a slow
/// client as a send that does not finish.
/// </para>
/// </summary>
internal sealed class MuxSocket : WebSocket
{
    private readonly record struct Inbound(byte[] Data, bool EndOfMessage, bool Close);

    private readonly MuxLink _link;
    private readonly uint _id;
    private readonly Lock _gate = new();
    private readonly Channel<Inbound> _inbound = Channel.CreateUnbounded<Inbound>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly TaskCompletionSource _closeReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private WebSocketState _state = WebSocketState.Open;
    private WebSocketCloseStatus? _closeStatus;
    private string? _closeDescription;

    // Receiving: the chunk being read from, and payload consumed but not yet credited back.
    private Inbound _current;
    private int _offset;
    private bool _haveCurrent;
    private int _unacknowledged;

    // Sending: payload the peer has room for, and a waiter when there is none.
    private int _credit = MuxLink.Window;
    private TaskCompletionSource? _creditWaiter;

    internal MuxSocket(MuxLink link, uint id)
    {
        _link = link;
        _id = id;
    }

    public override WebSocketCloseStatus? CloseStatus
    {
        get
        {
            lock (_gate)
            {
                return _closeStatus;
            }
        }
    }

    public override string? CloseStatusDescription
    {
        get
        {
            lock (_gate)
            {
                return _closeDescription;
            }
        }
    }

    public override WebSocketState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    public override string? SubProtocol => null;

    public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
    {
        if (!_haveCurrent)
        {
            if (State == WebSocketState.Aborted)
                throw Dropped();
            try
            {
                _current = await _inbound.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException)
            {
                throw Dropped();
            }

            _offset = 0;
            _haveCurrent = true;
        }

        if (_current.Close)
        {
            _haveCurrent = false;
            WebSocketCloseStatus? status;
            string? description;
            lock (_gate)
            {
                _state = _state == WebSocketState.CloseSent ? WebSocketState.Closed : WebSocketState.CloseReceived;
                status = _closeStatus;
                description = _closeDescription;
            }

            _closeReceived.TrySetResult();
            if (State == WebSocketState.Closed)
                _link.Forget(_id);
            return new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, status, description);
        }

        var count = Math.Min(buffer.Count, _current.Data.Length - _offset);
        _current.Data.AsSpan(_offset, count).CopyTo(buffer.AsSpan());
        _offset += count;
        var finished = _offset == _current.Data.Length;
        var endOfMessage = finished && _current.EndOfMessage;
        if (finished)
            _haveCurrent = false;

        // Credit back in quarters of the window: often enough that a reader keeping up never
        // starves its sender, rarely enough that credit envelopes are a small share of the link.
        _unacknowledged += count;
        if (_unacknowledged >= MuxLink.Window / 4)
        {
            _link.SendCredit(_id, _unacknowledged);
            _unacknowledged = 0;
        }

        return new WebSocketReceiveResult(count, WebSocketMessageType.Binary, endOfMessage);
    }

    public override async Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var offset = 0;
            do
            {
                var length = Math.Min(MuxLink.MaxChunk, buffer.Count - offset);
                await TakeCreditAsync(length, cancellationToken).ConfigureAwait(false);
                var last = offset + length == buffer.Count;
                if (!_link.SendData(_id, buffer.AsSpan(offset, length), last && endOfMessage))
                    throw Dropped();
                offset += length;
            }
            while (offset < buffer.Count);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task TakeCreditAsync(int length, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task wait;
            lock (_gate)
            {
                if (_state is WebSocketState.Aborted or WebSocketState.Closed or WebSocketState.CloseSent)
                    throw _state == WebSocketState.Aborted ? Dropped() : new WebSocketException(WebSocketError.InvalidState);
                if (_credit >= length)
                {
                    _credit -= length;
                    return;
                }

                _creditWaiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                wait = _creditWaiter.Task;
            }

            await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public override async Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
    {
        bool forget;
        lock (_gate)
        {
            if (_state is WebSocketState.Aborted)
                throw Dropped();
            if (_state is WebSocketState.CloseSent or WebSocketState.Closed)
                return;
            _state = _state == WebSocketState.CloseReceived ? WebSocketState.Closed : WebSocketState.CloseSent;
            forget = _state == WebSocketState.Closed;
            _closeStatus ??= closeStatus;
            _closeDescription ??= statusDescription;
            WakeSender();
        }

        _link.SendClose(_id, closeStatus, statusDescription);
        if (forget)
            _link.Forget(_id);
        await Task.CompletedTask.ConfigureAwait(false);
    }

    public override async Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
    {
        await CloseOutputAsync(closeStatus, statusDescription, cancellationToken).ConfigureAwait(false);

        // A websocket's close completes when the peer's close arrives. Drain to it, as a real
        // close does, so a caller awaiting CloseAsync sees Closed afterwards.
        var drain = new byte[1024];
        while (State != WebSocketState.Closed && State != WebSocketState.Aborted)
            await ReceiveAsync(drain, cancellationToken).ConfigureAwait(false);
    }

    public override void Abort()
    {
        lock (_gate)
        {
            if (_state is WebSocketState.Aborted or WebSocketState.Closed)
                return;
            _state = WebSocketState.Aborted;
            WakeSender();
        }

        _inbound.Writer.TryComplete();
        _link.SendAbort(_id);
        _link.Forget(_id);
    }

    public override void Dispose()
    {
        Abort();
        _sendLock.Dispose();
    }

    /// <summary>The link carried a chunk of this session's next message.</summary>
    internal void Deliver(byte[] data, bool endOfMessage) =>
        _inbound.Writer.TryWrite(new Inbound(data, endOfMessage, Close: false));

    /// <summary>The peer closed its side of the session.</summary>
    internal void RemoteClosed(WebSocketCloseStatus status, string description)
    {
        lock (_gate)
        {
            _closeStatus ??= status;
            _closeDescription ??= description;
        }

        _inbound.Writer.TryWrite(new Inbound([], EndOfMessage: true, Close: true));
    }

    /// <summary>The peer consumed <paramref name="bytes"/> of what this side sent.</summary>
    internal void Credit(int bytes)
    {
        lock (_gate)
        {
            _credit += bytes;
            WakeSender();
        }
    }

    /// <summary>The link died or the peer aborted: the session reads as a dropped connection.</summary>
    internal void LinkLost()
    {
        lock (_gate)
        {
            if (_state is WebSocketState.Closed)
                return;
            _state = WebSocketState.Aborted;
            WakeSender();
        }

        _inbound.Writer.TryComplete();
        _closeReceived.TrySetResult();
    }

    /// <summary>Releases a sender waiting on credit, to take it or to find the session ended. Call under <c>_gate</c>.</summary>
    private void WakeSender()
    {
        _creditWaiter?.TrySetResult();
        _creditWaiter = null;
    }

    private static WebSocketException Dropped() => new(WebSocketError.ConnectionClosedPrematurely);
}
