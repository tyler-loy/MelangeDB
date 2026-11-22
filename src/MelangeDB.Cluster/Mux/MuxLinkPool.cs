using System.Net.WebSockets;

namespace MelangeDB.Cluster;

/// <summary>
/// The gateway's links to shard nodes (<c>Cluster:GatewayUpstream = Shared</c>):
/// <c>Cluster:GatewayLinksPerNode</c> of them per node address, each new session taking the next in
/// turn. One link has one writer and one reader, so a node's few hundred sessions through a single
/// link queue behind each other; several spread them (phase 31). A link is connected on first use
/// and replaced when it dies. Concurrent callers share one connect attempt.
/// </summary>
internal sealed class MuxLinkPool : IAsyncDisposable
{
    private readonly Func<string, Uri> _linkUri;
    private readonly Func<int> _linksPerNode;
    private readonly Lock _gate = new();
    private readonly Dictionary<(string Address, int Slot), Task<MuxLink>> _links = [];
    private int _nextSlot;
    private bool _disposed;

    public MuxLinkPool(Func<string, Uri> linkUri, Func<int>? linksPerNode = null)
    {
        _linkUri = linkUri;
        _linksPerNode = linksPerNode ?? (static () => 1);
    }

    /// <summary>Live links, for tests and diagnostics.</summary>
    internal int LiveLinks
    {
        get
        {
            lock (_gate)
            {
                return _links.Values.Count(static t => t.IsCompletedSuccessfully && t.Result.IsAlive);
            }
        }
    }

    /// <summary>Opens a session to <paramref name="shard"/> on the node at <paramref name="publicAddress"/>.</summary>
    public async Task<WebSocket> OpenAsync(string publicAddress, ShardKey shard, CancellationToken ct)
    {
        var link = await LinkAsync(publicAddress, ct).ConfigureAwait(false);
        return link.Open(shard.Value);
    }

    private async Task<MuxLink> LinkAsync(string publicAddress, CancellationToken ct)
    {
        var slot = (int)((uint)Interlocked.Increment(ref _nextSlot) % (uint)Math.Clamp(_linksPerNode(), 1, 64));
        var key = (publicAddress, slot);
        Task<MuxLink> pending;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_links.TryGetValue(key, out pending!)
                || pending.IsFaulted
                || pending.IsCanceled
                || (pending.IsCompletedSuccessfully && !pending.Result.IsAlive))
            {
                // The connect runs on its own token: callers come and go, and the first caller's
                // cancellation must not fail the link for the ones waiting with it.
                pending = ConnectAsync(publicAddress);
                _links[key] = pending;
            }
        }

        return await pending.WaitAsync(ct).ConfigureAwait(false);
    }

    private async Task<MuxLink> ConnectAsync(string publicAddress)
    {
        var socket = new ClientWebSocket();
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await socket.ConnectAsync(_linkUri(publicAddress), timeout.Token).ConfigureAwait(false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        var link = MuxLink.Client(socket);
        link.Start();
        return link;
    }

    public async ValueTask DisposeAsync()
    {
        Task<MuxLink>[] links;
        lock (_gate)
        {
            _disposed = true;
            links = [.. _links.Values];
            _links.Clear();
        }

        foreach (var pending in links)
        {
            try
            {
                await (await pending.ConfigureAwait(false)).DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // A link that never connected has nothing to close.
            }
        }
    }
}
