using System.Net.WebSockets;

namespace MelangeDB.Cluster;

/// <summary>
/// The gateway's links to shard nodes, one per node address (<c>Cluster:GatewayUpstream = Shared</c>).
/// A link is connected on first use and replaced when it dies; every player session to that node
/// rides it. Concurrent callers share one connect attempt.
/// </summary>
internal sealed class MuxLinkPool : IAsyncDisposable
{
    private readonly Func<string, Uri> _linkUri;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, Task<MuxLink>> _links = new(StringComparer.OrdinalIgnoreCase);
    private bool _disposed;

    public MuxLinkPool(Func<string, Uri> linkUri) => _linkUri = linkUri;

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
        Task<MuxLink> pending;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_links.TryGetValue(publicAddress, out pending!)
                || pending.IsFaulted
                || pending.IsCanceled
                || (pending.IsCompletedSuccessfully && !pending.Result.IsAlive))
            {
                // The connect runs on its own token: callers come and go, and the first caller's
                // cancellation must not fail the link for the ones waiting with it.
                pending = ConnectAsync(publicAddress);
                _links[publicAddress] = pending;
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
