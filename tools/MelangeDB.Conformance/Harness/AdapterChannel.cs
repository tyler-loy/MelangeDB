using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace MelangeDB.Conformance.Harness;

/// <summary>
/// The suite's end of the adapter protocol: newline-delimited JSON in both directions. Requests
/// carry an <c>id</c> and an <c>op</c>; the adapter answers each with the same <c>id</c> and
/// <c>ok</c>, and may interleave <c>event</c> lines at any time. Events and responses share one
/// ordered stream, which is what lets a <c>sync</c> response stand as a barrier: every event the
/// adapter raised before answering is already in <see cref="Events"/>.
/// </summary>
public sealed class AdapterChannel : IAsyncDisposable
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    private readonly TextReader _fromAdapter;
    private readonly TextWriter _toAdapter;
    private readonly ConcurrentDictionary<int, TaskCompletionSource<JsonObject>> _pending = new();
    private readonly List<JsonObject> _events = [];
    private readonly Lock _eventsLock = new();
    private readonly Task _readLoop;
    private readonly IAsyncDisposable? _owner;
    private int _nextId;
    private Exception? _broken;

    public AdapterChannel(TextReader fromAdapter, TextWriter toAdapter, IAsyncDisposable? owner = null)
    {
        _fromAdapter = fromAdapter;
        _toAdapter = toAdapter;
        _owner = owner;
        _readLoop = Task.Run(ReadLoopAsync);
    }

    /// <summary>Lines the adapter wrote that were not JSON, kept for the failure report.</summary>
    public ConcurrentQueue<string> Noise { get; } = new();

    /// <summary>Sends one request and awaits its response, whether <c>ok</c> or not.</summary>
    public async Task<JsonObject> RequestAsync(string op, JsonObject? arguments = null, TimeSpan? timeout = null)
    {
        if (_broken is { } broken)
            throw new InvalidOperationException("The adapter stream is closed.", broken);

        var id = Interlocked.Increment(ref _nextId);
        var request = arguments?.DeepClone().AsObject() ?? [];
        request.Insert(0, "id", id);
        request.Insert(1, "op", op);
        var pending = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = pending;
        await _toAdapter.WriteLineAsync(request.ToJsonString()).ConfigureAwait(false);
        await _toAdapter.FlushAsync().ConfigureAwait(false);
        try
        {
            return await pending.Task.WaitAsync(timeout ?? DefaultTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"The adapter did not answer '{op}' within {(timeout ?? DefaultTimeout).TotalSeconds}s.");
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    /// <summary>Sends a request that must succeed, returning its response.</summary>
    public async Task<JsonObject> MustAsync(string op, JsonObject? arguments = null, TimeSpan? timeout = null)
    {
        var response = await RequestAsync(op, arguments, timeout).ConfigureAwait(false);
        if (response["ok"]?.GetValue<bool>() != true)
            throw new ConformanceFailure($"'{op}' failed: {response["error"]?.ToJsonString() ?? response.ToJsonString()}");
        return response;
    }

    /// <summary>Removes and returns every event received so far, in arrival order.</summary>
    public List<JsonObject> TakeEvents()
    {
        lock (_eventsLock)
        {
            var taken = new List<JsonObject>(_events);
            _events.Clear();
            return taken;
        }
    }

    /// <summary>Waits until the received events satisfy <paramref name="done"/>, without removing them.</summary>
    public async Task<bool> WaitForEventsAsync(Func<IReadOnlyList<JsonObject>, bool> done, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            lock (_eventsLock)
            {
                if (done(_events))
                    return true;
            }

            await Task.Delay(10).ConfigureAwait(false);
        }

        lock (_eventsLock)
        {
            return done(_events);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _toAdapter.Dispose();
        if (_owner is not null)
            await _owner.DisposeAsync().ConfigureAwait(false);
        try
        {
            await _readLoop.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
        }
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (await _fromAdapter.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                if (line.Length == 0)
                    continue;
                JsonObject message;
                try
                {
                    message = JsonNode.Parse(line)?.AsObject() ?? throw new JsonException("null");
                }
                catch (Exception exception) when (exception is JsonException or InvalidOperationException)
                {
                    Noise.Enqueue(line);
                    continue;
                }

                if (message["event"] is not null)
                {
                    lock (_eventsLock)
                    {
                        _events.Add(message);
                    }
                }
                else if (message["id"]?.GetValue<int>() is { } id && _pending.TryGetValue(id, out var pending))
                {
                    pending.TrySetResult(message);
                }
                else
                {
                    Noise.Enqueue(line);
                }
            }

            _broken = new EndOfStreamException("The adapter closed its output.");
        }
        catch (Exception exception)
        {
            _broken = exception;
        }

        foreach (var pending in _pending.Values)
            pending.TrySetException(_broken!);
    }
}

/// <summary>A conformance obligation the client under test did not meet.</summary>
public sealed class ConformanceFailure(string message) : Exception(message);
