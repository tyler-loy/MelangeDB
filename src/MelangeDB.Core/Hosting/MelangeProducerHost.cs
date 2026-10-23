using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// The outcome of one producer call: the value the body returned (for a transport that renders
/// it some other way, such as JSON), its encoding (see <see cref="ProducerResultWriter"/>), and the
/// LSN the call's read view answered for.
/// </summary>
public sealed record ProducerResult(object? Value, ReadOnlyMemory<byte> Payload, ulong Lsn);

/// <summary>
/// Dispatches producer calls. The order is the contract (docs/design/producers.md): decode and
/// validate the arguments, rate limit and authorize a client call — all before anything is pinned —
/// then resolve the producer from a fresh DI scope, pin one read view, run the body under the call's
/// timeout, encode the result while the pin is still held, release it, and wait for the log to be
/// durable through the pinned LSN so an answer never reflects a commit a crash could take back.
/// </summary>
public sealed class MelangeProducerHost
{
    private readonly MelangeEngine _engine;
    private readonly ProducerRegistry _registry;
    private readonly IServiceScopeFactory _scopes;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly ReducerRateLimiter _rateLimiter;
    private readonly TimeProvider _time;
    private volatile bool _stopping;

    public MelangeProducerHost(
        MelangeEngine engine,
        ProducerRegistry registry,
        IServiceScopeFactory scopes,
        IOptionsMonitor<MelangeDbOptions> options,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _registry = registry;
        _scopes = scopes;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _rateLimiter = new ReducerRateLimiter(_time);
    }

    /// <summary>The registered producers.</summary>
    public IReadOnlyList<ProducerDescriptor> Producers => _registry.Producers;

    /// <summary>Producers with no authorization policy — listed beside reducers by the unpoliced report.</summary>
    public IReadOnlyList<string> UnpolicedProducers =>
        _registry.Producers.Where(static p => p.Policy is null).Select(static p => p.Name).ToArray();

    /// <summary>The descriptor for <paramref name="name"/>, or <see cref="UnknownProducerException"/>.</summary>
    public ProducerDescriptor Get(string name) =>
        _registry.TryGet(name, out var descriptor) ? descriptor : throw new UnknownProducerException(name);

    /// <summary>Encodes <paramref name="arguments"/> and calls a producer in-process: no rate limit, no policy.</summary>
    public Task<ProducerResult> CallAsync(string producerName, Identity caller, params object?[] arguments) =>
        CallAsync(producerName, caller, ConnectionId.None, ReducerArguments.Encode(arguments));

    /// <summary>
    /// Calls a producer from encoded arguments — the path a transport uses. A client-originated
    /// call (<paramref name="source"/>) is rate limited and authorized first. Throws
    /// <see cref="UnknownProducerException"/>, <see cref="ReducerArgumentException"/>,
    /// <see cref="RateLimitedException"/>, <see cref="ReducerDeniedException"/>,
    /// <see cref="ProducerTimeoutException"/>, <see cref="ProducerResultTooLargeException"/>, or
    /// whatever the body throws (a <see cref="RejectedException"/> is the body saying no).
    /// <paramref name="cancellationToken"/> is the caller's — a transport cancels it on disconnect.
    /// <paramref name="render"/> replaces the binary encoding — the HTTP endpoint renders JSON — and
    /// runs where the encoder would, while the pin is held, given the value and
    /// <c>Producers:MaxResponseBytes</c>.
    /// </summary>
    public async Task<ProducerResult> CallAsync(
        string producerName,
        Identity caller,
        ConnectionId connectionId,
        ReadOnlyMemory<byte> encodedArguments,
        ActivityContext parentContext = default,
        CallSource source = default,
        CallerClaims? claims = null,
        CancellationToken cancellationToken = default,
        Func<object?, int, ReadOnlyMemory<byte>>? render = null)
    {
        if (_stopping)
            throw new InvalidOperationException("MelangeDB is shutting down; no further producer calls are accepted.");

        var descriptor = Get(producerName);
        var options = _options.CurrentValue;
        var telemetry = _engine.Telemetry;
        using var activity = telemetry?.StartProducer(descriptor.Name, caller, parentContext);
        var started = Stopwatch.GetTimestamp();
        var outcome = "error";
        try
        {
            if (source.ClientOriginated && options.RateLimit.Enabled && !_rateLimiter.TryAcquire(caller, descriptor.Name, options.RateLimit))
            {
                telemetry?.RecordRateLimited(descriptor.Name);
                outcome = "rate_limited";
                throw new RateLimitedException(
                    $"Producer '{descriptor.Name}' is over its rate limit for this identity (RateLimit:*); the call did not run.");
            }

            var validation = options.Validation;
            var validator = new ReducerArgsReader(encodedArguments.Span, validation);
            descriptor.Validate(ref validator);

            using var scope = _scopes.CreateScope();
            if (source.ClientOriginated)
                CallAuthorization.Authorize(_engine, "Producer", descriptor.Name, descriptor.Policy, caller, source, options.Policies, scope.ServiceProvider);
            var instance = scope.ServiceProvider.GetRequiredService(descriptor.ProducerClass);

            var result = await RunPinnedAsync(descriptor, instance, caller, connectionId, encodedArguments, options, claims, render, cancellationToken).ConfigureAwait(false);
            outcome = "ok";
            return result;
        }
        catch (ReducerArgumentException)
        {
            outcome = "invalid_args";
            throw;
        }
        catch (ReducerDeniedException)
        {
            outcome = "denied";
            throw;
        }
        catch (ProducerTimeoutException)
        {
            outcome = "timeout";
            throw;
        }
        catch (ProducerResultTooLargeException)
        {
            outcome = "too_many_bytes";
            throw;
        }
        catch (RejectedException)
        {
            outcome = "rejected";
            throw;
        }
        finally
        {
            activity?.SetTag("melange.producer.outcome", outcome);
            telemetry?.RecordProducer(descriptor.Name, outcome, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
    }

    internal void SignalStopping() => _stopping = true;

    private async Task<ProducerResult> RunPinnedAsync(
        ProducerDescriptor descriptor,
        object instance,
        Identity caller,
        ConnectionId connectionId,
        ReadOnlyMemory<byte> encodedArguments,
        MelangeDbOptions options,
        CallerClaims? claims,
        Func<object?, int, ReadOnlyMemory<byte>>? render,
        CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(options.Producers.TimeoutSeconds);
        using var deadline = new CancellationTokenSource(timeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        ulong lsn;
        ReadOnlyMemory<byte> payload;
        object? value;
        var pinned = Stopwatch.GetTimestamp();
        var view = _engine.OpenReadView();
        try
        {
            lsn = view.Lsn;
            var context = new ProducerContext(
                caller,
                connectionId,
                Timestamp.FromDateTimeOffset(_time.GetUtcNow()),
                lsn,
                view,
                linked.Token,
                claims);

            MelangeEngine.InProducer.Value = true;
            try
            {
                value = await descriptor.Invoke(instance, context, encodedArguments, options.Validation)
                    .AsTask()
                    .WaitAsync(linked.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new ProducerTimeoutException(descriptor.Name, timeout);
            }
            finally
            {
                MelangeEngine.InProducer.Value = false;
            }

            // Encoded while the pin is held: a lazy query inside the result reads it now.
            if (render is not null)
            {
                payload = render(value, options.Producers.MaxResponseBytes);
            }
            else
            {
                var writer = new ProducerResultWriter(options.Producers.MaxResponseBytes);
                descriptor.Encode(writer, value);
                payload = writer.Written;
            }
        }
        finally
        {
            view.Dispose();
            _engine.Telemetry?.RecordProducerPin(descriptor.Name, Stopwatch.GetElapsedTime(pinned).TotalMilliseconds);
        }

        // A pinned view can include a commit whose fsync is still in flight. A reducer's caller is
        // answered only once its commit is durable; a producer's answer holds to the same rule.
        var log = _engine.Log;
        if (lsn > log.DurableLsn)
            await Task.Run(() => log.WaitDurable(lsn), CancellationToken.None).ConfigureAwait(false);
        return new ProducerResult(value, payload, lsn);
    }
}
