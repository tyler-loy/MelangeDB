using System.Diagnostics;
using System.Net;
using MelangeDB.Client;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A real Kestrel host on loopback with two listeners — one HTTP/1.1, one HTTP/2 cleartext —
/// because HTTP/2 extended CONNECT and heartbeat aborts need real sockets, and the in-memory test
/// server silently falls back to HTTP/1.1, which is exactly the bug being guarded against.
/// Restartable on the same ports so epoch and resume tests can kill and revive the server.
/// </summary>
internal sealed class TransportTestHost : IAsyncDisposable
{
    private readonly Dictionary<string, string?> _settings;
    private readonly string _root;
    private readonly Action<IServiceCollection>? _services;
    private WebApplication? _app;
    private int _http1Port;
    private int _http2Port;

    private TransportTestHost(string root, Dictionary<string, string?> settings, ManualTimeProvider? time, Action<IServiceCollection>? services)
    {
        _root = root;
        _settings = settings;
        Time = time;
        _services = services;
    }

    public ManualTimeProvider? Time { get; }

    public Uri WsUri => new($"ws://127.0.0.1:{_http1Port}/melange");

    public Uri WsUriHttp2 => new($"ws://127.0.0.1:{_http2Port}/melange");

    public Uri HttpBase => new($"http://127.0.0.1:{_http1Port}");

    public MelangeEngine Engine => _app!.Services.GetRequiredService<MelangeEngine>();

    public MelangeReducerHost Reducers => _app!.Services.GetRequiredService<MelangeReducerHost>();

    /// <summary>The identity of <see cref="TestTokens.Default"/> — server-side writes and the default client share it.</summary>
    public static Identity Caller { get; } = TestTokens.IdentityOf(TestTokens.DefaultSubject);

    public MelangeSessions Sessions => _app!.Services.GetRequiredService<MelangeSessions>();

    /// <summary>Lifecycle-fire record shared across restarts, so pairing asserts survive a bounce.</summary>
    public SessionEvents SessionEvents { get; } = new();

    public IServiceProvider Services => _app!.Services;

    public static async Task<TransportTestHost> StartAsync(
        Dictionary<string, string?>? settings = null,
        bool manualTime = false,
        Action<IServiceCollection>? services = null)
    {
        var root = Directory.CreateTempSubdirectory("melange-transport-").FullName;
        var host = new TransportTestHost(root, settings ?? [], manualTime ? new ManualTimeProvider() : null, services);
        await host.StartAppAsync();
        return host;
    }

    /// <summary>Calls a reducer server-side — the writes a client is meant to observe as deltas.</summary>
    public ulong Call(string reducer, params object?[] args) => Reducers.Call(reducer, Caller, args);

    public MelangeClient CreateClient(Action<MelangeClientOptions>? configure = null)
    {
        var options = new MelangeClientOptions { Uri = WsUri, Token = TestTokens.Default };
        configure?.Invoke(options);
        return new MelangeClient(options);
    }

    /// <summary>An HTTP client for the plain endpoints, authenticated as <paramref name="token"/> (default: the shared test identity).</summary>
    public HttpClient CreateHttp(string? token = "default")
    {
        var http = new HttpClient { BaseAddress = HttpBase };
        if (token is not null)
        {
            http.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token == "default" ? TestTokens.Default : token);
        }

        return http;
    }

    /// <summary>
    /// Kills the server (no goodbyes to clients) and starts a fresh one on the same ports.
    /// <paramref name="freshLog"/> wipes the data directory — a different log incarnation, so a
    /// new epoch; otherwise the log and its epoch survive.
    /// </summary>
    public async Task RestartAsync(bool freshLog = false)
    {
        await _app!.StopAsync();
        await _app.DisposeAsync();
        _app = null;
        if (freshLog)
            Directory.Delete(_root, recursive: true);
        await StartAppAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }

        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>Polls until <paramref name="condition"/> holds, failing loudly on timeout.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string what, int timeoutSeconds = 15)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(timeoutSeconds), $"Timed out waiting for: {what}");
            await Task.Yield();
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    private async Task StartAppAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Loopback, _http1Port, listen => listen.Protocols = HttpProtocols.Http1);
            kestrel.Listen(IPAddress.Loopback, _http2Port, listen => listen.Protocols = HttpProtocols.Http2);
        });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(_root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(_root, "hot"),
        });
        builder.Configuration.AddInMemoryCollection(_settings);
        if (Time is not null)
            builder.Services.AddSingleton<TimeProvider>(Time);

        // The tests' own IdP: MelangeDB validates against the host's JWT bearer scheme, so the
        // harness registers one over the test signing key. Under manual time, token lifetime is
        // judged by the hand-cranked clock too — otherwise expiry tests would race the wall clock.
        builder.Services.AddAuthentication().AddJwtBearer(jwt =>
        {
            jwt.TokenValidationParameters = new Microsoft.IdentityModel.Tokens.TokenValidationParameters
            {
                ValidIssuers = [TestTokens.Issuer, TestTokens.SecondIssuer],
                ValidateAudience = false,
                IssuerSigningKey = TestTokens.Key,
                RoleClaimType = "role",
            };
            if (Time is { } time)
            {
                jwt.TokenValidationParameters.LifetimeValidator =
                    (_, expires, _, _) => expires is null || expires > time.GetUtcNow().UtcDateTime;
            }
        });
        builder.Services.AddSingleton<MelangeSessions>();
        builder.Services.AddSingleton(SessionEvents);
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Chunk).Assembly)
            .AddReducersFrom(typeof(TransportReducers).Assembly));
        _services?.Invoke(builder.Services);

        var app = builder.Build();
        app.UseWebSockets();
        app.MapMelangeSocket();
        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.ToList();
        _http1Port = new Uri(addresses[0]).Port;
        _http2Port = new Uri(addresses[1]).Port;
        _app = app;
    }
}
