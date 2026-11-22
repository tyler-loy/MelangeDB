using System.Net;
using MelangeDB.Core;
using MelangeDB.Server;
using MelangeDB.Testing;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace MelangeDB.Conformance;

/// <summary>
/// The server a conformance run talks to: the conformance module on a real Kestrel host, over a
/// throwaway data directory, on a <see cref="ManualClock"/> so nothing time-driven happens unless
/// the run moves the clock. It is its own identity provider (a symmetric dev key), because a
/// client under test needs tokens and a run must not depend on anything outside the process.
/// </summary>
public sealed class ConformanceServer : IAsyncDisposable
{
    /// <summary>The issuer every conformance token carries.</summary>
    public const string Issuer = "melange-conformance";

    /// <summary>The subject of <see cref="Token"/>, the identity a corpus session runs as.</summary>
    public const string Subject = "conformance-client";

    /// <summary>
    /// Small on purpose: the every-kind initial set must span several chunks, so the corpus holds
    /// a chunk with a descriptor and chunks without one.
    /// </summary>
    public const int InitialSetChunkBytes = 200;

    /// <summary>Kept long so only an explicit clock move ever sends a heartbeat.</summary>
    public const int HeartbeatIntervalMs = 60_000;

    private static readonly SymmetricSecurityKey Key =
        new(System.Text.Encoding.UTF8.GetBytes("melange-conformance-signing-key-0123456789"));

    private readonly string _root;
    private readonly WebApplication _app;

    private ConformanceServer(string root, WebApplication app, ManualClock clock)
    {
        _root = root;
        _app = app;
        Clock = clock;
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        HttpBase = new Uri(address);
        SocketUri = new UriBuilder(HttpBase) { Scheme = "ws", Path = "/melange" }.Uri;
    }

    /// <summary>The clock the server reads; heartbeats and token lifetimes move only with it.</summary>
    public ManualClock Clock { get; }

    public Uri SocketUri { get; }

    public Uri HttpBase { get; }

    public MelangeEngine Engine => _app.Services.GetRequiredService<MelangeEngine>();

    /// <summary>The identity <see cref="Token"/> authenticates as.</summary>
    public static Identity Caller { get; } = Identity.FromIssuerSubject(Issuer, Subject);

    /// <summary>A token for <see cref="Subject"/>.</summary>
    public string Token => TokenFor(Subject);

    /// <summary>A token for <paramref name="subject"/>, valid for a day of the server's clock.</summary>
    public string TokenFor(string subject) => new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Issuer = Issuer,
        Claims = new Dictionary<string, object> { ["sub"] = subject },
        IssuedAt = Clock.GetUtcNow().UtcDateTime,
        NotBefore = Clock.GetUtcNow().UtcDateTime,
        Expires = Clock.GetUtcNow().AddDays(1).UtcDateTime,
        SigningCredentials = new SigningCredentials(Key, SecurityAlgorithms.HmacSha256),
    });

    public static async Task<ConformanceServer> StartAsync(CancellationToken cancellationToken = default)
    {
        var root = Directory.CreateTempSubdirectory("melange-conformance-").FullName;
        var clock = new ManualClock();
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http1));
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(root, "hot"),
            ["MelangeDb:Transport:MaxInitialSetChunkBytes"] = InitialSetChunkBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Transport:HeartbeatIntervalMs"] = HeartbeatIntervalMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = (HeartbeatIntervalMs * 3).ToString(System.Globalization.CultureInfo.InvariantCulture),
        });
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddAuthentication().AddJwtBearer(jwt =>
        {
            jwt.TokenValidationParameters = new TokenValidationParameters
            {
                ValidIssuer = Issuer,
                ValidateAudience = false,
                IssuerSigningKey = Key,
                LifetimeValidator = (notBefore, expires, _, _) =>
                    (notBefore is null || notBefore <= clock.GetUtcNow().UtcDateTime)
                    && (expires is null || expires > clock.GetUtcNow().UtcDateTime),
            };
        });
        builder.Services.AddSingleton<IColumnPolicy<Secret>, OwnerSeesHidden>();
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Sample).Assembly)
            .AddReducersFrom(typeof(ConformanceReducers).Assembly)
            .AddProducersFrom(typeof(ConformanceProducers).Assembly));

        var app = builder.Build();
        app.UseWebSockets();
        app.MapMelangeSocket();
        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        return new ConformanceServer(root, app, clock);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
