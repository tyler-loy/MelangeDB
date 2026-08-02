using System.Security.Claims;
using MelangeDB.Core;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace MelangeDB.Server;

/// <summary>
/// One authenticated credential: the identity it resolves to and the token facts sessions need.
/// <see cref="TokenExpiresAt"/> drives the re-auth grace window; <see cref="IsGuest"/> is the
/// <c>Auth:GuestRole</c> claim — a guest is an ordinary identity policies may treat differently.
/// <see cref="IsSqlOwner"/> is the <c>Sql:OwnerRole</c> claim — what authorizes a caller when
/// ad-hoc SQL runs in owner mode. <see cref="IsBulkOwner"/> is the <c>Bulk:OwnerRole</c> claim —
/// what authorizes a caller on the bulk ingestion endpoint.
/// </summary>
internal sealed record AuthResult(
    Identity Identity,
    bool IsGuest,
    DateTimeOffset TokenExpiresAt,
    bool IsSqlOwner = false,
    bool IsBulkOwner = false,
    bool IsInternal = false,
    bool FiresLifecycle = true)
{
    /// <summary>A validation failure, carrying a reason safe to send to the client.</summary>
    public static AuthFailure Failure(string reason) => new(reason);
}

internal sealed record AuthFailure(string Reason);

/// <summary>
/// Validates bearer tokens against the host's own ASP.NET Core JWT bearer configuration — the
/// scheme named by <c>Auth:Scheme</c>. MelangeDB owns no issuer, audience, or key settings and
/// mints nothing: <b>the IdP is the gate</b>, and this class only asks the host's configuration
/// whether a presented token is genuine. Identity is
/// <see cref="Identity.FromIssuerSubject"/> over the validated issuer and subject.
/// </summary>
internal sealed class MelangeAuthenticator
{
    private static readonly string[] RoleClaimTypes = [ClaimTypes.Role, "role", "roles"];

    private readonly IServiceProvider _services;
    private readonly Func<AuthOptions> _options;
    private readonly Func<SqlOptions> _sqlOptions;
    private readonly Func<BulkOptions> _bulkOptions;
    private readonly Func<ClusterOptions>? _clusterOptions;
    private readonly TimeProvider _time;

    public MelangeAuthenticator(
        IServiceProvider services,
        Func<AuthOptions> options,
        Func<SqlOptions>? sqlOptions = null,
        Func<BulkOptions>? bulkOptions = null,
        Func<ClusterOptions>? clusterOptions = null,
        TimeProvider? time = null)
    {
        _services = services;
        _options = options;
        _sqlOptions = sqlOptions ?? (static () => new SqlOptions());
        _bulkOptions = bulkOptions ?? (static () => new BulkOptions());
        _clusterOptions = clusterOptions;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>
    /// Fails fast at map time when the configured scheme does not exist — every connection would
    /// be rejected at runtime otherwise, which is the same outcome discovered much later.
    /// </summary>
    public void EnsureSchemeConfigured()
    {
        var scheme = _options().Scheme;
        var schemes = _services.GetService<IAuthenticationSchemeProvider>();
        if (schemes is null || schemes.GetSchemeAsync(scheme).GetAwaiter().GetResult() is null)
        {
            throw new InvalidOperationException(
                $"MelangeDB validates connection tokens against the host's '{scheme}' authentication scheme " +
                "(Auth:Scheme), but no such scheme is registered. Configure it — e.g. " +
                "services.AddAuthentication().AddJwtBearer(...) with your IdP's authority or a dev signing key — " +
                "the IdP is the gate, and MelangeDB deliberately has no token settings of its own.");
        }
    }

    /// <summary>Validates one bearer token; returns an <see cref="AuthResult"/> or an <see cref="AuthFailure"/>.</summary>
    public async ValueTask<object> ValidateAsync(string? token)
    {
        if (string.IsNullOrEmpty(token))
            return AuthResult.Failure("No bearer token was presented; every connection presents a valid token.");

        // A hub-minted internal identity assertion, not a JWT: the gateway authenticated the
        // client against the IdP once and vouches for it with the cluster secret. Only accepted
        // when this node is clustered and holds the secret.
        if (InternalIdentityAssertion.IsAssertion(token))
        {
            var cluster = _clusterOptions?.Invoke();
            if (cluster is not { Role: not ClusterRole.None, Secret.Length: > 0 })
                return AuthResult.Failure("This node accepts no internal identity assertions.");
            var asserted = InternalIdentityAssertion.Validate(cluster.Secret, token, _time.GetUtcNow(), out var reason);
            if (asserted is not { } valid)
                return AuthResult.Failure(reason ?? "The assertion is invalid.");
            return new AuthResult(
                valid.Identity, valid.IsGuest, valid.ExpiresAt, valid.IsSqlOwner, valid.IsBulkOwner,
                IsInternal: true, FiresLifecycle: valid.FiresLifecycle);
        }

        var options = _services.GetRequiredService<IOptionsMonitor<JwtBearerOptions>>().Get(_options().Scheme);
        var parameters = options.TokenValidationParameters.Clone();
        if (options.ConfigurationManager is { } metadata)
        {
            try
            {
                var configuration = await metadata.GetConfigurationAsync(CancellationToken.None).ConfigureAwait(false);
                parameters.ValidIssuer ??= configuration.Issuer;
                var keys = parameters.IssuerSigningKeys?.ToList() ?? [];
                keys.AddRange(configuration.SigningKeys);
                parameters.IssuerSigningKeys = keys;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                return AuthResult.Failure($"The authority's metadata could not be retrieved: {exception.Message}");
            }
        }

        var handler = options.TokenHandlers.OfType<JsonWebTokenHandler>().FirstOrDefault() ?? new JsonWebTokenHandler();
        var result = await handler.ValidateTokenAsync(token, parameters).ConfigureAwait(false);
        if (!result.IsValid)
            return AuthResult.Failure($"Token validation failed: {result.Exception?.Message ?? "invalid token"}");

        var jwt = (JsonWebToken)result.SecurityToken;
        var issuer = jwt.Issuer;
        var subject = jwt.Subject;
        if (string.IsNullOrEmpty(issuer) || string.IsNullOrEmpty(subject))
            return AuthResult.Failure("The token carries no issuer or no subject; identity is a hash of both.");

        var expires = jwt.ValidTo == default
            ? DateTimeOffset.MaxValue
            : new DateTimeOffset(DateTime.SpecifyKind(jwt.ValidTo, DateTimeKind.Utc));
        return new AuthResult(
            Identity.FromIssuerSubject(issuer, subject),
            HasRole(result.ClaimsIdentity, _options().GuestRole),
            expires,
            HasRole(result.ClaimsIdentity, _sqlOptions().OwnerRole),
            HasRole(result.ClaimsIdentity, _bulkOptions().OwnerRole));
    }

    private static bool HasRole(ClaimsIdentity? claims, string role)
    {
        if (string.IsNullOrEmpty(role) || claims is null)
            return false;
        foreach (var claim in claims.Claims)
        {
            if (claim.Value != role)
                continue;
            if (claim.Type == claims.RoleClaimType || RoleClaimTypes.Contains(claim.Type, StringComparer.Ordinal))
                return true;
        }

        return false;
    }
}
