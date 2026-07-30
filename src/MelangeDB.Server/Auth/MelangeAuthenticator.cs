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
/// </summary>
internal sealed record AuthResult(Identity Identity, bool IsGuest, DateTimeOffset TokenExpiresAt)
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

    public MelangeAuthenticator(IServiceProvider services, Func<AuthOptions> options)
    {
        _services = services;
        _options = options;
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
        return new AuthResult(Identity.FromIssuerSubject(issuer, subject), IsGuest(result.ClaimsIdentity), expires);
    }

    private bool IsGuest(ClaimsIdentity? claims)
    {
        var guestRole = _options().GuestRole;
        if (string.IsNullOrEmpty(guestRole) || claims is null)
            return false;
        foreach (var claim in claims.Claims)
        {
            if (claim.Value != guestRole)
                continue;
            if (claim.Type == claims.RoleClaimType || RoleClaimTypes.Contains(claim.Type, StringComparer.Ordinal))
                return true;
        }

        return false;
    }
}
