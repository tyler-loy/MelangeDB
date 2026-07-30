using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace MelangeDB.Sample;

/// <summary>
/// A dev-only stand-in for an identity provider: one symmetric key, one issuer, tokens minted on
/// request. MelangeDB deliberately mints no identities — <b>the IdP is the gate</b> — so even the
/// sample authenticates properly; in production this whole file is replaced by your real IdP's
/// authority configuration (or <c>dotnet user-jwts</c> for local dev).
/// </summary>
public static class DevIdentity
{
    public const string Issuer = "melange-sample-dev";

    /// <summary>Shared with the sample console client. A dev constant, never a production pattern.</summary>
    public const string SigningKey = "melange-sample-dev-signing-key-not-for-production";

    /// <summary>Registers the JWT bearer scheme MelangeDB validates connection tokens against.</summary>
    public static IServiceCollection AddDevJwtAuthentication(this IServiceCollection services)
    {
        services.AddAuthentication().AddJwtBearer(options =>
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidIssuer = Issuer,
                ValidateAudience = false,
                IssuerSigningKey = new SymmetricSecurityKey(System.Text.Encoding.UTF8.GetBytes(SigningKey)),
                RoleClaimType = "role",
            });
        return services;
    }

    /// <summary>Mints a dev token; the sample's equivalent of an IdP issuing one.</summary>
    public static string MintToken(string subject, TimeSpan? lifetime = null, string? role = null)
    {
        var claims = new Dictionary<string, object> { ["sub"] = subject };
        if (role is not null)
            claims["role"] = role;
        return new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = Issuer,
            Claims = claims,
            Expires = DateTime.UtcNow.Add(lifetime ?? TimeSpan.FromHours(1)),
            SigningCredentials = new SigningCredentials(
                new SymmetricSecurityKey(System.Text.Encoding.UTF8.GetBytes(SigningKey)),
                SecurityAlgorithms.HmacSha256),
        });
    }
}
