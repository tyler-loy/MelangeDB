using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The tests' identity provider: a dev issuer with a symmetric key, minting whatever JWT a test
/// needs — subjects, roles, second issuers, chosen expiries. The IdP is the gate, so the tests
/// bring their own gatekeeper.
/// </summary>
internal static class TestTokens
{
    public const string Issuer = "melange-tests";

    /// <summary>A second token source, for proving issuer+subject hashing keeps sources apart.</summary>
    public const string SecondIssuer = "melange-tests-other-idp";

    public const string DefaultSubject = "transport-tests";

    public static readonly SymmetricSecurityKey Key =
        new(System.Text.Encoding.UTF8.GetBytes("melange-transport-tests-signing-key-0123456789"));

    /// <summary>The token most tests connect with; its identity is <see cref="TransportTestHost.Caller"/>.</summary>
    public static string Default { get; } = For(DefaultSubject);

    public static string For(
        string subject,
        string issuer = Issuer,
        DateTimeOffset? expires = null,
        string? role = null)
    {
        var claims = new Dictionary<string, object> { ["sub"] = subject };
        if (role is not null)
            claims["role"] = role;
        return new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Claims = claims,
            Expires = (expires ?? DateTimeOffset.UtcNow.AddHours(1)).UtcDateTime,
            SigningCredentials = new SigningCredentials(Key, SecurityAlgorithms.HmacSha256),
        });
    }

    public static Identity IdentityOf(string subject, string issuer = Issuer) =>
        Identity.FromIssuerSubject(issuer, subject);
}
