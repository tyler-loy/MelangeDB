using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The tests' identity provider — the same pattern as the transport suite: a dev issuer with a
/// symmetric key. The IdP is the gate; only the gateway (and the hub) ever validate these, since
/// shard nodes trust hub-minted assertions instead.
/// </summary>
internal static class TestTokens
{
    public const string Issuer = "melange-cluster-tests";

    public const string DefaultSubject = "cluster-tests";

    public static readonly SymmetricSecurityKey Key =
        new(System.Text.Encoding.UTF8.GetBytes("melange-cluster-tests-signing-key-0123456789"));

    public static string Default { get; } = For(DefaultSubject);

    public static string For(string subject, DateTimeOffset? expires = null, string? role = null)
    {
        var claims = new Dictionary<string, object> { ["sub"] = subject };
        if (role is not null)
            claims["role"] = role;
        return new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = Issuer,
            Claims = claims,
            Expires = (expires ?? DateTimeOffset.UtcNow.AddHours(1)).UtcDateTime,
            SigningCredentials = new SigningCredentials(Key, SecurityAlgorithms.HmacSha256),
        });
    }

    public static Identity IdentityOf(string subject) => Identity.FromIssuerSubject(Issuer, subject);
}
