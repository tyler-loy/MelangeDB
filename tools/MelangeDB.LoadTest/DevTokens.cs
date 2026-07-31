using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace MelangeDB.LoadTest;

/// <summary>
/// The tool's identity provider — the sample client's hand-rolled HS256 issuer, shared by both
/// halves: the serve side validates against it, the drive side mints one token per simulated
/// player. A stand-in for a real IdP; the load path being measured does not care who signs.
/// </summary>
public static class DevTokens
{
    public const string Issuer = "melange-loadtest";

    public const string SigningKey = "melange-loadtest-signing-key-not-for-production-0123456789";

    public static Identity IdentityOf(string subject) => Identity.FromIssuerSubject(Issuer, subject);

    public static string For(string subject)
    {
        var now = DateTimeOffset.UtcNow;
        var header = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new { alg = "HS256", typ = "JWT" }));
        var payload = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new
        {
            iss = Issuer,
            sub = subject,
            iat = now.ToUnixTimeSeconds(),
            exp = now.AddHours(12).ToUnixTimeSeconds(),
        }));
        var signature = Base64Url(HMACSHA256.HashData(
            Encoding.UTF8.GetBytes(SigningKey),
            Encoding.ASCII.GetBytes($"{header}.{payload}")));
        return $"{header}.{payload}.{signature}";

        static string Base64Url(byte[] bytes) =>
            Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }
}
