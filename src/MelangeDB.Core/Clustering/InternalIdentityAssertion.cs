using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// The signed internal identity assertion the hub mints for the gateway's upstream sessions: a
/// shard node trusts "this connection acts as identity X" because the assertion is HMAC-signed
/// with the cluster secret — the client authenticated once, at the gateway, against the IdP, and
/// re-validating its JWT on every shard node would be wasteful and impossible for hub-issued
/// guest identities. The trust boundary this creates is stated in docs/SECURITY.md: any holder of
/// the cluster secret can assert any identity.
/// </summary>
public static class InternalIdentityAssertion
{
    /// <summary>The token prefix marking an assertion; anything else is an ordinary bearer JWT.</summary>
    public const string Prefix = "mliassert1.";

    private sealed record Payload(string I, bool G, long E, bool O, bool L);

    /// <summary>
    /// Mints an assertion for one identity, valid until <paramref name="expiresAt"/>.
    /// <paramref name="firesLifecycle"/> marks the one upstream session per client that represents
    /// the client's real session — the hub attachment — so ClientConnected/ClientDisconnected fire
    /// exactly once per client, on the hub, and never for shard attachments.
    /// </summary>
    public static string Mint(
        string secret,
        Identity identity,
        bool isGuest,
        bool isSqlOwner,
        DateTimeOffset expiresAt,
        bool firesLifecycle = false)
    {
        ArgumentException.ThrowIfNullOrEmpty(secret);
        var payload = JsonSerializer.SerializeToUtf8Bytes(
            new Payload(identity.ToString(), isGuest, expiresAt.ToUnixTimeSeconds(), isSqlOwner, firesLifecycle));
        var signature = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload);
        return Prefix + Convert.ToBase64String(payload) + "." + Convert.ToBase64String(signature);
    }

    /// <summary>Whether a token is shaped like an assertion (which says nothing about validity).</summary>
    public static bool IsAssertion(string? token) =>
        token is not null && token.StartsWith(Prefix, StringComparison.Ordinal);

    /// <summary>
    /// Validates an assertion: signature first (constant-time), expiry second. Returns null with
    /// a reason on any failure — a tampered or expired assertion must read as "not authenticated",
    /// never as a different identity.
    /// </summary>
    public static (Identity Identity, bool IsGuest, bool IsSqlOwner, DateTimeOffset ExpiresAt, bool FiresLifecycle)? Validate(
        string secret,
        string token,
        DateTimeOffset now,
        out string? failure)
    {
        failure = null;
        if (string.IsNullOrEmpty(secret))
        {
            failure = "This node has no cluster secret configured; internal assertions are not accepted.";
            return null;
        }

        if (!IsAssertion(token))
        {
            failure = "Not an internal identity assertion.";
            return null;
        }

        var parts = token[Prefix.Length..].Split('.');
        byte[] payload;
        byte[] signature;
        try
        {
            if (parts.Length != 2)
                throw new FormatException();
            payload = Convert.FromBase64String(parts[0]);
            signature = Convert.FromBase64String(parts[1]);
        }
        catch (FormatException)
        {
            failure = "The assertion is malformed.";
            return null;
        }

        var expected = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload);
        if (!CryptographicOperations.FixedTimeEquals(expected, signature))
        {
            failure = "The assertion signature does not verify against this node's cluster secret.";
            return null;
        }

        Payload? parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<Payload>(payload);
        }
        catch (JsonException)
        {
            parsed = null;
        }

        if (parsed is null || parsed.I.Length != Identity.Size * 2)
        {
            failure = "The assertion payload is malformed.";
            return null;
        }

        var expiresAt = DateTimeOffset.FromUnixTimeSeconds(parsed.E);
        if (now > expiresAt)
        {
            failure = "The assertion has expired.";
            return null;
        }

        return (new Identity(Convert.FromHexString(parsed.I)), parsed.G, parsed.O, expiresAt, parsed.L);
    }
}
