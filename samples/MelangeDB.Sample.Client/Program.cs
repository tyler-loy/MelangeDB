using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MelangeDB.Client;
using MelangeDB.Types;

// Connects to the sample worker (dotnet run in samples/MelangeDB.Sample.Worker first), subscribes
// to the Visitor table through the generated typed bindings, calls the generated Greet stub, and
// watches its own row arrive as a typed live delta — no column-name strings, no reducer-name
// strings; a renamed column is a build error here, not a runtime null. The bindings come from
// ../MelangeDB.Sample.Worker/melange-schema.json (see docs/CLIENT-BINDINGS.md); the token is
// minted against the worker's dev issuer — a stand-in for your IdP; MelangeDB itself mints no
// identities. The FileTokenStore persists it across runs, the same mechanism that keeps a guest
// identity from being lost with the process.
var uri = new Uri(args.Length > 0 ? args[0] : "ws://localhost:5310/melange");
await using var client = new MelangeClient(new MelangeClientOptions
{
    Uri = uri,
    Token = MintDevToken("console-visitor"),
    TokenStore = new FileTokenStore(Path.Combine(AppContext.BaseDirectory, "melange-token.txt")),
});
await client.ConnectAsync();
var conn = new MelangeConnection(client);
Console.WriteLine($"Connected over {client.NegotiatedHttpProtocol}; log epoch {client.LogEpochId:N}; schema {conn.SchemaHash[..12]}….");

conn.Db.Visitor.OnInsert += v => Console.WriteLine($"  + visitor #{v.Id}: {v.Name} at {v.VisitedAt}{(v.GreetedExcitedly ? "!!!" : "")}");
conn.Db.Visitor.OnUpdate += (_, v) => Console.WriteLine($"  ~ visitor #{v.Id}: {v.Name}");
conn.Db.Visitor.OnDelete += v => Console.WriteLine($"  - visitor #{v.Id}");
await conn.Db.Visitor.SubscribeAllAsync();
Console.WriteLine($"Subscribed: {conn.Db.Visitor.Count} visitor(s) in the initial set.");

var lsn = await conn.Reducers.GreetAsync("ConsoleVisitor");
Console.WriteLine($"Greet committed at LSN {lsn}; watching live deltas. Press Enter to exit.");
Console.ReadLine();

// A hand-rolled HS256 JWT against the sample worker's dev issuer, so this project needs no
// token packages. Your real client gets its token from your IdP's login flow instead.
static string MintDevToken(string subject)
{
    const string issuer = "melange-sample-dev";
    const string signingKey = "melange-sample-dev-signing-key-not-for-production";
    var now = DateTimeOffset.UtcNow;
    var header = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new { alg = "HS256", typ = "JWT" }));
    var payload = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new
    {
        iss = issuer,
        sub = subject,
        iat = now.ToUnixTimeSeconds(),
        exp = now.AddHours(1).ToUnixTimeSeconds(),
    }));
    var signature = Base64Url(HMACSHA256.HashData(
        Encoding.UTF8.GetBytes(signingKey),
        Encoding.ASCII.GetBytes($"{header}.{payload}")));
    return $"{header}.{payload}.{signature}";

    static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
