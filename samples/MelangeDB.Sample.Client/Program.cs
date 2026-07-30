using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MelangeDB.Client;

// Connects to the sample worker (dotnet run in samples/MelangeDB.Sample.Worker first), subscribes
// to the Visitor table, calls the Greet reducer, and watches its own row arrive as a live delta.
// The token is minted against the worker's dev issuer — a stand-in for your IdP; MelangeDB itself
// mints no identities. The FileTokenStore persists it across runs, the same mechanism that keeps
// a guest identity from being lost with the process.
var uri = new Uri(args.Length > 0 ? args[0] : "ws://localhost:5310/melange");
await using var client = new MelangeClient(new MelangeClientOptions
{
    Uri = uri,
    Token = MintDevToken("console-visitor"),
    TokenStore = new FileTokenStore(Path.Combine(AppContext.BaseDirectory, "melange-token.txt")),
});
await client.ConnectAsync();
Console.WriteLine($"Connected over {client.NegotiatedHttpProtocol}; log epoch {client.LogEpochId:N}.");

var visitors = await client.SubscribeAsync("SELECT * FROM Visitor");
Console.WriteLine($"Subscribed: {visitors.Count} visitor(s) in the initial set (anchor LSN {visitors.AnchorLsn}).");
visitors.OnInsert += row => Console.WriteLine($"  + visitor #{row.Columns["Id"]}: {row.Columns["Name"]}");
visitors.OnUpdate += (_, row) => Console.WriteLine($"  ~ visitor #{row.Columns["Id"]}: {row.Columns["Name"]}");
visitors.OnDelete += row => Console.WriteLine($"  - visitor #{row.Columns["Id"]}");

var lsn = await client.CallReducerAsync("Greet", ["ConsoleVisitor"]);
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
