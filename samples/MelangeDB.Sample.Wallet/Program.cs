using MelangeDB;
using MelangeDB.Core;
using MelangeDB.Sample;
using MelangeDB.Sample.Wallet;
using MelangeDB.Server;
using MelangeDB.Storage.Postgres;

// The archive tier's worked example. Needs Postgres: an archive table without it refuses to start,
// because its rows would be lost the first time the log truncated. Point ConnectionStrings:Melange
// at a database, and the applier creates the schema (AutoMigrate is on in appsettings.json).
//
// A player's history — hot and archived — is the History producer (WalletProducers.cs): a client
// calls conn.Producers.HistoryAsync(), or POST /melange/producers/History with a JSON array of
// arguments. Clients never read an archive table directly; the producer decides what they see.
var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<WalletOptions>(builder.Configuration.GetSection("Wallet"));
builder.Services.AddDevJwtAuthentication();
builder.Services.AddMelangeDb(melange => melange
    .AddPostgres(builder.Configuration.GetConnectionString("Melange")!)
    .AddTablesFrom(typeof(Wallet).Assembly)
    .AddReducersFrom(typeof(WalletReducers).Assembly)
    .AddProducersFrom(typeof(WalletProducers).Assembly));

var app = builder.Build();
app.UseWebSockets();
app.MapMelangeSocket();

app.Run();
