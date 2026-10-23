using MelangeDB;
using MelangeDB.Core;
using MelangeDB.Sample;
using MelangeDB.Sample.Wallet;
using MelangeDB.Server;
using MelangeDB.Storage.Postgres;

// The archive tier's worked example. Needs Postgres: an archive table without it refuses to start,
// because its rows would be lost the first time the log truncated. Point ConnectionStrings:Melange
// at a database, and the applier creates the schema (AutoMigrate is on in appsettings.json).
var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<WalletOptions>(builder.Configuration.GetSection("Wallet"));
builder.Services.AddDevJwtAuthentication();
builder.Services.AddMelangeDb(melange => melange
    .AddPostgres(builder.Configuration.GetConnectionString("Melange")!)
    .AddTablesFrom(typeof(Wallet).Assembly)
    .AddReducersFrom(typeof(WalletReducers).Assembly));

var app = builder.Build();
app.UseWebSockets();
app.MapMelangeSocket();

// Archived history, newest first, read outside any reducer. A page is Postgres plus the log tail
// it has not applied yet, so an entry the sweep just moved never goes missing in between.
app.MapGet("/wallet/{owner}/archive", async (string owner, string? after, IArchiveReader archive, CancellationToken ct) =>
{
    var page = await archive.ReadAsync<WalletEntryArchive>(
        new ArchiveQuery
        {
            Column = nameof(WalletEntryArchive.Owner),
            Equal = new Identity(Convert.FromHexString(owner)),
            Descending = true,
            After = after,
            Limit = 50,
        },
        ct);
    return Results.Ok(new
    {
        entries = page.Rows.Select(static e => new { e.Key, e.Delta, e.Reason, at = e.At.ToDateTimeOffset() }),
        next = page.Next,
        page.Stale,
    });
});

app.Run();
