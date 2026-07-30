using MelangeDB.Client;

// Connects to the sample worker (dotnet run in samples/MelangeDB.Sample.Worker first), subscribes
// to the Visitor table, calls the Greet reducer, and watches its own row arrive as a live delta.
var uri = new Uri(args.Length > 0 ? args[0] : "ws://localhost:5310/melange");
await using var client = new MelangeClient(new MelangeClientOptions { Uri = uri });
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
