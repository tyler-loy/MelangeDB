using MelangeDB.Conformance.Adapter.CSharp;

// The C# client's conformance adapter: the adapter protocol on stdin/stdout. Run by the suite
// (`MelangeDB.Conformance run --adapter "<this program>"`), never by hand.
Console.InputEncoding = System.Text.Encoding.UTF8;
var output = new StreamWriter(Console.OpenStandardOutput(), new System.Text.UTF8Encoding(false)) { AutoFlush = false };
await CSharpAdapter.RunAsync(Console.In, output);
return 0;
