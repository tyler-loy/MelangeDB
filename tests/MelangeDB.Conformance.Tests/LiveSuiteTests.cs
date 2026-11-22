using System.IO.Pipelines;
using System.Text;
using System.Text.Json.Nodes;
using MelangeDB.Conformance.Adapter.CSharp;
using MelangeDB.Conformance.Harness;
using Xunit;

namespace MelangeDB.Conformance.Tests;

/// <summary>
/// The C# client defines conformance, so it must pass the live suite (docs/CLIENT-CONFORMANCE.md).
/// Its adapter runs in-process over pipes, speaking exactly the protocol another language's
/// adapter speaks over stdin and stdout.
/// </summary>
public sealed class LiveSuiteTests
{
    [Fact]
    public async Task The_csharp_client_passes_the_live_suite()
    {
        var corpus = JsonNode.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "conformance", "corpus.json")))!.AsObject();
        var toAdapter = new Pipe();
        var fromAdapter = new Pipe();
        var utf8 = new UTF8Encoding(false);
        var adapter = CSharpAdapter.RunAsync(
            new StreamReader(toAdapter.Reader.AsStream(), utf8),
            new StreamWriter(fromAdapter.Writer.AsStream(), utf8),
            TestContext.Current.CancellationToken);

        IReadOnlyList<ScenarioResult> results;
        await using (var channel = new AdapterChannel(
            new StreamReader(fromAdapter.Reader.AsStream(), utf8),
            new StreamWriter(toAdapter.Writer.AsStream(), utf8)))
        {
            results = await ConformanceSuite.RunAsync(channel, corpus, cancellationToken: TestContext.Current.CancellationToken);
        }

        await adapter.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        var failures = results.Where(static r => !r.Passed).Select(static r => $"{r.Name} ({r.Obligations}): {r.Failure}").ToList();
        Assert.True(failures.Count == 0, string.Join("\n\n", failures));
        Assert.NotEmpty(results);
    }
}
