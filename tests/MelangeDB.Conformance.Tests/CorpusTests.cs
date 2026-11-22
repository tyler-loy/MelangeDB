using System.Text.Json.Nodes;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Conformance.Tests;

/// <summary>
/// Holds the committed decode corpus to the server that produced it. A client implementer reads
/// conformance/corpus.json and nothing else, so a corpus that drifted from the server would be a
/// contract that lies — these tests make that a build failure.
/// </summary>
public sealed class CorpusTests
{
    private const string Regenerate =
        "Regenerate with `dotnet run --project tools/MelangeDB.Conformance -c Release -- record` from the repo root, " +
        "and review the diff: a changed frame is a wire change every client must follow.";

    private static readonly MessagePackFrameSerializer Serializer = new();

    private static string Committed(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "conformance", name)).ReplaceLineEndings("\n");

    [Fact]
    public async Task The_committed_corpus_is_what_the_server_sends_today()
    {
        var recorded = CorpusRecorder.Render(await CorpusRecorder.RecordAsync(TestContext.Current.CancellationToken));
        Assert.True(recorded == Committed("corpus.json"), "conformance/corpus.json is stale. " + Regenerate);
    }

    /// <summary>
    /// Every frame's bytes decode to its <c>expect</c>, and re-encode to the same bytes: the
    /// server's encoding is the minimal one, which is what makes the corpus a byte-exact fixture
    /// rather than one acceptable encoding among several.
    /// </summary>
    [Fact]
    public void Every_corpus_frame_decodes_to_its_expectation_and_reencodes_identically()
    {
        var corpus = JsonNode.Parse(Committed("corpus.json"))!.AsObject();
        var sessions = corpus["sessions"]!.AsArray().ToDictionary(static s => (string)s!["name"]!, static s => s!.AsObject());
        var checkedFrames = 0;
        foreach (var session in sessions.Values)
        {
            var descriptors = new Dictionary<uint, WireDescriptor>();
            if (session["descriptorsFrom"] is { } from)
                Replay(sessions[(string)from!], descriptors, ref checkedFrames, check: false);
            Replay(session, descriptors, ref checkedFrames, check: true);
        }

        Assert.True(checkedFrames > 50, $"Only {checkedFrames} frames checked.");
    }

    [Fact]
    public void The_corpus_covers_every_server_to_client_frame_type()
    {
        var corpus = JsonNode.Parse(Committed("corpus.json"))!.AsObject();
        var seen = corpus["sessions"]!.AsArray()
            .SelectMany(static s => s!["steps"]!.AsArray())
            .SelectMany(static step => step!["receive"]!.AsArray())
            .Select(static f => (string)f!["expect"]!["type"]!)
            .ToHashSet();
        FrameType[] serverToClient =
        [
            FrameType.Welcome, FrameType.ReducerResult, FrameType.Unsubscribed, FrameType.SubscriptionApplied,
            FrameType.TransactionUpdate, FrameType.Error, FrameType.Ping, FrameType.Pong, FrameType.ResumeResult,
            FrameType.ReauthenticateResult, FrameType.ProducerResult,
        ];
        var missing = serverToClient.Select(static t => t.ToString()).Where(t => !seen.Contains(t)).ToList();
        Assert.True(missing.Count == 0, $"The corpus has no {string.Join(", ", missing)} frame.");
    }

    [Fact]
    public void The_committed_manifest_is_the_conformance_module()
    {
        Assert.True(
            MelangeDB.Generated.MelangeSchemaManifest.Json.ReplaceLineEndings("\n") == Committed("melange-schema.json"),
            "conformance/melange-schema.json is stale. Regenerate with `dotnet run --project tools/MelangeDB.Conformance -c Release -- schema`.");
    }

    private static void Replay(JsonObject session, Dictionary<uint, WireDescriptor> descriptors, ref int checkedFrames, bool check)
    {
        foreach (var step in session["steps"]!.AsArray())
        {
            foreach (var recorded in step!["receive"]!.AsArray())
            {
                var bytes = Convert.FromHexString((string)recorded!["hex"]!);
                var frame = Serializer.Deserialize(bytes);
                var decoded = CorpusJson.Frame(frame, descriptors);
                if (!check)
                    continue;
                Assert.True(
                    JsonNode.DeepEquals(decoded, recorded["expect"]),
                    $"Session '{session["name"]}': {recorded["hex"]} decodes as {decoded.ToJsonString()}, but the corpus expects {recorded["expect"]!.ToJsonString()}.");
                Assert.Equal(bytes, Serializer.Serialize(frame));
                checkedFrames++;
            }
        }
    }
}
