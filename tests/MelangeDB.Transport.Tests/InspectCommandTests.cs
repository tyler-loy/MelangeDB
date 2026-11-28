using System.Text.Json;
using MelangeDB.Cli;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// <c>melange inspect</c> (road-to-0.3 phase 21) against a host that wrote its log through the
/// generated dispatcher, so the arguments are encoded the way a real client's are. The verbs read
/// the live data directory while the host runs, and an archive of it after.
/// </summary>
public class InspectCommandTests
{
    private static (int Code, string Output, string Error) Inspect(params string[] args)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        var code = InspectCommand.Run(args, output, error);
        return (code, output.ToString(), error.ToString());
    }

    private static async Task<TransportTestHost> HostWithHistoryAsync()
    {
        var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 10L, new byte[] { 1, 2, 3 });
        host.Call("SetChunk", 2L, 20L, new byte[] { 4 });
        host.Call("SetChunk", 1L, 11L, new byte[] { 5, 6 });
        host.Call("DeleteChunk", 2L);
        return host;
    }

    private static string LogDir(TransportTestHost host) => Path.Combine(host.Root, "log");

    [Fact]
    public async Task Log_lists_a_live_directorys_commits_after_its_facts_and_filters_them()
    {
        await using var host = await HostWithHistoryAsync();
        var (code, output, error) = Inspect("log", LogDir(host));
        Assert.True(code == 0, error);
        var factsEnd = output.IndexOf("REDUCER", StringComparison.Ordinal);
        Assert.True(output.IndexOf("epoch ", StringComparison.Ordinal) is var epoch && epoch >= 0 && epoch < factsEnd, output);
        Assert.Contains("read without a lock", output);
        Assert.Contains("SetChunk", output);
        Assert.Contains("DeleteChunk", output);

        var deletes = Inspect("log", LogDir(host), "--reducer", "DeleteChunk");
        Assert.Equal(0, deletes.Code);
        Assert.Contains("1 record shown.", deletes.Output);
        Assert.DoesNotContain("SetChunk", deletes.Output[deletes.Output.IndexOf("REDUCER", StringComparison.Ordinal)..]);

        var byTable = Inspect("log", LogDir(host), "--table", "Chunk", "--json");
        Assert.Equal(0, byTable.Code);
        using var json = JsonDocument.Parse(byTable.Output);
        var records = json.RootElement.GetProperty("records");
        Assert.Equal(4, records.GetArrayLength());
        Assert.Equal("directory", json.RootElement.GetProperty("source").GetProperty("kind").GetString());
        var first = records[0].GetProperty("lsn").GetUInt64();
        var window = Inspect("log", LogDir(host), "--from", (first + 1).ToString(), "--to", (first + 2).ToString(), "--json");
        using var windowed = JsonDocument.Parse(window.Output);
        Assert.Equal([first + 1, first + 2], windowed.RootElement.GetProperty("records").EnumerateArray().Select(static r => r.GetProperty("lsn").GetUInt64()));

        var nobody = Inspect("log", LogDir(host), "--caller", "00");
        Assert.Contains("0 records shown.", nobody.Output);
        Assert.Contains("no shape in this source names a table 'Nope'", Inspect("log", LogDir(host), "--table", "Nope").Output);
    }

    [Fact]
    public async Task Record_shows_one_commit_in_full_with_named_arguments_from_a_manifest()
    {
        await using var host = await HostWithHistoryAsync();
        var lsn = LsnOf(host, "SetChunk", nth: 3);
        var manifest = Path.Combine(AppContext.BaseDirectory, "melange-schema.json");

        var (code, output, error) = Inspect("record", LogDir(host), "--at-lsn", lsn.ToString(), "--manifest", manifest);
        Assert.True(code == 0, error);
        Assert.Contains($"LSN {lsn} ", output);
        Assert.Contains("reducer   SetChunk", output);
        Assert.Contains("[0] id = 1", output);
        Assert.Contains("[1] x = 11", output);
        Assert.Contains("[2] data = 0x0506", output);
        Assert.Contains("update Chunk  Id = 1", output);
        Assert.Matches(@"X\s+11", output);

        var json = Inspect("record", LogDir(host), "--at-lsn", lsn.ToString(), "--json");
        using var document = JsonDocument.Parse(json.Output);
        var record = document.RootElement.GetProperty("record");
        Assert.Equal("SetChunk", record.GetProperty("reducer").GetString());
        var arguments = record.GetProperty("arguments");
        Assert.Equal("int64", arguments[0].GetProperty("type").GetString());
        Assert.Equal("1", arguments[0].GetProperty("value").GetString());
        Assert.False(arguments[0].TryGetProperty("name", out _));
        var write = record.GetProperty("writes")[0];
        Assert.Equal("update", write.GetProperty("op").GetString());
        Assert.Equal("Chunk", write.GetProperty("table").GetString());
        Assert.Equal("Id", write.GetProperty("key").GetProperty("column").GetString());
        Assert.Equal("11", write.GetProperty("row").GetProperty("columns").GetProperty("X").GetString());
        Assert.Equal("0506", write.GetProperty("row").GetProperty("columns").GetProperty("Data").GetString());

        var delete = Inspect("record", LogDir(host), "--at-lsn", LsnOf(host, "DeleteChunk").ToString());
        Assert.Contains("delete Chunk  Id = 2", delete.Output);
    }

    [Fact]
    public async Task An_archive_reads_the_same_and_an_lsn_it_cannot_hold_is_explained_not_returned_empty()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 10L, new byte[] { 1 });
        var archive = Path.Combine(host.Root, "world.mbak");
        await host.RestartAsync(whileStopped: () => MelangeBackup.Create(LogDir(host), archive));

        var (code, output, error) = Inspect("log", archive);
        Assert.True(code == 0, error);
        Assert.Contains("archive, engine node", output);
        Assert.Contains("SetChunk", output);
        Assert.DoesNotContain("read without a lock", output);

        var past = Inspect("record", archive, "--at-lsn", "999999");
        Assert.Equal(1, past.Code);
        Assert.Contains("past this archive's head", past.Error);
        Assert.Contains("no engine 'hub'", Inspect("record", archive, "--at-lsn", "1", "--engine", "hub").Error);
    }

    [Fact]
    public async Task Row_shows_who_mutated_a_key_and_what_each_version_changed()
    {
        await using var host = await HostWithHistoryAsync();
        var (code, output, error) = Inspect("row", LogDir(host), "--table", "Chunk", "--key", "1");
        Assert.True(code == 0, error);
        Assert.Contains("row       Chunk  Id = 1", output);
        Assert.Contains("2 commits touched it", output);
        Assert.Matches(@"LSN 1  .*  insert  by SetChunk  caller 560827fa", output);
        Assert.Matches(@"X\s+10\r?\n", output);
        Assert.Matches(@"X\s+11   \(was 10\)", output);
        Assert.Matches(@"Data\s+0x0506   \(was 0x010203\)", output);

        var deleted = Inspect("row", LogDir(host), "--table", "Chunk", "--key", "2", "--json");
        Assert.True(deleted.Code == 0, deleted.Error);
        using var json = JsonDocument.Parse(deleted.Output);
        var history = json.RootElement.GetProperty("history");
        Assert.Equal(["insert", "delete"], history.EnumerateArray().Select(static h => h.GetProperty("op").GetString()));
        Assert.Equal(JsonValueKind.Null, history[1].GetProperty("row").ValueKind);
        Assert.Equal("DeleteChunk", history[1].GetProperty("reducer").GetString());

        // The encoded key a write set carries works too, and agrees.
        var hex = json.RootElement.GetProperty("row").GetProperty("key").GetProperty("hex").GetString()!;
        Assert.Contains("2 commits touched it", Inspect("row", LogDir(host), "--table", "Chunk", "--key-hex", hex).Output);

        Assert.Contains("not a Int64 key for Chunk.Id", Inspect("row", LogDir(host), "--table", "Chunk", "--key", "one").Error);
        var wrongCase = Inspect("row", LogDir(host), "--table", "chunk", "--key", "1");
        Assert.Equal(1, wrongCase.Code);
        Assert.Equal("melange inspect: no shape in this source names a table 'chunk'. Names are case-sensitive; inspect log lists the tables a commit wrote.", wrongCase.Error.Trim());
        Assert.Contains("0 commits touched it", Inspect("row", LogDir(host), "--table", "Chunk", "--key", "42").Output);
    }

    [Fact]
    public async Task Row_in_an_archive_starts_from_the_snapshots_version()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?> { ["MelangeDb:Resume:RetentionWindowSeconds"] = "0" });
        host.Call("SetChunk", 1L, 10L, new byte[] { 1 });
        Assert.NotNull(host.Engine.TakeSnapshot());
        host.Call("SetChunk", 1L, 12L, new byte[] { 1 });
        var archive = Path.Combine(host.Root, "world.mbak");
        await host.RestartAsync(whileStopped: () => MelangeBackup.Create(LogDir(host), archive));

        var (code, output, error) = Inspect("row", archive, "--table", "Chunk", "--key", "1");
        Assert.True(code == 0, error);
        Assert.Contains("as of the snapshot at LSN 1:", output);
        Assert.Contains("an archive holds only the log above its snapshot", output);
        Assert.Matches(@"X\s+12   \(was 10\)", output);
        Assert.Contains("1 commit touched it", output);
    }

    [Fact]
    public void Usage_errors_name_what_is_wrong()
    {
        Assert.Equal(2, Inspect().Code);
        Assert.Contains("unknown verb 'frob'", Inspect("frob", ".").Error);
        Assert.Contains("takes --at-lsn", Inspect("record", ".").Error);
        Assert.Contains("does not take --at-lsn", Inspect("log", ".", "--at-lsn", "3").Error);
        Assert.Contains("an LSN", Inspect("log", ".", "--from", "x").Error);
        Assert.Contains("past --to", Inspect("log", ".", "--from", "5", "--to", "4").Error);
        Assert.Contains("in hex", Inspect("log", ".", "--caller", "zz").Error);
        Assert.Contains("one of --key", Inspect("row", ".", "--table", "Chunk").Error);
        Assert.Contains("one of --key", Inspect("row", ".", "--table", "Chunk", "--key", "1", "--key-hex", "00").Error);
        var missing = Inspect("log", Path.Combine(Path.GetTempPath(), "melange-no-such-" + Guid.NewGuid().ToString("N")));
        Assert.Equal(1, missing.Code);
        Assert.Contains("does not exist", missing.Error);
    }

    private static ulong LsnOf(TransportTestHost host, string reducer, int nth = 1)
    {
        using var source = InspectSource.Open(LogDir(host));
        return source.Records().Where(r => r.ReducerName == reducer).Skip(nth - 1).First().Lsn;
    }
}
