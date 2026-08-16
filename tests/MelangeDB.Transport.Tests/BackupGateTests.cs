using System.Text.Json;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The online backup gate (road-to-0.2 phase 15): <c>/melange/backup</c> reads everything —
/// every table, every row, no policy — so it is off by default (<c>Backup:Enabled</c>) and
/// owner-role-gated when on (<c>Backup:OwnerRole</c>, its own key: read-everything is not
/// write-anything is not backup-everything).
/// </summary>
public class BackupGateTests
{
    private const string BackupOwnerRole = "melange-backup-owner";
    private const string SqlOwnerRole = "melange-owner";
    private const string BulkOwnerRole = "melange-bulk-owner";
    private const string ClusterSecret = "backup-gate-cluster-secret";

    private static readonly Dictionary<string, string?> Enabled = new()
    {
        ["MelangeDb:Backup:Enabled"] = "true",
    };

    /// <summary>A shard-role host, so the authenticator accepts internal identity assertions.</summary>
    private static readonly Dictionary<string, string?> EnabledClustered = new()
    {
        ["MelangeDb:Backup:Enabled"] = "true",
        ["MelangeDb:Cluster:Role"] = "Shard",
        ["MelangeDb:Cluster:NodeName"] = "backup-gate-tests",
        ["MelangeDb:Cluster:Secret"] = ClusterSecret,
    };

    [Fact]
    public async Task Disabled_by_default_even_for_a_token_carrying_the_backup_owner_role()
    {
        await using var host = await TransportTestHost.StartAsync();
        foreach (var role in new string?[] { null, BackupOwnerRole })
        {
            using var http = host.CreateHttp(TestTokens.For("operator", role: role));
            var response = await http.GetAsync("/melange/backup", TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("backup_disabled", (await ReadJsonAsync(response)).GetProperty("error").GetString());
        }
    }

    [Fact]
    public async Task Enabled_still_refuses_a_caller_without_the_backup_owner_claim()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);

        // No role, the SQL owner role, and the bulk owner role: each existing owner capability
        // must not imply this one.
        foreach (var role in new string?[] { null, SqlOwnerRole, BulkOwnerRole })
        {
            using var http = host.CreateHttp(TestTokens.For("caller", role: role));
            var response = await http.GetAsync("/melange/backup", TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("owner_required", (await ReadJsonAsync(response)).GetProperty("error").GetString());
        }
    }

    [Fact]
    public async Task Enabled_with_the_backup_owner_claim_streams_an_archive_that_verifies()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);
        host.Call("SetChunk", 1L, 1L, new byte[] { 1, 2, 3 });
        host.Call("SetChunk", 2L, 2L, new byte[] { 4 });

        using var http = host.CreateHttp(TestTokens.For("operator", role: BackupOwnerRole));
        var response = await http.GetAsync("/melange/backup", TestContext.Current.CancellationToken);
        Assert.True(response.IsSuccessStatusCode);
        var archive = Path.Combine(host.Root, "gate-download.mbak");
        await File.WriteAllBytesAsync(archive, await response.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        var report = MelangeBackup.Verify(archive);
        var engine = Assert.Single(report.Engines);
        Assert.Equal(host.Engine.Log.EpochId, engine.Identity.SourceEpoch);
        Assert.Equal(host.Engine.Log.HeadLsn, engine.Identity.HeadLsn);
    }

    [Fact]
    public async Task An_assertion_minted_with_backup_owner_authorizes_and_one_without_is_refused()
    {
        await using var host = await TransportTestHost.StartAsync(EnabledClustered);

        var authorized = InternalIdentityAssertion.Mint(
            ClusterSecret, TestTokens.IdentityOf("operator"),
            isGuest: false, isSqlOwner: false, isBulkOwner: false, DateTimeOffset.UtcNow.AddMinutes(5),
            firesLifecycle: false, isBackupOwner: true);
        using (var http = host.CreateHttp(authorized))
        {
            var response = await http.GetAsync("/melange/backup", TestContext.Current.CancellationToken);
            Assert.True(response.IsSuccessStatusCode);
        }

        // Every other capability set, and still refused: the flag is additive and fail-closed.
        var unauthorized = InternalIdentityAssertion.Mint(
            ClusterSecret, TestTokens.IdentityOf("operator"),
            isGuest: false, isSqlOwner: true, isBulkOwner: true, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var http = host.CreateHttp(unauthorized))
        {
            var response = await http.GetAsync("/melange/backup", TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("owner_required", (await ReadJsonAsync(response)).GetProperty("error").GetString());
        }
    }

    [Fact]
    public async Task The_cli_url_form_downloads_a_verifiable_archive_and_its_refusal_names_the_gates()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);
        host.Call("SetChunk", 5L, 5L, new byte[] { 5 });

        var archive = Path.Combine(host.Root, "cli-download.mbak");
        var bytes = await MelangeDB.Cli.BackupFetcher.FetchAsync(
            new Uri(host.HttpBase.ToString()), archive, TestTokens.For("operator", role: BackupOwnerRole));
        Assert.Equal(bytes, new FileInfo(archive).Length);
        MelangeBackup.Verify(archive);

        // A refused fetch tells the operator which knobs matter, and leaves no partial file.
        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => MelangeDB.Cli.BackupFetcher.FetchAsync(
            new Uri(host.HttpBase.ToString()), Path.Combine(host.Root, "refused.mbak"), TestTokens.For("operator", role: null)));
        Assert.Contains("Backup:OwnerRole", refusal.Message);
        Assert.False(File.Exists(Path.Combine(host.Root, "refused.mbak")));
    }

    [Fact]
    public async Task An_unauthenticated_request_is_401_before_the_gate_answers_anything()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);
        using var anonymous = host.CreateHttp(token: null);

        var response = await anonymous.GetAsync("/melange/backup", TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private static async Task<JsonElement> ReadJsonAsync(HttpResponseMessage response)
    {
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonDocument.Parse(text).RootElement.Clone();
    }
}
