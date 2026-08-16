using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The cluster archive (road-to-0.2 phase 15): the hub's `/melange/backup` fans out — its own
/// engine plus every shard engine over shared storage, one fenced LSN per engine, under one
/// manifest keyed by shard. Per-shard consistent, not globally consistent: there is no global
/// total order to capture, and the test asserts the bound rather than assuming it away — shards
/// captured at different LSNs restore to a working world.
/// </summary>
public class ClusterBackupTests
{
    private const string BackupOwnerRole = "melange-backup-owner";

    [Fact]
    public async Task The_cluster_round_trip_hub_plus_shards_out_restored_and_booted()
    {
        var scratch = Directory.CreateTempSubdirectory("melange-cluster-backup-").FullName;
        var archive = Path.Combine(scratch, "cluster.mbak");
        Guid hubEpochBefore;
        try
        {
            var fixture = await ClusterFixture.StartAsync(
                shardNodes: 2,
                extraSettings: new Dictionary<string, string?> { ["MelangeDb:Backup:Enabled"] = "true" });
            await using (fixture)
            {
                await fixture.EnsureShardOwnedAsync(80);
                await fixture.EnsureShardOwnedAsync(81);

                // Hub truth (Global and Replicated tables) plus deliberately different shard
                // histories, so the two shards' fenced LSNs cannot coincide.
                fixture.HubReducers.Call("BumpGlobal", ClusterFixture.Caller, 7L);
                fixture.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 1L, "sword");
                for (var i = 0; i < 3; i++)
                    await fixture.Coordinator.ExecuteOnShardAsync(new ShardKey(80), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [80u, 10], TestContext.Current.CancellationToken);
                for (var i = 0; i < 5; i++)
                    await fixture.Coordinator.ExecuteOnShardAsync(new ShardKey(81), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [81u, 10], TestContext.Current.CancellationToken);

                // One shard snapshots and truncates before the backup, so the archive carries the
                // snapshot-plus-tail shape for it — and the truncation refresh writes the border
                // registry sidecar this test asserts rides along.
                Assert.NotNull(fixture.ShardOf(80).Engine.TakeSnapshot());
                Assert.True(fixture.ShardOf(80).Engine.Log.BaseLsn > 0);

                hubEpochBefore = fixture.HubEngine.Log.EpochId;

                // The whole-cluster download, from the hub, while both shard owners keep serving.
                using var http = new HttpClient();
                http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Bearer", TestTokens.For("operator", role: BackupOwnerRole));
                var response = await http.GetAsync(
                    new Uri($"http://127.0.0.1:{fixture.GatewayUri.Port}/melange/backup"), TestContext.Current.CancellationToken);
                Assert.True(response.IsSuccessStatusCode);
                await File.WriteAllBytesAsync(archive, await response.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
            }

            // The archive verifies, holds the whole fleet under one manifest, and the per-shard
            // consistency bound is visible: each engine fenced at its own LSN.
            var report = MelangeBackup.Verify(archive);
            Assert.Equal(["hub", "shard-80", "shard-81"], report.Engines.Select(e => e.Identity.Key));
            var shard80 = report.Engines.Single(e => e.Identity.Key == "shard-80").Identity;
            var shard81 = report.Engines.Single(e => e.Identity.Key == "shard-81").Identity;
            Assert.NotEqual(shard80.HeadLsn, shard81.HeadLsn);
            Assert.True(shard80.SnapshotLsn > 0, "shard-80's snapshot should be in the archive");

            // Restore materializes the deployment layout; the border registry rode along.
            var restoreRoot = Path.Combine(scratch, "restored");
            var restore = MelangeBackup.Restore(archive, restoreRoot);
            Assert.Equal(3, restore.Engines.Count);
            Assert.True(File.Exists(Path.Combine(restoreRoot, "shards", "shard-80", "borrowed.sidecar")));

            // Boot a fresh cluster over the restored directories: hub log at {root}/hub/log per
            // the fixture's layout, shard data at {root}/shards per Cluster:ShardDataPath.
            var newRoot = Path.Combine(scratch, "reborn");
            Directory.CreateDirectory(Path.Combine(newRoot, "hub"));
            Directory.Move(Path.Combine(restoreRoot, "hub"), Path.Combine(newRoot, "hub", "log"));
            Directory.Move(Path.Combine(restoreRoot, "shards"), Path.Combine(newRoot, "shards"));

            await using var reborn = await ClusterFixture.StartAsync(shardNodes: 2, dataRoot: newRoot);
            Assert.NotEqual(hubEpochBefore, reborn.HubEngine.Log.EpochId);
            Assert.Equal(1, reborn.HubEngine.CommittedView.Count<GlobalCounter>());
            Assert.Equal(1, reborn.HubEngine.CommittedView.Count<ItemDef>());

            // Reassignment is recovery: assigning the restored shards opens their directories and
            // the worlds come back — 3 mobs and 5 mobs, captured at different fences, serving.
            var owner80 = await reborn.EnsureShardOwnedAsync(80);
            var owner81 = await reborn.EnsureShardOwnedAsync(81);
            Assert.Equal(3, owner80.Runtime.TryGetShard(new ShardKey(80))!.Engine.CommittedView.Count<Mob>());
            Assert.Equal(5, owner81.Runtime.TryGetShard(new ShardKey(81))!.Engine.CommittedView.Count<Mob>());

            // The restored world is live: a fresh spawn lands beside the recovered history.
            await reborn.Coordinator.ExecuteOnShardAsync(new ShardKey(80), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [80u, 10], TestContext.Current.CancellationToken);
            Assert.Equal(4, owner80.Runtime.TryGetShard(new ShardKey(80))!.Engine.CommittedView.Count<Mob>());
        }
        finally
        {
            try
            {
                Directory.Delete(scratch, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
