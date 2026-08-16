using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Restore's client-facing semantic (road-to-0.2 phase 15): a restore is a rewind, and the fresh
/// epoch it always mints is what forces a client whose resume cursor sits past the restored head
/// into a full resync through the existing machinery — never a resume into history that no
/// longer happened.
/// </summary>
public class BackupRestoreTransportTests
{
    [Fact]
    public async Task A_client_holding_a_pre_restore_resume_cursor_is_refused_resume_and_full_resyncs()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 2L, new byte[] { 2 });

        // The nightly backup, taken with the server stopped — the offline verb's contract. The
        // scratch lives under the host's root so it outlives the host and is reaped with it: the
        // restored directory becomes the live data directory, and deleting it before the host
        // stops would pull the log out from under the shutdown checkpoint.
        var scratch = Path.Combine(host.Root, "backup-scratch");
        Directory.CreateDirectory(scratch);
        {
            var archive = Path.Combine(scratch, "world.mbak");
            await host.RestartAsync(whileStopped: () =>
            {
                MelangeBackup.Create(Path.Combine(host.Root, "log"), archive);
                MelangeBackup.Verify(archive);
            });

            // Life goes on after the backup: a client connects, subscribes, and acks history the
            // archive does not contain.
            await using var client = host.CreateClient();
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(2, subscription.Count);
            var epochBefore = client.LogEpochId;
            var postBackupHead = host.Call("SetChunk", 3L, 3L, new byte[] { 3 });
            await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= postBackupHead, "the post-backup delta to arrive");
            Assert.Equal(3, subscription.Count);
            client.Abort();

            // Disaster; the archive is restored and the server boots from it. Chunk 3 rewinds out
            // of existence, and the restored world runs under a fresh epoch.
            var restored = Path.Combine(scratch, "restored");
            await host.RestartAsync(
                whileStopped: () => MelangeBackup.Restore(archive, restored),
                settings: new Dictionary<string, string?>
                {
                    ["MelangeDb:CommitLog:Path"] = restored,
                    ["MelangeDb:HotStore:Path"] = Path.Combine(scratch, "hot"),
                });

            // The cursor names an epoch the restored world has never seen: refused resume, full
            // resync, and the initial set is the restored truth — two chunks, not three.
            var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
            Assert.False(resumed, "a pre-restore cursor must be refused resume");
            Assert.NotEqual(epochBefore, client.LogEpochId);
            Assert.Equal(2, subscription.Count);

            // The restored world is live: new writes commit and flow to the resynced client.
            var newHead = host.Call("SetChunk", 4L, 4L, new byte[] { 4 });
            await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= newHead, "a post-restore delta to arrive");
            Assert.Equal(3, subscription.Count);
        }
    }
}
