using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The online backup stream (road-to-0.2 phase 15): a consistent archive at a fenced LSN under
/// sustained live writes, a truncation pin held for exactly the stream's duration, and — the
/// bounded half of the contract — a stalled client aborted at <c>Backup:StreamStallTimeoutMs</c>
/// with the pin released, because a wedged backup client must not become a full disk.
/// </summary>
public class BackupStreamTests
{
    private const string BackupOwnerRole = "melange-backup-owner";

    [Fact]
    public async Task A_backup_under_sustained_live_writes_captures_one_fenced_lsn_and_verifies()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Backup:Enabled"] = "true",
            ["MelangeDb:Resume:RetentionWindowSeconds"] = "0",
        });
        for (var i = 0L; i < 200; i++)
            host.Call("SetChunk", i, i % 16, new byte[512]);

        // A snapshot first, so the stream exercises the full online shape — snapshot rows plus
        // the tail above them — against a truncated log.
        Assert.NotNull(host.Engine.TakeSnapshot());
        var baseBefore = host.Engine.Log.BaseLsn;
        Assert.True(baseBefore > 0);
        for (var i = 200L; i < 260; i++)
            host.Call("SetChunk", i, i % 16, new byte[512]);

        // The 2 p.m. backup: writes keep landing for the whole stream.
        using var pumpStop = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            var next = 1_000L;
            while (!pumpStop.IsCancellationRequested)
            {
                host.Call("SetChunk", next++, next % 16, new byte[256]);
                await Task.Yield();
            }
        }, TestContext.Current.CancellationToken);

        try
        {
            using var http = host.CreateHttp(TestTokens.For("operator", role: BackupOwnerRole));
            var response = await http.GetAsync("/melange/backup", HttpCompletionOption.ResponseHeadersRead, TestContext.Current.CancellationToken);
            Assert.True(response.IsSuccessStatusCode);
            var archive = Path.Combine(host.Root, "live-download.mbak");
            await using (var output = File.Create(archive))
            await using (var stream = await response.Content.ReadAsStreamAsync(TestContext.Current.CancellationToken))
            {
                await stream.CopyToAsync(output, TestContext.Current.CancellationToken);
            }

            // Verify proves the fence: every frame intact, the record chain contiguous from the
            // snapshot LSN to the declared head — writes that landed after the fence are absent,
            // not torn. The pump is still committing, so the world provably moved past the fence
            // while the archive stayed one consistent instant.
            var report = MelangeBackup.Verify(archive);
            var engine = Assert.Single(report.Engines);
            Assert.True(engine.Identity.SnapshotLsn > 0, "the archive should carry the snapshot");
            Assert.True(engine.Identity.HeadLsn >= 260);
            await TransportTestHost.WaitUntilAsync(
                () => host.Engine.Log.HeadLsn > engine.Identity.HeadLsn,
                "the pump to commit past the fence");
        }
        finally
        {
            pumpStop.Cancel();
            await pump;
        }

        // And the pin released with the stream: the next snapshot truncates past where the base
        // stood while the pin held it.
        Assert.NotNull(host.Engine.TakeSnapshot());
        Assert.True(host.Engine.Log.BaseLsn > baseBefore, "truncation should be free again once the stream ended");
    }

    [Fact]
    public async Task A_stalled_client_is_aborted_at_the_timeout_and_the_pin_releases()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Backup:Enabled"] = "true",
            ["MelangeDb:Backup:StreamStallTimeoutMs"] = "1500",
            ["MelangeDb:Resume:RetentionWindowSeconds"] = "0",
            ["MelangeDb:CommitLog:FsyncPolicy"] = "OsBuffered",
        });

        // A world big enough that the archive cannot fit in the response and socket buffers: the
        // server must block mid-stream when the client stops reading. Chunk payloads sit at the
        // Validation:MaxCollectionLength ceiling (4096), so size comes from count.
        for (var i = 0L; i < 3_000; i++)
            host.Call("SetChunk", i, i % 16, new byte[4_096]);

        using var http = host.CreateHttp(TestTokens.For("operator", role: BackupOwnerRole));
        http.Timeout = Timeout.InfiniteTimeSpan;
        var response = await http.GetAsync("/melange/backup", HttpCompletionOption.ResponseHeadersRead, TestContext.Current.CancellationToken);
        Assert.True(response.IsSuccessStatusCode);
        var stream = await response.Content.ReadAsStreamAsync(TestContext.Current.CancellationToken);

        // Read a token amount, then wedge. The watchdog sees no progress past the timeout and
        // aborts the connection; resuming the read observes the reset instead of more archive.
        var buffer = new byte[1_024];
        await stream.ReadExactlyAsync(buffer, TestContext.Current.CancellationToken);
        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(5)), TestContext.Current.CancellationToken);
        Exception? observed = null;
        try
        {
            var drain = new byte[64 * 1024];
            while (await stream.ReadAsync(drain, TestContext.Current.CancellationToken) > 0)
            {
            }
        }
        catch (Exception exception)
        {
            observed = exception;
        }

        Assert.True(observed is not null, "the download completed; the server never aborted the stalled stream");

        // The pin died with the stream — truncation is free again — and the endpoint is healthy:
        // a fresh, promptly-read download completes and verifies.
        Assert.NotNull(host.Engine.TakeSnapshot());
        Assert.True(host.Engine.Log.BaseLsn > 0, "the aborted stream must not leave a truncation pin behind");

        var archive = Path.Combine(host.Root, "post-stall.mbak");
        var retry = await http.GetAsync("/melange/backup", HttpCompletionOption.ResponseHeadersRead, TestContext.Current.CancellationToken);
        Assert.True(retry.IsSuccessStatusCode);
        await using (var output = File.Create(archive))
        await using (var body = await retry.Content.ReadAsStreamAsync(TestContext.Current.CancellationToken))
        {
            await body.CopyToAsync(output, TestContext.Current.CancellationToken);
        }

        MelangeBackup.Verify(archive);
    }
}
