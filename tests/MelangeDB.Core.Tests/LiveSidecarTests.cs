using System.Text.Json;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// #321: an online backup and the event bus share <c>melange.events.json</c>, which the bus
/// replaces after every delivered event. On Windows a replace fails while another handle holds
/// the file without sharing delete, and an open fails while a replace is under way. Neither side
/// may break the other. (On Linux both always succeed, and these pass trivially.)
/// </summary>
public class LiveSidecarTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-sidecar-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private string SidecarPath => Path.Combine(_root, "melange.events.json");

    private static ulong LsnOf(byte[] json, string subscriber) =>
        JsonSerializer.Deserialize<Dictionary<string, EventCheckpointStore.Entry>>(json)![subscriber].Lsn;

    [Fact]
    public void The_bus_replaces_the_sidecar_while_a_backup_holds_it_open()
    {
        var store = new EventCheckpointStore(_root);
        store.Entries["audit"] = new EventCheckpointStore.Entry { Lsn = 7 };
        Assert.Null(store.Save());

        using (var backup = OnlineEngineCapture.OpenLiveSidecar(SidecarPath))
        {
            store.Entries["audit"].Lsn = 8;
            Assert.Null(store.Save());

            // The backup's handle still reads the complete file it opened.
            var held = new byte[backup.Length];
            backup.ReadExactly(held);
            Assert.Equal(7UL, LsnOf(held, "audit"));
        }

        Assert.Equal(8UL, LsnOf(File.ReadAllBytes(SidecarPath), "audit"));
    }

    [Fact]
    public void A_save_the_file_refuses_returns_the_failure_instead_of_throwing()
    {
        var store = new EventCheckpointStore(_root);
        store.Entries["audit"] = new EventCheckpointStore.Entry { Lsn = 7 };
        Assert.Null(store.Save());

        // What File.ReadAllBytes opens with: on Windows, no replace gets past it.
        using (new FileStream(SidecarPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            store.Entries["audit"].Lsn = 8;
            var failure = store.Save();
            if (OperatingSystem.IsWindows())
                Assert.NotNull(failure);
        }

        // The checkpoints stayed in memory, so the next save writes them.
        Assert.Null(store.Save());
        Assert.Equal(8UL, LsnOf(File.ReadAllBytes(SidecarPath), "audit"));
    }

    [Fact]
    public async Task A_backup_that_opens_the_sidecar_mid_replace_waits_for_it()
    {
        var store = new EventCheckpointStore(_root);
        store.Entries["audit"] = new EventCheckpointStore.Entry { Lsn = 7 };
        Assert.Null(store.Save());

        // An exclusive handle stands in for the replace under way; it lets go shortly.
        var exclusive = new FileStream(SidecarPath, FileMode.Open, FileAccess.Read, FileShare.None);
        var read = Task.Run(() => OnlineEngineCapture.ReadLiveSidecar(SidecarPath), TestContext.Current.CancellationToken);
        await Task.Delay(30, TestContext.Current.CancellationToken);
        await exclusive.DisposeAsync();

        var bytes = await read;
        Assert.NotNull(bytes);
        Assert.Equal(7UL, LsnOf(bytes, "audit"));
    }

    [Fact]
    public void A_missing_sidecar_reads_as_none()
    {
        Assert.Null(OnlineEngineCapture.ReadLiveSidecar(SidecarPath));
    }
}
