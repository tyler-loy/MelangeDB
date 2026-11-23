using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <see cref="DurableFile"/>: replacement that survives power loss. Power loss itself cannot be
/// simulated here, so these hold the observable contract — the replacement happens, nothing is left
/// behind, and a failure throws rather than passing for durable — on whichever OS runs the suite.
/// CI runs Linux, so the directory-fsync path is exercised there; a Windows run exercises the
/// POSIX-semantics rename and its flush.
/// </summary>
public class DurableFileTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-durable-").FullName;

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

    [Fact]
    public void WriteAllBytes_creates_the_file_and_leaves_no_temp_behind()
    {
        var path = Path.Combine(_root, "sidecar");

        DurableFile.WriteAllBytes(path, [1, 2, 3]);

        Assert.Equal([1, 2, 3], File.ReadAllBytes(path));
        Assert.False(File.Exists(path + ".tmp"));
    }

    [Fact]
    public void WriteAllBytes_replaces_an_existing_file_whole()
    {
        var path = Path.Combine(_root, "sidecar");
        File.WriteAllBytes(path, [9, 9, 9, 9, 9, 9]);

        DurableFile.WriteAllBytes(path, [4, 5]);

        Assert.Equal([4, 5], File.ReadAllBytes(path));
    }

    [Fact]
    public void WriteAllBytes_overwrites_a_stale_temp_left_by_a_crash()
    {
        var path = Path.Combine(_root, "sidecar");
        File.WriteAllBytes(path + ".tmp", [7, 7, 7, 7]);

        DurableFile.WriteAllBytes(path, [1]);

        Assert.Equal([1], File.ReadAllBytes(path));
        Assert.False(File.Exists(path + ".tmp"));
    }

    [Fact]
    public void Replace_moves_the_source_over_the_destination()
    {
        var source = Path.Combine(_root, "log.compact");
        var destination = Path.Combine(_root, "log");
        File.WriteAllBytes(source, [1, 2]);
        File.WriteAllBytes(destination, [3, 4, 5]);

        DurableFile.Replace(source, destination);

        Assert.Equal([1, 2], File.ReadAllBytes(destination));
        Assert.False(File.Exists(source));
    }

    [Fact]
    public void Replace_succeeds_while_a_reader_holds_the_destination_open()
    {
        // The commit log's readers each open the log, and its compaction replaces it. A plain
        // Windows replace is refused while any handle is open; a POSIX rename is not, and the
        // reader keeps the file it opened, as on Linux.
        var source = Path.Combine(_root, "log.compact");
        var destination = Path.Combine(_root, "log");
        File.WriteAllBytes(source, [1, 2]);
        File.WriteAllBytes(destination, [3, 4, 5]);

        using (var reader = new FileStream(destination, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            DurableFile.Replace(source, destination);

            Assert.Equal([1, 2], File.ReadAllBytes(destination));
            var old = new byte[3];
            reader.ReadExactly(old);
            Assert.Equal([3, 4, 5], old);
        }

        Assert.False(File.Exists(source));
    }

    [Fact]
    public void Replace_of_a_missing_source_throws_and_leaves_the_destination_alone()
    {
        var destination = Path.Combine(_root, "log");
        File.WriteAllBytes(destination, [3]);

        Assert.ThrowsAny<IOException>(() => DurableFile.Replace(Path.Combine(_root, "absent"), destination));
        Assert.Equal([3], File.ReadAllBytes(destination));
    }

    [Fact]
    public void FlushDirectory_succeeds_on_a_directory_and_throws_on_a_missing_one_off_Windows()
    {
        DurableFile.FlushDirectory(_root);

        var missing = Path.Combine(_root, "absent");
        if (OperatingSystem.IsWindows())
            DurableFile.FlushDirectory(missing); // A no-op: Windows makes the rename itself durable.
        else
            Assert.ThrowsAny<IOException>(() => DurableFile.FlushDirectory(missing));
    }
}
