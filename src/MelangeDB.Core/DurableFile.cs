using System.ComponentModel;
using System.Runtime.InteropServices;

namespace MelangeDB.Core;

/// <summary>
/// Atomic file replacement that survives power loss, not only a process crash. A temp-file-then-
/// rename write is atomic against a crash, but on POSIX the rename is a change to the directory,
/// and the directory is a file of its own: until it is fsynced, power loss can bring back the old
/// entry, or a zero-length file if the temp file's contents were never flushed either. For the
/// commit log's compaction swap that window is the worst kind — appends acknowledged as durable
/// land in the new file, and a reverted rename orphans them.
/// </summary>
/// <remarks>
/// <para>On Linux and macOS, <see cref="Replace"/> renames and then fsyncs the containing
/// directory. On Windows it renames with <c>MOVEFILE_WRITE_THROUGH</c>, which does not return
/// until the move is on disk; Windows has no directory fsync to call.</para>
/// <para>Filesystems that cannot fsync a directory report <c>EINVAL</c>; that is treated as nothing
/// to do, as PostgreSQL treats it, since such a filesystem gives the guarantee another way or not
/// at all. Every other failure throws: a rename that may not be durable must not be reported as
/// one.</para>
/// </remarks>
internal static class DurableFile
{
    /// <summary>
    /// Writes <paramref name="bytes"/> to <paramref name="path"/> atomically and durably: a temp file
    /// beside it, flushed to disk, then <see cref="Replace"/>d over it.
    /// </summary>
    public static void WriteAllBytes(string path, ReadOnlySpan<byte> bytes)
    {
        var temp = path + ".tmp";
        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(bytes);
            stream.Flush(flushToDisk: true);
        }

        Replace(temp, path);
    }

    /// <summary>
    /// Renames <paramref name="source"/> over <paramref name="destination"/> and makes the rename
    /// durable. The caller must already have flushed <paramref name="source"/>'s contents; this makes
    /// the directory entry durable, not the data behind it. Both paths must be on one volume.
    /// </summary>
    public static void Replace(string source, string destination)
    {
        if (OperatingSystem.IsWindows())
        {
            if (!Windows.MoveFileExW(source, destination, Windows.MoveFileReplaceExisting | Windows.MoveFileWriteThrough))
            {
                var error = Marshal.GetLastPInvokeError();
                throw new IOException(
                    $"Could not replace '{destination}' with '{source}': {new Win32Exception(error).Message}", error);
            }

            return;
        }

        File.Move(source, destination, overwrite: true);
        FlushDirectory(Path.GetDirectoryName(Path.GetFullPath(destination))!);
    }

    /// <summary>
    /// Fsyncs a directory, making the entries created, renamed, or deleted in it durable. A no-op on
    /// Windows, where <see cref="Replace"/> gets the same guarantee from the rename itself.
    /// </summary>
    public static void FlushDirectory(string directory)
    {
        if (OperatingSystem.IsWindows())
            return;

        var fd = Posix.Open(directory);
        if (fd < 0)
        {
            var error = Marshal.GetLastPInvokeError();
            throw new IOException($"Could not open directory '{directory}' to fsync it (errno {error}).", error);
        }

        try
        {
            if (Posix.Fsync(fd) != 0)
            {
                var error = Marshal.GetLastPInvokeError();
                if (error != Posix.EINVAL)
                    throw new IOException($"Could not fsync directory '{directory}' (errno {error}).", error);
            }
        }
        finally
        {
            Posix.Close(fd);
        }
    }

    private static class Windows
    {
        public const uint MoveFileReplaceExisting = 0x1;
        public const uint MoveFileWriteThrough = 0x8;

        [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool MoveFileExW(string existingFileName, string newFileName, uint flags);
    }

    private static class Posix
    {
        // Same value on Linux and macOS.
        public const int EINVAL = 22;
        private const int ORdOnly = 0;

        // "libc" resolves on glibc, musl, and macOS; the versioned glibc soname is the fallback for
        // a glibc image whose loader does not map the bare name.
        private static bool s_versioned;

        public static int Open(string path)
        {
            if (!s_versioned)
            {
                try
                {
                    return Libc.open(path, ORdOnly);
                }
                catch (DllNotFoundException)
                {
                    s_versioned = true;
                }
            }

            return LibcSo6.open(path, ORdOnly);
        }

        public static int Fsync(int fd) => s_versioned ? LibcSo6.fsync(fd) : Libc.fsync(fd);

        public static int Close(int fd) => s_versioned ? LibcSo6.close(fd) : Libc.close(fd);

        private static class Libc
        {
            [DllImport("libc", SetLastError = true)]
            public static extern int open([MarshalAs(UnmanagedType.LPUTF8Str)] string path, int flags);

            [DllImport("libc", SetLastError = true)]
            public static extern int fsync(int fd);

            [DllImport("libc", SetLastError = true)]
            public static extern int close(int fd);
        }

        private static class LibcSo6
        {
            [DllImport("libc.so.6", SetLastError = true)]
            public static extern int open([MarshalAs(UnmanagedType.LPUTF8Str)] string path, int flags);

            [DllImport("libc.so.6", SetLastError = true)]
            public static extern int fsync(int fd);

            [DllImport("libc.so.6", SetLastError = true)]
            public static extern int close(int fd);
        }
    }
}
