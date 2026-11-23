using System.Buffers.Binary;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

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
/// directory. On Windows it renames with POSIX semantics, so handles open on the destination do
/// not refuse the replace, and flushes the renamed file's handle, which is what
/// <c>MOVEFILE_WRITE_THROUGH</c> does; Windows has no directory fsync to call. Where POSIX
/// renames are unsupported it falls back to <c>MoveFileEx</c> with <c>MOVEFILE_WRITE_THROUGH</c>,
/// which a reader holding the destination open makes fail.</para>
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
            if (!Windows.TryReplaceAsPosix(source, destination))
                Windows.ReplaceWithMoveFile(source, destination);
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
        private const uint MoveFileReplaceExisting = 0x1;
        private const uint MoveFileWriteThrough = 0x8;

        private const uint Delete = 0x0001_0000;
        private const uint Synchronize = 0x0010_0000;
        private const uint GenericWrite = 0x4000_0000;
        private const uint ShareAll = 0x7; // read, write, delete
        private const uint OpenExisting = 3;

        private const int FileRenameInfoEx = 22;
        private const uint RenameReplaceIfExists = 0x1;
        private const uint RenamePosixSemantics = 0x2;

        private const int ErrorInvalidFunction = 1;
        private const int ErrorNotSupported = 50;
        private const int ErrorInvalidParameter = 87;

        /// <summary>
        /// Renames with POSIX semantics: the replaced file's name goes at once, and handles still
        /// open on it keep reading the old file, as on Linux. A plain replace is refused while any
        /// other handle has the destination open, and the commit log's readers each hold one. The
        /// rename is made durable by flushing the renamed file's handle, which is how
        /// <c>MOVEFILE_WRITE_THROUGH</c> does it. Returns false when the filesystem or Windows
        /// version has no POSIX rename (FAT, or before Windows 10 1709), for the caller to fall back.
        /// </summary>
        public static bool TryReplaceAsPosix(string source, string destination)
        {
            using var handle = CreateFileW(Path.GetFullPath(source), Delete | Synchronize | GenericWrite, ShareAll, 0, OpenExisting, 0, 0);
            if (handle.IsInvalid)
                throw Failed(Marshal.GetLastPInvokeError(), source, destination);

            // FILE_RENAME_INFO: a flags word, the root directory handle at pointer alignment, the
            // name's length in bytes, then the name inline.
            var name = Path.GetFullPath(destination);
            var nameOffset = (2 * IntPtr.Size) + sizeof(uint);
            var size = nameOffset + ((name.Length + 1) * sizeof(char));
            var buffer = new byte[Math.Max(size, 3 * IntPtr.Size)];
            BinaryPrimitives.WriteUInt32LittleEndian(buffer, RenameReplaceIfExists | RenamePosixSemantics);
            BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(2 * IntPtr.Size), (uint)(name.Length * sizeof(char)));
            MemoryMarshal.AsBytes(name.AsSpan()).CopyTo(buffer.AsSpan(nameOffset));

            if (!SetFileInformationByHandle(handle, FileRenameInfoEx, buffer, (uint)buffer.Length))
            {
                var error = Marshal.GetLastPInvokeError();
                if (error is ErrorInvalidParameter or ErrorNotSupported or ErrorInvalidFunction)
                    return false;
                throw Failed(error, source, destination);
            }

            if (!FlushFileBuffers(handle))
                throw Failed(Marshal.GetLastPInvokeError(), source, destination);
            return true;
        }

        public static void ReplaceWithMoveFile(string source, string destination)
        {
            if (!MoveFileExW(source, destination, MoveFileReplaceExisting | MoveFileWriteThrough))
                throw Failed(Marshal.GetLastPInvokeError(), source, destination);
        }

        private static IOException Failed(int error, string source, string destination) =>
            new($"Could not replace '{destination}' with '{source}': {new Win32Exception(error).Message}", error);

        [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool MoveFileExW(string existingFileName, string newFileName, uint flags);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern SafeFileHandle CreateFileW(string fileName, uint desiredAccess, uint shareMode, nint securityAttributes, uint creationDisposition, uint flagsAndAttributes, nint templateFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool SetFileInformationByHandle(SafeFileHandle file, int fileInformationClass, byte[] fileInformation, uint bufferSize);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool FlushFileBuffers(SafeFileHandle file);
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
