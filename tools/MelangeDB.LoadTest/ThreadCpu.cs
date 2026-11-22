using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace MelangeDB.LoadTest;

/// <summary>
/// Where the serve process's CPU goes, by kind of thread, measured from inside the process with no
/// profiler attached. The runtime names its threads (".NET Server GC", ".NET TP Worker", and so
/// on) through the OS thread description, so summing each thread's processor time under its name
/// separates collection from work. Windows only; elsewhere the breakdown is empty.
/// </summary>
internal static partial class ThreadCpu
{
    private const int ThreadQueryLimitedInformation = 0x0800;

    /// <summary>Processor seconds so far, summed per thread name with digits collapsed.</summary>
    public static Dictionary<string, double> SecondsByKind()
    {
        var totals = new Dictionary<string, double>(StringComparer.Ordinal);
        if (!OperatingSystem.IsWindows())
            return totals;

        using var process = Process.GetCurrentProcess();
        foreach (ProcessThread thread in process.Threads)
        {
            double seconds;
            try
            {
                seconds = thread.TotalProcessorTime.TotalSeconds;
            }
            catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
            {
                continue; // Exited between the listing and the read.
            }

            var kind = Kind(Describe(thread.Id));
            totals[kind] = totals.GetValueOrDefault(kind) + seconds;
        }

        return totals;
    }

    private static string Kind(string? description) =>
        string.IsNullOrWhiteSpace(description) ? "(unnamed)" : Digits().Replace(description.Trim(), "#");

    private static string? Describe(int threadId)
    {
        var handle = OpenThread(ThreadQueryLimitedInformation, false, (uint)threadId);
        if (handle == IntPtr.Zero)
            return null;
        try
        {
            if (GetThreadDescription(handle, out var text) < 0 || text == IntPtr.Zero)
                return null;
            try
            {
                return Marshal.PtrToStringUni(text);
            }
            finally
            {
                LocalFree(text);
            }
        }
        finally
        {
            CloseHandle(handle);
        }
    }

    [GeneratedRegex(@"\d+")]
    private static partial Regex Digits();

#pragma warning disable SYSLIB1054 // Plain DllImport keeps this tool free of AllowUnsafeBlocks.
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr OpenThread(int access, bool inherit, uint threadId);

    [DllImport("kernel32.dll")]
    private static extern int GetThreadDescription(IntPtr thread, out IntPtr description);

    [DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr memory);

    [DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr handle);
#pragma warning restore SYSLIB1054
}
