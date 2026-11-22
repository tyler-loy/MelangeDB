using System.Diagnostics;
using System.Text;

namespace MelangeDB.Conformance.Harness;

/// <summary>An adapter run as a child process: the protocol on its stdin and stdout, its stderr passed through.</summary>
public sealed class AdapterProcess : IAsyncDisposable
{
    private readonly Process _process;

    private AdapterProcess(Process process) => _process = process;

    public static AdapterChannel Start(string fileName, IEnumerable<string> arguments)
    {
        // A path to a file is resolved here; a bare command name (node, dotnet) is left to the PATH.
        var start = new ProcessStartInfo(File.Exists(fileName) ? Path.GetFullPath(fileName) : fileName)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            StandardInputEncoding = new UTF8Encoding(false),
            StandardOutputEncoding = new UTF8Encoding(false),
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);
        var process = Process.Start(start) ?? throw new InvalidOperationException($"Could not start adapter '{fileName}'.");
        return new AdapterChannel(process.StandardOutput, process.StandardInput, new AdapterProcess(process));
    }

    public async ValueTask DisposeAsync()
    {
        using var exit = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _process.WaitForExitAsync(exit.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _process.Kill(entireProcessTree: true);
        }

        _process.Dispose();
    }
}
