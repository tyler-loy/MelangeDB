using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Core;

/// <summary>
/// The host rung of the restore check, where the schema lives. The full-fidelity proof needs the
/// application's registry — indexes, residency, the shape guard's judgement of this code against
/// these row bytes — which only the host has, so this is the API-first form of "boot the restored
/// directory and tell me it worked".
/// </summary>
public static class HostRestoreCheckExtensions
{
    /// <summary>
    /// Boots <paramref name="restoredDirectory"/> through the ordinary engine constructor with
    /// this host's schema registry, against a scratch copy, and returns what it found — without
    /// starting the host, serving traffic, or touching the directory. Throws the refusal recovery
    /// would have thrown.
    /// <para>
    /// The staging runbook's one line, and the CI job's: restore last night's archive into a
    /// temporary directory, call this, alert on the throw. It needs the host <em>built</em>, not
    /// started — <c>using var host = builder.Build(); host.CheckRestore(dir);</c> — because
    /// starting it would open the deployment's own data directory beside the one under test.
    /// </para>
    /// </summary>
    public static RestoreCheckReport CheckRestore(this IHost host, string restoredDirectory)
    {
        ArgumentNullException.ThrowIfNull(host);
        var schema = host.Services.GetService<SchemaRegistry>()
            ?? throw new InvalidOperationException(
                "This host has no MelangeDB schema registered; call AddMelangeDb before checking a restore. " +
                "Without a registry there is no full-fidelity check to run — MelangeBackup.CheckRestore(directory) " +
                "is the file-level rung that needs no schema.");
        return MelangeBackup.CheckRestore(restoredDirectory, schema, host.Services.GetService<ILoggerFactory>());
    }
}
