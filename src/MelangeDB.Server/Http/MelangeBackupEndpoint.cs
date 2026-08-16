using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Server;

/// <summary>
/// GET {path}/backup — the online form of <c>melange backup</c>: streams a <c>.mbak</c> archive
/// of the live engine at a fenced LSN, holding a truncation pin for exactly the stream's
/// duration. Gated like the other privileged HTTP surfaces (<c>Sql:*</c>, <c>Bulk:*</c>): off by
/// default (<c>Backup:Enabled</c>), owner-role-gated when on (<c>Backup:OwnerRole</c>, its own
/// key — backup is read-<em>everything</em> by definition, policies included). Mapped
/// unconditionally and gated per request, the <c>/schema</c> pattern, because the option is
/// live-reloadable.
/// <para>
/// The pin is what makes the stream consistent, and like every truncation pin it must be
/// bounded: a client that stops reading stalls the response pipe, the watchdog sees no progress
/// for <c>Backup:StreamStallTimeoutMs</c>, and the connection is aborted — the pin releases with
/// it, because a wedged backup client must not become a full disk.
/// </para>
/// </summary>
internal static class MelangeBackupEndpoint
{
    public static async Task BackupAsync(HttpContext context, MelangeTransport transport)
    {
        if (await MelangeHttpEndpoints.AuthenticateAsync(context, transport).ConfigureAwait(false) is not { } session)
            return;
        var options = transport.Options.Backup;
        if (!options.Enabled)
        {
            await MelangeHttpEndpoints.WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.BackupDisabled,
                "The online backup endpoint is disabled; set Backup:Enabled to true to opt in. " +
                "The offline form (melange backup <data-dir>, server stopped) needs no configuration.").ConfigureAwait(false);
            return;
        }

        if (!session.IsBackupOwner)
        {
            await MelangeHttpEndpoints.WriteErrorAsync(
                context, StatusCodes.Status403Forbidden, MelangeErrorCodes.OwnerRequired,
                "This caller's token carries no Backup:OwnerRole claim; backup reads everything, and the capability is never granted implicitly.").ConfigureAwait(false);
            return;
        }

        // The archive writer is synchronous by design (its other callers write files), so this
        // request opts into synchronous body writes; the watchdog below is what bounds them.
        if (context.Features.Get<IHttpBodyControlFeature>() is { } bodyControl)
            bodyControl.AllowSynchronousIO = true;
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "application/octet-stream";
        context.Response.Headers.ContentDisposition = "attachment; filename=\"world.mbak\"";

        var stallTimeout = TimeSpan.FromMilliseconds(Math.Max(1_000, options.StreamStallTimeoutMs));
        var started = transport.Time.GetTimestamp();
        var guard = new StallGuardedStream(context.Response.Body, transport.Time);
        LogMessages.BackupStreamStarted(transport.Logger);
        using var watchdog = transport.Time.CreateTimer(
            _ =>
            {
                if (guard.SinceLastProgress() > stallTimeout)
                {
                    guard.MarkStalled();
                    context.Abort();
                }
            },
            null,
            stallTimeout,
            TimeSpan.FromMilliseconds(Math.Max(250, stallTimeout.TotalMilliseconds / 4)));

        try
        {
            // On a hub the endpoint fans out: its own engine plus every shard engine under
            // Cluster:ShardDataPath, over shared storage, one fenced LSN per engine — the whole
            // cluster under one manifest, per-shard consistent.
            var cluster = transport.Options.Cluster;
            var summary = cluster.Role == ClusterRole.Hub
                ? MelangeBackup.CreateClusterOnline(transport.Engine, cluster.ShardDataPath, guard)
                : MelangeBackup.CreateOnline(transport.Engine, guard);
            await context.Response.Body.FlushAsync(context.RequestAborted).ConfigureAwait(false);
            var elapsed = transport.Time.GetElapsedTime(started).TotalMilliseconds;
            LogMessages.BackupStreamCompleted(transport.Logger, summary.TotalBytes, elapsed, summary.Engines[0].HeadLsn);
            transport.Telemetry?.RecordBackupStream(summary.TotalBytes, elapsed, "completed");
        }
        catch (Exception exception) when (exception is IOException or OperationCanceledException or ObjectDisposedException or InvalidOperationException)
        {
            // The pin released when CreateOnline unwound; abort the transfer so the client sees a
            // broken download, never a 200 with a truncated body that might pass for an archive.
            context.Abort();
            var elapsed = transport.Time.GetElapsedTime(started).TotalMilliseconds;
            LogMessages.BackupStreamAborted(
                transport.Logger, guard.Stalled ? "the client stalled past Backup:StreamStallTimeoutMs" : "the client disconnected or the capture failed",
                guard.BytesWritten, exception);
            transport.Telemetry?.RecordBackupStream(guard.BytesWritten, elapsed, "aborted");
        }
    }

    /// <summary>
    /// Progress is a completed write into the response pipe: Kestrel's response buffer gives a
    /// stalled client a little rope (its default buffer), after which writes block and the last
    /// progress timestamp freezes — which is exactly what the watchdog is watching.
    /// </summary>
    private sealed class StallGuardedStream(Stream inner, TimeProvider time) : Stream
    {
        private long _lastProgress = time.GetTimestamp();
        private long _bytesWritten;
        private volatile bool _stalled;

        public long BytesWritten => Volatile.Read(ref _bytesWritten);

        public bool Stalled => _stalled;

        public TimeSpan SinceLastProgress() => time.GetElapsedTime(Volatile.Read(ref _lastProgress));

        public void MarkStalled() => _stalled = true;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            inner.Write(buffer);
            Volatile.Write(ref _lastProgress, time.GetTimestamp());
            Interlocked.Add(ref _bytesWritten, buffer.Length);
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, Exception?> StartedMessage =
            LoggerMessage.Define(
                LogLevel.Information,
                new EventId(1801, "BackupStreamStarted"),
                "Online backup stream started; log truncation is pinned for its duration.");

        private static readonly Action<ILogger, long, double, ulong, Exception?> CompletedMessage =
            LoggerMessage.Define<long, double, ulong>(
                LogLevel.Information,
                new EventId(1802, "BackupStreamCompleted"),
                "Online backup stream completed: {Bytes} bytes in {ElapsedMs:F0} ms, fenced at LSN {Fence}. The truncation pin is released.");

        private static readonly Action<ILogger, long, string, Exception?> AbortedMessage =
            LoggerMessage.Define<long, string>(
                LogLevel.Warning,
                new EventId(1803, "BackupStreamAborted"),
                "Online backup stream aborted after {Bytes} bytes: {Reason}. The truncation pin is released; the partial download will fail verify.");

        public static void BackupStreamStarted(ILogger logger) => StartedMessage(logger, null);

        public static void BackupStreamCompleted(ILogger logger, long bytes, double elapsedMs, ulong fence) =>
            CompletedMessage(logger, bytes, elapsedMs, fence, null);

        public static void BackupStreamAborted(ILogger logger, string reason, long bytes, Exception exception) =>
            AbortedMessage(logger, bytes, reason, exception);
    }
}
