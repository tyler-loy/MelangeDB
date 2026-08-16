namespace MelangeDB.Cli;

/// <summary>
/// The online half of `melange backup`: fetches a <c>.mbak</c> archive from a running server's
/// backup endpoint. Unlike the schema endpoint this surface is privileged — the server refuses
/// it unless <c>Backup:Enabled</c> is on and the presented bearer token carries the
/// <c>Backup:OwnerRole</c> claim — so the verb takes a token where `melange schema` takes none.
/// The download lands in a temp file and is swapped in atomically, so an interrupted fetch never
/// leaves a plausible-looking partial archive (which would fail verify anyway — but a file that
/// never exists beats a file that fails).
/// </summary>
internal static class BackupFetcher
{
    public const string DefaultEndpointPath = "/melange/backup";

    public static async Task<long> FetchAsync(Uri source, string outputPath, string? token)
    {
        var uri = source.AbsolutePath is "" or "/"
            ? new Uri(source, DefaultEndpointPath)
            : source;

        // No client timeout: a large world legitimately streams for a while, and the server's own
        // Backup:StreamStallTimeoutMs is the bound that matters.
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        if (!string.IsNullOrEmpty(token))
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"The server answered {(int)response.StatusCode} for {uri}. The online backup requires " +
                "Backup:Enabled on the server and a bearer token carrying its Backup:OwnerRole claim " +
                $"(pass one with --token).{(body.Length > 0 ? $" Server said: {body}" : "")}");
        }

        var tempPath = outputPath + ".tmp";
        try
        {
            await using (var file = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await response.Content.CopyToAsync(file);
                file.Flush(flushToDisk: true);
            }

            File.Move(tempPath, outputPath, overwrite: true);
        }
        catch
        {
            try
            {
                File.Delete(tempPath);
            }
            catch (IOException)
            {
            }

            throw;
        }

        return new FileInfo(outputPath).Length;
    }
}
