namespace MelangeDB.Client;

/// <summary>
/// Where the client keeps its bearer token between runs. This matters most for guest play: the
/// IdP mints guest identities, so the token <em>is</em> the character — a client that loses it has
/// lost the character, no matter how well account linking works later. Ship a durable store
/// (<see cref="FileTokenStore"/> or your platform's secure storage) in anything real; the
/// in-memory default is honest only for tests and throwaway tools.
/// </summary>
public interface ITokenStore
{
    /// <summary>Loads the persisted token, or null when none exists.</summary>
    ValueTask<string?> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>Persists the token, replacing any previous one.</summary>
    ValueTask SaveAsync(string token, CancellationToken cancellationToken = default);
}

/// <summary>A token store that forgets on process exit. The default; fine for tests, wrong for guests.</summary>
public sealed class InMemoryTokenStore : ITokenStore
{
    private volatile string? _token;

    public ValueTask<string?> LoadAsync(CancellationToken cancellationToken = default) => new(_token);

    public ValueTask SaveAsync(string token, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(token);
        _token = token;
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// The reference durable store: one token in one file. The write is atomic (temp file + rename)
/// so a crash mid-save never leaves a torn token — a torn guest token is a lost character.
/// </summary>
public sealed class FileTokenStore(string path) : ITokenStore
{
    public async ValueTask<string?> LoadAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path))
            return null;
        var token = await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false);
        return token.Length == 0 ? null : token;
    }

    public async ValueTask SaveAsync(string token, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(token);
        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);
        var temp = path + ".tmp";
        await File.WriteAllTextAsync(temp, token, cancellationToken).ConfigureAwait(false);
        File.Move(temp, path, overwrite: true);
    }
}
