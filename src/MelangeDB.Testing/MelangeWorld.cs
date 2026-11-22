using System.Reflection;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MelangeDB.Testing;

/// <summary>
/// A real MelangeDB engine over a throwaway data directory, driven by a <see cref="ManualClock"/>:
/// the game's own tables and reducers, the real commit log, the real scheduler, the real in-memory
/// projection. Nothing is mocked. Calls go through the same dispatch path production uses
/// (argument decoding, validation, isolation), ticks fire when the test advances time, and every
/// commit is captured for assertions.
/// <code>
/// await using var world = MelangeWorld.Create(typeof(Creature).Assembly).Build();
/// world.AdvanceBy(TimeSpan.FromSeconds(60));          // every tick due in that minute fires
/// world.Call("Attack", world.Identity("alice"), deerId);
/// Assert.Single(world.LastCommit!.Updated&lt;Creature&gt;());
/// </code>
/// </summary>
public sealed class MelangeWorld : IDisposable, IAsyncDisposable
{
    private readonly MelangeWorldBuilder _builder;
    private readonly List<WorldCommit> _commits = [];
    private readonly Lock _commitsLock = new();
    private IHost _host;
    private bool _disposed;

    internal MelangeWorld(MelangeWorldBuilder builder, string root)
    {
        _builder = builder;
        Root = root;
        Clock = builder.ClockInstance;
        _host = StartHost();
    }

    /// <summary>Starts describing a world over the tables and reducers generated into <paramref name="gameAssemblies"/>.</summary>
    public static MelangeWorldBuilder Create(params Assembly[] gameAssemblies) => new(gameAssemblies);

    /// <summary>The world's clock. Time moves only through it (or <see cref="AdvanceBy"/>).</summary>
    public ManualClock Clock { get; }

    /// <summary>The world's data directory: the commit log, snapshots and sidecars. Deleted on dispose.</summary>
    public string Root { get; }

    /// <summary>The world's current time.</summary>
    public DateTimeOffset Now => Clock.GetUtcNow();

    /// <summary>
    /// The engine itself: <b>an escape hatch, not the intended path.</b> It reaches what the kit's
    /// surface does not. <c>Engine.Invoke</c> runs an ad-hoc body that skips argument decoding,
    /// validation and the reducer's own DI scope, which is exactly what <see cref="Call"/> exists
    /// to cover; prefer <see cref="Call"/> and <see cref="Read{T}"/>. Replaced by <see cref="Restart"/>.
    /// </summary>
    public MelangeEngine Engine => _host.Services.GetRequiredService<MelangeEngine>();

    /// <summary>The world's service provider, for resolving a substituted dependency. Replaced by <see cref="Restart"/>.</summary>
    public IServiceProvider Services => _host.Services;

    /// <summary>Every commit since the world started (across restarts), in LSN order.</summary>
    public IReadOnlyList<WorldCommit> Commits
    {
        get
        {
            lock (_commitsLock)
            {
                return [.. _commits];
            }
        }
    }

    /// <summary>The newest commit, or null if nothing has committed.</summary>
    public WorldCommit? LastCommit
    {
        get
        {
            lock (_commitsLock)
            {
                return _commits.Count == 0 ? null : _commits[^1];
            }
        }
    }

    /// <summary>
    /// A stable test identity for a role: the same name is the same identity everywhere, and two
    /// names never collide, so tests read as "alice attacks the deer" and never share a caller by
    /// accident.
    /// </summary>
    public Identity Identity(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return MelangeDB.Identity.Hash($"melange-testing/{name}");
    }

    /// <summary>
    /// Calls a declared reducer by name through the production dispatch path: arguments are encoded
    /// and decoded exactly as a client's would be, validated, and run in the reducer's own DI scope.
    /// Returns the commit it produced, or null when it wrote nothing. A rejection or a fault is
    /// thrown as it would be to any in-process caller; see <see cref="Rejects"/> and
    /// <see cref="Faults"/> to expect one.
    /// </summary>
    public WorldCommit? Call(string reducer, Identity caller, params object?[] arguments)
    {
        ArgumentException.ThrowIfNullOrEmpty(reducer);
        var before = Engine.Log.HeadLsn;
        _host.Services.GetRequiredService<MelangeReducerHost>().Call(reducer, caller, arguments);
        lock (_commitsLock)
        {
            // The call's own commit, not a tick it raced with: the newest commit by this reducer
            // past the head it started from. Calls run on the test's thread, so normally it is
            // simply the last one.
            for (var i = _commits.Count - 1; i >= 0 && _commits[i].Lsn > before; i--)
            {
                if (_commits[i].Reducer == reducer)
                    return _commits[i];
            }
        }

        return null;
    }

    /// <summary>
    /// Expects <paramref name="reducer"/> to reject the call (<see cref="RejectedException"/>,
    /// the reducer saying no) and returns the rejection. Fails if the call committed, or if it
    /// failed any other way: a fault is a bug, and a kit that let a fault pass as a rejection
    /// would hide exactly the class of bug a rejection test exists to catch.
    /// </summary>
    public RejectedException Rejects(string reducer, Identity caller, params object?[] arguments)
    {
        var head = Engine.Log.HeadLsn;
        try
        {
            Call(reducer, caller, arguments);
        }
        catch (RejectedException rejection)
        {
            if (Engine.Log.HeadLsn != head)
                throw new MelangeTestFailure($"'{reducer}' was rejected, but the log moved from LSN {head} to {Engine.Log.HeadLsn}.");
            return rejection;
        }
        catch (Exception other)
        {
            throw new MelangeTestFailure(
                $"Expected '{reducer}' to reject the call, but it failed with {other.GetType().Name}: {other.Message}", other);
        }

        throw new MelangeTestFailure($"Expected '{reducer}' to reject the call, but it committed.");
    }

    /// <summary>
    /// Expects <paramref name="reducer"/> to fail with something other than a rejection (a thrown
    /// exception, a refused argument, an unknown reducer) and returns it. Fails if the call
    /// committed or was rejected: a test asserting "this is a bug" must not pass on a deliberate
    /// "no".
    /// </summary>
    public Exception Faults(string reducer, Identity caller, params object?[] arguments)
    {
        try
        {
            Call(reducer, caller, arguments);
        }
        catch (RejectedException rejection)
        {
            throw new MelangeTestFailure(
                $"Expected '{reducer}' to fault, but it rejected the call: {rejection.Message}", rejection);
        }
        catch (Exception fault)
        {
            return fault;
        }

        throw new MelangeTestFailure($"Expected '{reducer}' to fault, but it committed.");
    }

    /// <summary>
    /// Reads committed state through the game's generated accessors:
    /// <c>world.Read(db =&gt; db.Creature.Id.Find(deerId))</c>. The view is read-only; it sees every
    /// commit so far and nothing in flight.
    /// </summary>
    public T Read<T>(Func<IDbView, T> read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read(Engine.CommittedView);
    }

    /// <summary>
    /// Moves time forward, firing every scheduled reducer that comes due, in order, each committing
    /// before the next fires, all before this returns.
    /// </summary>
    public void AdvanceBy(TimeSpan by) => Clock.Advance(by);

    /// <summary>Moves time forward to <paramref name="instant"/>, as <see cref="AdvanceBy"/> does.</summary>
    public void AdvanceTo(DateTimeOffset instant) => Clock.AdvanceTo(instant);

    /// <summary>
    /// Stops the world as a deploy does (drain, flush, close) and starts it again on the same data
    /// directory, same clock and same services: the engine rebuilds from what is on disk. The
    /// captured commits are kept. <see cref="Engine"/> and <see cref="Services"/> are new objects
    /// afterwards.
    /// </summary>
    public void Restart()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        StopHost(_host);
        _host = StartHost();
    }

    /// <summary>
    /// Restarts the world and fails unless every table holds exactly the rows it held before,
    /// byte for byte: the property a world that persists anything must have.
    /// </summary>
    public void RestartAndVerify()
    {
        var before = Dump();
        Restart();
        var after = Dump();
        var lost = before.Except(after).ToList();
        var gained = after.Except(before).ToList();
        if (lost.Count == 0 && gained.Count == 0)
            return;
        throw new MelangeTestFailure(
            $"The world did not survive a restart: {lost.Count} row(s) lost or changed, {gained.Count} appeared." +
            (lost.Count > 0 ? $" First lost: {lost[0]}." : string.Empty) +
            (gained.Count > 0 ? $" First appeared: {gained[0]}." : string.Empty));
    }

    /// <summary>Every row of every table as "table|key|row bytes", sorted: a byte-faithful picture of the world.</summary>
    private List<string> Dump()
    {
        var engine = Engine;
        return [.. engine.Schema.Tables
            .SelectMany(table => engine.HotStore.Scan(table.Id)
                .Select(pair => $"{table.Name}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}"))
            .Order(StringComparer.Ordinal)];
    }

    private IHost StartHost()
    {
        var host = _builder.BuildHost(Root);
        try
        {
            // Observing from before the host starts: lifecycle reducers (Init, and anything a
            // start-up tick commits) are captured too.
            var engine = host.Services.GetRequiredService<MelangeEngine>();
            engine.AddCommitObserver(new Capture(this, engine.Schema));
            host.StartAsync().GetAwaiter().GetResult();
            return host;
        }
        catch
        {
            host.Dispose();
            throw;
        }
    }

    private static void StopHost(IHost host)
    {
        try
        {
            host.StopAsync().GetAwaiter().GetResult();
        }
        finally
        {
            host.Dispose();
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        StopHost(_host);
        try
        {
            Directory.Delete(Root, recursive: true);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // A handle the OS has not released yet; the temp directory is the OS's to reap.
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed class Capture(MelangeWorld world, SchemaRegistry schema) : ICommitObserver
    {
        public void OnCommit(CommitRecord record)
        {
            lock (world._commitsLock)
            {
                world._commits.Add(new WorldCommit(record, schema));
            }
        }
    }
}
