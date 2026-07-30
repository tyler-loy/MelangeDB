using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

public class HostIntegrationTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-host-").FullName;

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
    public async Task Reducer_resolves_from_a_fresh_scope_per_call_with_singleton_and_scoped_services()
    {
        using var host = TestApp.Build(_root);
        await host.StartAsync(TestContext.Current.CancellationToken);

        var reducers = host.Reducers();
        reducers.Call("AddNote", TestApp.Caller, "first", 1.5);
        reducers.Call("AddNote", TestApp.Caller, "second", 2.5);

        var singleton = host.Services.GetRequiredService<SingletonProbe>();
        Assert.Equal(2, singleton.Scopes.Count);
        var scopes = singleton.Scopes.ToArray();
        Assert.NotEqual(scopes[0].Id, scopes[1].Id);
        Assert.All(scopes, probe => Assert.True(probe.Disposed, "The call's scope must be disposed when the call returns."));

        var engine = host.Engine();
        Assert.Equal(2UL, engine.Log.HeadLsn);
        engine.Invoke("Verify", TestApp.Caller, ctx =>
        {
            Assert.Equal(2, ctx.Db.Note.Iter().Count());
            Assert.Equal(2, ctx.Db.Audit.Iter().Count());
        });

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Array_arguments_decode_and_rejections_pass_through()
    {
        using var host = TestApp.Build(_root);
        await host.StartAsync(TestContext.Current.CancellationToken);

        var reducers = host.Reducers();
        reducers.Call("AddMany", TestApp.Caller, new[] { "a", "b", "c" }, new byte[] { 1, 2 });
        host.Engine().Invoke("Verify", TestApp.Caller, ctx => Assert.Equal(3, ctx.Db.Note.Iter().Count()));

        host.ReloadWith("Feature:Enabled", "false");
        Assert.Throws<RejectedException>(() => reducers.Call("AddNote", TestApp.Caller, "rejected", 0.0));

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Options_bind_from_configuration_and_builder_code_wins()
    {
        using (var host = TestApp.Build(_root, new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:FsyncPolicy"] = "OsBuffered",
            ["MelangeDb:Telemetry:SlowReducerMs"] = "125",
        }))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(FsyncPolicy.OsBuffered, host.Engine().Options.CommitLog.FsyncPolicy);
            Assert.Equal(125, host.Engine().Options.Telemetry.SlowReducerMs);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        using (var host = TestApp.Build(
            Path.Combine(_root, "second"),
            new Dictionary<string, string?> { ["MelangeDb:CommitLog:FsyncPolicy"] = "OsBuffered" },
            builder => builder.Services.AddMelangeDb(melange =>
                melange.UseCommitLog(o => o.FsyncPolicy = FsyncPolicy.Interval))))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(FsyncPolicy.Interval, host.Engine().Options.CommitLog.FsyncPolicy);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Live_config_change_alters_reducer_behaviour_without_restart()
    {
        using var host = TestApp.Build(_root, new Dictionary<string, string?> { ["Feature:Enabled"] = "false" });
        await host.StartAsync(TestContext.Current.CancellationToken);
        var reducers = host.Reducers();

        Assert.Throws<RejectedException>(() => reducers.Call("AddNote", TestApp.Caller, "off", 0.0));

        // The feature-flag scenario: one config change, no restart, next invocation behaves differently.
        host.ReloadWith("Feature:Enabled", "true");
        reducers.Call("AddNote", TestApp.Caller, "on", 0.0);
        host.Engine().Invoke("Verify", TestApp.Caller, ctx => Assert.Single(ctx.Db.Note.Iter()));

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Live_melange_keys_reach_the_running_engine_without_restart()
    {
        using var host = TestApp.Build(_root);
        await host.StartAsync(TestContext.Current.CancellationToken);
        var engine = host.Engine();
        Assert.Equal(FsyncPolicy.OnCommit, engine.Options.CommitLog.FsyncPolicy);
        Assert.Equal(50, engine.Options.Telemetry.SlowReducerMs);

        host.ReloadWith("MelangeDb:CommitLog:FsyncPolicy", "OsBuffered");
        host.ReloadWith("MelangeDb:Telemetry:SlowReducerMs", "999");

        Assert.Equal(FsyncPolicy.OsBuffered, engine.Options.CommitLog.FsyncPolicy);
        Assert.Equal(999, engine.Options.Telemetry.SlowReducerMs);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Hosted_service_recovers_state_across_restart()
    {
        using (var host = TestApp.Build(_root))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            host.Reducers().Call("AddNote", TestApp.Caller, "durable", 4.0);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        using (var host = TestApp.Build(_root))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            host.Engine().Invoke("Verify", TestApp.Caller, ctx =>
            {
                var note = Assert.Single(ctx.Db.Note.Iter());
                Assert.Equal("durable", note.Text);
            });
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Stopping_host_drains_and_rejects_further_calls()
    {
        using var host = TestApp.Build(_root);
        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Reducers().Call("AddNote", TestApp.Caller, "before-stop", 0.0);
        await host.StopAsync(TestContext.Current.CancellationToken);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            host.Reducers().Call("AddNote", TestApp.Caller, "after-stop", 0.0));
        Assert.Contains("shutting down", exception.Message);
    }

    [Fact]
    public async Task Encoded_arguments_reach_the_reducer_span_when_opted_in()
    {
        using var host = TestApp.Build(_root, new Dictionary<string, string?>
        {
            ["MelangeDb:Telemetry:IncludeReducerArguments"] = "true",
        });
        await host.StartAsync(TestContext.Current.CancellationToken);

        var captured = new List<string>();
        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = source => source.Name == "MelangeDB",
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) =>
                System.Diagnostics.ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "melange.reducer" && activity.GetTagItem("melange.reducer.args") is string args)
                {
                    lock (captured)
                    {
                        captured.Add(args);
                    }
                }
            },
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);

        var text = $"traced-{Guid.NewGuid():N}";
        var expected = Convert.ToHexStringLower(ReducerArguments.Encode(text, 1.0));
        host.Reducers().Call("AddNote", TestApp.Caller, text, 1.0);

        lock (captured)
        {
            Assert.Contains(expected, captured);
        }

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Unknown_reducer_is_rejected_by_name()
    {
        using var host = TestApp.Build(_root);
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.Throws<ArgumentException>(() => host.Reducers().Call("Nonexistent", TestApp.Caller));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
