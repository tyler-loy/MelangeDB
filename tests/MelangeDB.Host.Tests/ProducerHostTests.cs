using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// <see cref="MelangeProducerHost"/> (road-to-0.3 phase 26): the call order, the pinned read scope,
/// the limits, the policy ladder, and the rule that a producer writes nothing.
/// </summary>
public class ProducerHostTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-producers-").FullName;

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
    public async Task A_producer_reads_committed_rows_and_returns_its_value_and_encoding()
    {
        using var host = await StartAsync();
        host.Reducers().Call("AddNote", TestApp.Caller, "first", 1.5);

        var result = await host.Producers().CallAsync("Page", TestApp.Caller, 10);

        var page = Assert.IsType<NotePage>(result.Value);
        Assert.Equal("first", Assert.Single(page.Lines).Text);
        Assert.False(result.Payload.IsEmpty);
        Assert.Equal(host.Engine().HotStore.AppliedLsn, result.Lsn);
    }

    [Fact]
    public async Task Every_read_in_one_call_sees_the_lsn_it_pinned_however_many_commits_land_meanwhile()
    {
        using var host = await StartAsync();
        var gate = host.Services.GetRequiredService<ProducerGate>();
        host.Reducers().Call("AddNote", TestApp.Caller, "first", 1.0);

        var call = host.Producers().CallAsync("CountAcrossACommit", TestApp.Caller);
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        host.Reducers().Call("AddNote", TestApp.Caller, "second", 2.0);
        gate.Release.SetResult();
        var result = await call;

        Assert.Equal([1L, 1L], (long[])result.Value!);
        Assert.True(result.Lsn < host.Engine().HotStore.AppliedLsn);
    }

    [Fact]
    public async Task A_call_past_the_timeout_answers_timeout_whether_or_not_the_body_honours_its_token()
    {
        using var host = await StartAsync(new() { ["MelangeDb:Producers:TimeoutSeconds"] = "0.2" });

        await Assert.ThrowsAsync<ProducerTimeoutException>(() => host.Producers().CallAsync("WaitForCancellation", TestApp.Caller));
        var ignored = await Assert.ThrowsAsync<ProducerTimeoutException>(() => host.Producers().CallAsync("IgnoreCancellation", TestApp.Caller));
        Assert.Contains("Producers:TimeoutSeconds", ignored.Message, StringComparison.Ordinal);
        host.Services.GetRequiredService<ProducerGate>().Release.SetResult();
    }

    [Fact]
    public async Task A_result_over_the_response_cap_is_refused()
    {
        using var host = await StartAsync(new() { ["MelangeDb:Producers:MaxResponseBytes"] = "64" });

        Assert.Equal("xx", (string)(await host.Producers().CallAsync("Big", TestApp.Caller, 2)).Value!);
        await Assert.ThrowsAsync<ProducerResultTooLargeException>(() => host.Producers().CallAsync("Big", TestApp.Caller, 1000));
    }

    [Fact]
    public async Task A_producer_cannot_invoke_a_reducer()
    {
        using var host = await StartAsync();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => host.Producers().CallAsync("TryToWrite", TestApp.Caller));
        Assert.Contains("A producer cannot invoke a reducer", failure.Message, StringComparison.Ordinal);
        Assert.Equal(0, host.Engine().CommittedView.Count<Note>());

        // The flag belongs to the producer's flow, not the caller's: a reducer call afterwards still works.
        host.Reducers().Call("AddNote", TestApp.Caller, "after", 1.0);
    }

    [Fact]
    public async Task A_rejection_from_the_body_reaches_the_caller_as_itself()
    {
        using var host = await StartAsync();

        var rejected = await Assert.ThrowsAsync<RejectedException>(() => host.Producers().CallAsync("Refuse", TestApp.Caller));
        Assert.Equal("No.", rejected.Message);
    }

    [Fact]
    public async Task A_client_call_is_authorized_by_the_producers_policy_against_committed_state()
    {
        using var host = await StartAsync();
        var producers = host.Producers();

        await Assert.ThrowsAsync<ReducerDeniedException>(() => ClientCall(producers, "MyNoteCount"));
        host.Reducers().Call("AddNote", TestApp.Caller, "now I am an author", 1.0);
        Assert.Equal(1L, (await ClientCall(producers, "MyNoteCount")).Value);

        // In-process calls are the host's own code: no policy, so a caller with no notes runs and counts none.
        Assert.Equal(0L, (await producers.CallAsync("MyNoteCount", Identity.Hash("someone else"))).Value);
    }

    [Fact]
    public async Task An_unpoliced_producer_is_denied_to_clients_under_a_deny_posture()
    {
        using var host = await StartAsync(new() { ["MelangeDb:Policies:DefaultReducerPosture"] = "Deny" });

        var denied = await Assert.ThrowsAsync<ReducerDeniedException>(() => ClientCall(host.Producers(), "Answer"));
        Assert.Contains("Producer 'Answer' declares no policy", denied.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Client_calls_are_rate_limited_per_identity_and_producer()
    {
        using var host = await StartAsync(new()
        {
            ["MelangeDb:RateLimit:BurstCapacity"] = "2",
            ["MelangeDb:RateLimit:ReducerCallsPerSecond"] = "1",
        });
        var producers = host.Producers();

        await ClientCall(producers, "Answer");
        await ClientCall(producers, "Answer");
        await Assert.ThrowsAsync<RateLimitedException>(() => ClientCall(producers, "Answer"));
    }

    [Fact]
    public async Task Bad_arguments_and_unknown_names_are_refused_before_the_body_runs()
    {
        using var host = await StartAsync();
        var producers = host.Producers();

        await Assert.ThrowsAsync<UnknownProducerException>(() => producers.CallAsync("NoSuchProducer", TestApp.Caller));
        await Assert.ThrowsAsync<ReducerArgumentException>(() => producers.CallAsync("Big", TestApp.Caller, "not an int"));
    }

    [Fact]
    public async Task The_unpoliced_report_lists_producers_and_its_fail_mode_refuses_to_start()
    {
        using (var host = await StartAsync())
            Assert.Contains("Answer", host.Producers().UnpolicedProducers);

        using var failing = TestApp.Build(_root, new Dictionary<string, string?> { ["MelangeDb:Policies:UnpolicedReducerReport"] = "Fail" });
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => failing.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("Answer", failure.Message, StringComparison.Ordinal);
        Assert.Contains("[Producer(Policy = typeof(...))]", failure.Message, StringComparison.Ordinal);
    }

    private static Task<ProducerResult> ClientCall(MelangeProducerHost producers, string name) =>
        producers.CallAsync(name, TestApp.Caller, ConnectionId.None, ReducerArguments.Encode(), source: CallSource.Client());

    private async Task<IHost> StartAsync(Dictionary<string, string?>? settings = null)
    {
        var host = TestApp.Build(_root, settings);
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }
}
