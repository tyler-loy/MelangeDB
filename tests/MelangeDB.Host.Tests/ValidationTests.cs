using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// Argument validation happens during decode, before any transaction opens: a rejected call
/// appends no log record, creates no scope, and never reaches the reducer body.
/// </summary>
public class ValidationTests : IAsyncLifetime
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-validate-").FullName;
    private IHost _host = null!;

    public async ValueTask InitializeAsync()
    {
        _host = TestApp.Build(_root);
        await _host.StartAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void Non_finite_double_is_rejected_during_decode(double poison) =>
        AssertRejected("AddNote", "position", poison);

    [Theory]
    [InlineData(float.NaN)]
    [InlineData(float.PositiveInfinity)]
    public void Non_finite_float_is_rejected_during_decode(float poison) =>
        AssertRejected("Clamp", 1, poison);

    [Fact]
    public void Finite_double_overflowing_a_declared_float_is_rejected_during_decode()
    {
        // 1e39 is a perfectly finite double, but narrowing it to the declared float parameter
        // mints PositiveInfinity — the exact poison RejectNonFiniteFloats exists to stop.
        AssertRejected("Clamp", 1, 1e39);
    }

    [Fact]
    public void The_same_large_value_is_accepted_for_a_declared_double()
    {
        _host.Reducers().Call("AddNote", TestApp.Caller, "big-but-finite", 1e39);
    }

    [Fact]
    public void Over_long_string_is_rejected_during_decode()
    {
        AssertRejected("AddNote", new string('x', 5000), 1.0);
    }

    [Fact]
    public void Over_long_array_is_rejected_during_decode() =>
        AssertRejected("AddMany", Enumerable.Repeat("a", 5000).ToArray(), new byte[] { 1 });

    [Fact]
    public void Over_long_blob_is_rejected_during_decode() =>
        AssertRejected("AddMany", new[] { "a" }, new byte[5000]);

    [Fact]
    public void Out_of_range_integer_for_declared_type_is_rejected_during_decode() =>
        AssertRejected("Clamp", 5_000_000_000L, 1f);

    [Fact]
    public void Wrong_argument_count_is_rejected_during_decode() =>
        AssertRejected("AddNote", "only-one");

    [Fact]
    public void Validation_caps_are_live_configuration()
    {
        _host.Reducers().Call("AddNote", TestApp.Caller, new string('x', 100), 1.0);
        _host.ReloadWith("MelangeDb:Validation:MaxStringLength", "8");
        AssertRejected("AddNote", new string('x', 100), 1.0);

        _host.ReloadWith("MelangeDb:Validation:MaxStringLength", "4096");
        _host.Reducers().Call("AddNote", TestApp.Caller, new string('x', 100), 1.0);
    }

    [Fact]
    public void Non_finite_floats_can_be_admitted_only_by_explicit_opt_out()
    {
        AssertRejected("Clamp", 1, float.NaN);
        _host.ReloadWith("MelangeDb:Validation:RejectNonFiniteFloats", "false");
        _host.Reducers().Call("Clamp", TestApp.Caller, 1, float.NaN);
        _host.ReloadWith("MelangeDb:Validation:RejectNonFiniteFloats", "true");
    }

    private void AssertRejected(string reducer, params object?[] args)
    {
        var engine = _host.Engine();
        var headBefore = engine.Log.HeadLsn;
        var scopesBefore = _host.Services.GetRequiredService<SingletonProbe>().Scopes.Count;

        Assert.Throws<ReducerArgumentException>(() => _host.Reducers().Call(reducer, TestApp.Caller, args));

        // No log record, and the reducer body never ran — no transaction was opened.
        Assert.Equal(headBefore, engine.Log.HeadLsn);
        Assert.Equal(scopesBefore, _host.Services.GetRequiredService<SingletonProbe>().Scopes.Count);
    }
}
