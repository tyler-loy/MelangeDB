using BenchmarkDotNet.Running;

namespace MelangeDB.Benchmarks;

/// <summary>
/// The benchmark runner. Numbers quoted in the design documents come from here, so that a claim
/// like "identical container memory" is reproducible rather than remembered.
/// <para>
/// <c>dotnet run -c Release --project bench/MelangeDB.Benchmarks</c> lists the suites;
/// <c>--filter '*Container*'</c> runs one. Nothing here runs in CI — these are minutes-long
/// measurements on real hardware, and shared runners would produce numbers not worth recording.
/// </para>
/// </summary>
public static class Program
{
    public static void Main(string[] args) =>
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
