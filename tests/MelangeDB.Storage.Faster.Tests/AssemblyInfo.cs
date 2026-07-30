using Xunit;

// The memory-bound and timing assertions measure process-wide state (working set, wall clock), so
// concurrent test classes would perturb each other's measurements.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
