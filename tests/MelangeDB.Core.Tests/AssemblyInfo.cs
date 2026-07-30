using Xunit;

// The engines under test share one process-wide ActivitySource name and Meter name — that is the
// contract being tested — so concurrent test classes would observe each other's signals.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
