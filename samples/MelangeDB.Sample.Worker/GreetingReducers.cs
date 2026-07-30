using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Sample;

/// <summary>
/// An ordinary DI-resolved class: the options monitor hot-reloads from whatever configuration
/// source the host wired up, and the logger goes wherever the host sends logs. Nothing here is
/// MelangeDB-specific plumbing.
/// </summary>
public sealed class GreetingReducers(
    IOptionsMonitor<GreetingOptions> greeting,
    ILogger<GreetingReducers> logger)
{
    [Reducer]
    public void Greet(ReducerContext ctx, string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new RejectedException("A visitor needs a name.");

        var excited = greeting.CurrentValue.Excited;
        var visitor = ctx.Db.Visitor.Insert(new Visitor
        {
            Name = name,
            VisitedAt = ctx.Timestamp,
            GreetedExcitedly = excited,
        });

        var total = ctx.Db.GreetingTotal.Key.Find(0);
        if (total is { } existing)
            ctx.Db.GreetingTotal.Update(existing with { Count = existing.Count + 1 });
        else
            ctx.Db.GreetingTotal.Insert(new GreetingTotal { Key = 0, Count = 1 });

        if (excited)
            logger.LogInformation("HELLO {Name}!!! You are visitor #{Id}!!!", name, visitor.Id);
        else
            logger.LogInformation("Hello, {Name}. You are visitor #{Id}.", name, visitor.Id);
    }
}
