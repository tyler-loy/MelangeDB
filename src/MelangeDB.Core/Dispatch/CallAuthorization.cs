using Microsoft.Extensions.DependencyInjection;

namespace MelangeDB.Core;

/// <summary>
/// The policy check reducers and producers share: a callable's <see cref="IReducerPolicy"/>,
/// resolved from the call's DI scope and asked with the callable's name, or
/// <c>Policies:DefaultReducerPosture</c> when it declares none.
/// </summary>
internal static class CallAuthorization
{
    public static void Authorize(
        MelangeEngine engine,
        string kind,
        string name,
        Type? policyType,
        Identity caller,
        CallSource source,
        PoliciesOptions policies,
        IServiceProvider services)
    {
        if (policyType is null)
        {
            if (policies.DefaultReducerPosture == ReducerPolicyPosture.Deny)
            {
                throw new ReducerDeniedException(
                    $"{kind} '{name}' declares no policy and Policies:DefaultReducerPosture is Deny.");
            }

            return;
        }

        if (services.GetRequiredService(policyType) is not IReducerPolicy policy)
        {
            throw new InvalidOperationException(
                $"{kind} '{name}': policy type {policyType} does not implement IReducerPolicy.");
        }

        // Evaluated before anything opens, so off the write lock: the policy reads a pinned view
        // rather than the live store, which a commit could be applying to mid-read.
        var allowed = engine.ReadCommitted(db => policy.MayCall(name, new PolicyContext(caller, source.CallerIsGuest, db)));
        if (!allowed)
            throw new ReducerDeniedException($"{kind} '{name}' denied by {policyType.Name}.");
    }
}
