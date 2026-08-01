using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace MelangeDB.Core;

/// <summary>
/// The configuration surface of <c>AddMelangeDb</c>. Everything here is sugar over the options
/// the <c>MelangeDb:</c> configuration section binds — code wins over file only because it runs
/// last — plus registration of generated models discovered per assembly.
/// </summary>
public sealed class MelangeDbBuilder
{
    private readonly HashSet<Assembly> _tableAssemblies = [];
    private readonly HashSet<Assembly> _reducerAssemblies = [];
    private readonly HashSet<Assembly> _handlerAssemblies = [];
    private readonly List<TableSchema> _tables = [];
    private readonly List<ReducerDescriptor> _reducers = [];
    private readonly List<Type> _eventHandlers = [];
    private readonly SchemaManifests _manifests = new();

    internal MelangeDbBuilder(IServiceCollection services) => Services = services;

    /// <summary>The service collection MelangeDB is being added to.</summary>
    public IServiceCollection Services { get; }

    internal IReadOnlyList<TableSchema> Tables => _tables;

    internal IReadOnlyList<ReducerDescriptor> Reducers => _reducers;

    internal IReadOnlyList<Type> EventHandlers => _eventHandlers;

    internal SchemaManifests Manifests => _manifests;

    /// <summary>Configures the hot store. Runs after configuration binding, so code wins.</summary>
    public MelangeDbBuilder UseHotStore(Action<HotStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Services.Configure<MelangeDbOptions>(options => configure(options.HotStore));
        return this;
    }

    /// <summary>Configures the commit log. Runs after configuration binding, so code wins.</summary>
    public MelangeDbBuilder UseCommitLog(Action<CommitLogOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Services.Configure<MelangeDbOptions>(options => configure(options.CommitLog));
        return this;
    }

    /// <summary>
    /// Registers every <c>[Table]</c> struct in <paramref name="assembly"/> through its generated
    /// model. A table added to the assembly later needs no further registration anywhere.
    /// </summary>
    public MelangeDbBuilder AddTablesFrom(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        if (_tableAssemblies.Add(assembly))
        {
            _tables.AddRange(ModelOf(assembly).Tables());
            _manifests.AddFrom(assembly);
        }

        return this;
    }

    /// <summary>
    /// Registers every <c>[Reducer]</c> method in <paramref name="assembly"/> through its generated
    /// model, and its declaring classes as scoped services if not already registered.
    /// </summary>
    public MelangeDbBuilder AddReducersFrom(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        if (_reducerAssemblies.Add(assembly))
        {
            _reducers.AddRange(ModelOf(assembly).Reducers());
            _manifests.AddFrom(assembly);
        }

        return this;
    }

    /// <summary>
    /// Registers one event handler type. The type becomes a logical subscriber under its full
    /// name, with a durable checkpoint; it is resolved from a fresh DI scope per delivery.
    /// </summary>
    public MelangeDbBuilder AddEventHandler<THandler>()
        where THandler : class
    {
        if (!_eventHandlers.Contains(typeof(THandler)))
            _eventHandlers.Add(typeof(THandler));
        return this;
    }

    /// <summary>
    /// Registers every concrete <see cref="IEventHandler{TEvent}"/> implementation in
    /// <paramref name="assembly"/> as an event handler.
    /// </summary>
    public MelangeDbBuilder AddEventHandlersFrom(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        if (!_handlerAssemblies.Add(assembly))
            return this;
        foreach (var type in assembly.GetTypes())
        {
            if (type is { IsAbstract: false, IsClass: true }
                && type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>))
                && !_eventHandlers.Contains(type))
            {
                _eventHandlers.Add(type);
            }
        }

        return this;
    }

    private static IMelangeModel ModelOf(Assembly assembly)
    {
        var attribute = assembly.GetCustomAttribute<MelangeGeneratedModelAttribute>()
            ?? throw new InvalidOperationException(
                $"Assembly '{assembly.GetName().Name}' carries no generated MelangeDB model. " +
                "Reference the MelangeDB.CodeGen analyzer so tables and reducers are discovered at compile time.");
        return (IMelangeModel)Activator.CreateInstance(attribute.ModelType)!;
    }
}
