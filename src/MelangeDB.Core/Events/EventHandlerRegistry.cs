using System.Reflection;

namespace MelangeDB.Core;

/// <summary>
/// The registered event handlers. Each handler <em>type</em> is one logical subscriber: it owns
/// one durable checkpoint under its type's full name, which is why renaming a handler class is a
/// new subscriber that starts from current state.
/// </summary>
public sealed class EventHandlerRegistry
{
    public EventHandlerRegistry(IEnumerable<Type> handlerTypes)
    {
        ArgumentNullException.ThrowIfNull(handlerTypes);
        var handlers = new List<EventHandlerRegistration>();
        foreach (var type in handlerTypes.Distinct())
            handlers.Add(new EventHandlerRegistration(type));
        Handlers = handlers;
    }

    /// <summary>One registration per handler type, in registration order.</summary>
    public IReadOnlyList<EventHandlerRegistration> Handlers { get; }
}

/// <summary>One handler type and the event types it subscribes to.</summary>
public sealed class EventHandlerRegistration
{
    private readonly Dictionary<string, EventBinding> _bindings;

    internal EventHandlerRegistration(Type handlerType)
    {
        ArgumentNullException.ThrowIfNull(handlerType);
        HandlerType = handlerType;
        Name = handlerType.FullName
            ?? throw new ArgumentException("Handler type has no full name.", nameof(handlerType));

        _bindings = new Dictionary<string, EventBinding>(StringComparer.Ordinal);
        foreach (var contract in handlerType.GetInterfaces())
        {
            if (!contract.IsGenericType || contract.GetGenericTypeDefinition() != typeof(IEventHandler<>))
                continue;
            var eventType = contract.GetGenericArguments()[0];
            var handle = contract.GetMethod(nameof(IEventHandler<>.HandleAsync))!;
            _bindings[eventType.FullName!] = new EventBinding(eventType, handle);
        }

        if (_bindings.Count == 0)
        {
            throw new ArgumentException(
                $"Type '{handlerType}' implements no IEventHandler<TEvent> interface.", nameof(handlerType));
        }
    }

    /// <summary>The subscriber's durable identity: the handler type's full name.</summary>
    public string Name { get; }

    /// <summary>The DI-resolved handler type.</summary>
    public Type HandlerType { get; }

    /// <summary>The event type names this handler subscribes to.</summary>
    public IReadOnlyCollection<string> EventTypeNames => _bindings.Keys;

    internal bool TryGetBinding(string eventTypeName, out EventBinding binding) =>
        _bindings.TryGetValue(eventTypeName, out binding);

    internal readonly struct EventBinding(Type eventType, MethodInfo handleAsync)
    {
        public Type EventType { get; } = eventType;

        public Task Invoke(object handler, object @event, CancellationToken cancellationToken)
        {
            try
            {
                return (Task)handleAsync.Invoke(handler, [@event, cancellationToken])!;
            }
            catch (TargetInvocationException wrapped) when (wrapped.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(wrapped.InnerException).Throw();
                throw;
            }
        }
    }
}
