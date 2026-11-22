using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Text.Json.Nodes;
using MelangeDB.Client;
using MelangeDB.Protocol;

namespace MelangeDB.Conformance.Adapter.CSharp;

/// <summary>
/// The C# client under the adapter protocol (docs/CLIENT-CONFORMANCE.md, "The adapter protocol"):
/// one request per input line, one response per request, and cache events as they fire, all as
/// newline-delimited JSON. Every table is driven through the typed bindings generated from the
/// conformance manifest, because the merged, refcounted cache the suite tests lives only there.
/// Requests run concurrently; a response or event is written whole, under one lock, so the output
/// is one ordered stream — the property the suite's <c>sync</c> barrier rests on.
/// </summary>
public sealed class CSharpAdapter
{
    private static readonly MessagePackFrameSerializer Serializer = new();

    private readonly TextWriter _output;
    private readonly Lock _writeLock = new();
    private readonly ConcurrentDictionary<int, ISubscriptionHandle> _subscriptions = new();
    private readonly Dictionary<string, ITableBinding> _tables;
    private MelangeClient? _client;
    private int _nextSubscription;

    private CSharpAdapter(TextWriter output)
    {
        _output = output;
        _tables = DiscoverTables();
    }

    /// <summary>Serves the protocol until <paramref name="input"/> ends.</summary>
    public static async Task RunAsync(TextReader input, TextWriter output, CancellationToken cancellationToken = default)
    {
        var adapter = new CSharpAdapter(output);
        var running = new List<Task>();
        while (await input.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (line.Length == 0)
                continue;
            var request = JsonNode.Parse(line)!.AsObject();
            running.Add(Task.Run(() => adapter.ServeAsync(request), cancellationToken));
        }

        await Task.WhenAll(running).ConfigureAwait(false);
        if (adapter._client is { } client)
            await client.DisposeAsync().ConfigureAwait(false);
    }

    private async Task ServeAsync(JsonObject request)
    {
        var id = (int)request["id"]!;
        JsonObject response;
        try
        {
            response = await HandleAsync((string)request["op"]!, request).ConfigureAwait(false);
            response.Insert(0, "id", id);
            response.Insert(1, "ok", true);
        }
        catch (Exception exception)
        {
            var (code, message) = exception switch
            {
                MelangeCallException call => (call.Code, call.Message),
                MelangeSubscriptionException subscription => (subscription.Code, subscription.Message),
                MelangeSchemaMismatchException mismatch => ("schema_mismatch", mismatch.Message),
                _ => ("adapter", $"{exception.GetType().Name}: {exception.Message}"),
            };
            response = new JsonObject
            {
                ["id"] = id,
                ["ok"] = false,
                ["error"] = new JsonObject { ["code"] = code, ["message"] = message },
            };
        }

        Write(response);
    }

    private async Task<JsonObject> HandleAsync(string op, JsonObject request)
    {
        switch (op)
        {
            case "hello":
                return new JsonObject { ["adapter"] = "MelangeDB.Client", ["language"] = "C#" };

            case "decodeFrames":
                {
                    var descriptors = new Dictionary<uint, WireDescriptor>();
                    var decoded = new JsonArray();
                    foreach (var hex in request["frames"]!.AsArray())
                        decoded.Add(CorpusJson.Frame(Serializer.Deserialize(Convert.FromHexString((string)hex!)), descriptors));
                    return new JsonObject { ["decoded"] = decoded };
                }

            case "decodeColumn":
                {
                    var bytes = Convert.FromHexString((string)request["bytes"]!);
                    var reader = new RowReader(bytes);
                    var value = CorpusJson.Column(ref reader, (ColumnKind)(int)request["kind"]!);
                    return new JsonObject { ["value"] = value, ["consumed"] = reader.Position };
                }

            case "encodeKey":
                return new JsonObject { ["key"] = CorpusJson.Hex(EncodeKey((ColumnKind)(int)request["kind"]!, request["value"]).Span) };

            case "encodeArguments":
                return new JsonObject { ["bytes"] = CorpusJson.Hex(ReducerArgs.Encode(Arguments(request["arguments"]))) };

            case "connect":
                {
                    if (_client is { } previous)
                        await previous.DisposeAsync().ConfigureAwait(false);
                    _subscriptions.Clear();
                    var client = new MelangeClient(new MelangeClientOptions
                    {
                        Uri = new Uri((string)request["url"]!),
                        Token = (string)request["token"]!,
                    });
                    client.OnError += error => Write(new JsonObject
                    {
                        ["event"] = "error",
                        ["code"] = error.Code,
                        ["message"] = error.Message,
                    });
                    client.OnDisconnected += () => Write(new JsonObject { ["event"] = "disconnected" });
                    await client.ConnectAsync().ConfigureAwait(false);
                    var registry = new ClientCacheRegistry(client);
                    foreach (var table in _tables.Values)
                        table.Attach(registry, Write);
                    _client = client;
                    return new JsonObject
                    {
                        ["identity"] = client.Identity.ToString(),
                        ["protocolVersion"] = client.ProtocolVersion,
                    };
                }

            case "subscribe":
                {
                    var table = _tables.TryGetValue((string)request["table"]!, out var found)
                        ? found
                        : throw new InvalidOperationException($"No generated bindings for table '{request["table"]}'.");
                    var handle = await table.SubscribeAsync((string)request["query"]!, Parameters(request["parameters"])).ConfigureAwait(false);
                    var subscriptionId = Interlocked.Increment(ref _nextSubscription);
                    _subscriptions[subscriptionId] = handle;
                    return new JsonObject { ["subscription"] = subscriptionId };
                }

            case "rescope":
                await Subscription(request).RescopeAsync(Parameters(request["parameters"])!).ConfigureAwait(false);
                return [];

            case "unsubscribe":
                await Subscription(request).UnsubscribeAsync().ConfigureAwait(false);
                _subscriptions.TryRemove((int)request["subscription"]!, out _);
                return [];

            case "call":
                {
                    var lsn = await Client.CallReducerAsync((string)request["reducer"]!, Arguments(request["arguments"])).ConfigureAwait(false);
                    return new JsonObject { ["lsn"] = CorpusJson.U64(lsn) };
                }

            case "callProducer":
                {
                    var reply = await Client.CallProducerAsync((string)request["producer"]!, Arguments(request["arguments"])).ConfigureAwait(false);
                    return new JsonObject { ["lsn"] = CorpusJson.U64(reply.Lsn), ["value"] = CorpusJson.TaggedValue(reply.Payload.Span) };
                }

            case "sync":
                {
                    var target = ulong.Parse((string)request["lsn"]!, CultureInfo.InvariantCulture);
                    var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
                    while (Client.LastAckedLsn < target)
                    {
                        if (DateTime.UtcNow > deadline)
                            throw new TimeoutException($"The cursor stayed at {Client.LastAckedLsn}, below {target}.");
                        await Task.Delay(5).ConfigureAwait(false);
                    }

                    return new JsonObject { ["cursor"] = CorpusJson.U64(Client.LastAckedLsn) };
                }

            case "cache":
                {
                    var table = _tables[(string)request["table"]!];
                    return new JsonObject { ["rows"] = table.Rows() };
                }

            case "drop":
                Client.Abort();
                return [];

            case "reconnect":
                return new JsonObject { ["resumed"] = await Client.ReconnectAsync().ConfigureAwait(false) };

            case "reauthenticate":
                await Client.ReauthenticateAsync((string)request["token"]!).ConfigureAwait(false);
                return [];

            case "close":
                if (_client is { } open)
                {
                    _client = null;
                    await open.DisposeAsync().ConfigureAwait(false);
                }

                return [];

            default:
                throw new NotSupportedException($"Unknown op '{op}'.");
        }
    }

    private MelangeClient Client => _client ?? throw new InvalidOperationException("Not connected.");

    private ISubscriptionHandle Subscription(JsonObject request) =>
        _subscriptions.TryGetValue((int)request["subscription"]!, out var handle)
            ? handle
            : throw new InvalidOperationException($"No subscription {request["subscription"]}.");

    private void Write(JsonObject message)
    {
        var line = message.ToJsonString();
        lock (_writeLock)
        {
            _output.WriteLine(line);
            _output.Flush();
        }
    }

    /// <summary>Every table the generated bindings know, found by its generated row codec.</summary>
    private static Dictionary<string, ITableBinding> DiscoverTables()
    {
        var tables = new Dictionary<string, ITableBinding>(StringComparer.Ordinal);
        foreach (var type in typeof(CSharpAdapter).Assembly.GetTypes())
        {
            var codecInterface = type.GetInterfaces().FirstOrDefault(static i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IClientRowCodec<>));
            if (codecInterface is null)
                continue;
            var codec = type.GetField("Instance", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
            var binding = (ITableBinding)Activator.CreateInstance(
                typeof(TableBinding<>).MakeGenericType(codecInterface.GetGenericArguments()[0]), codec)!;
            tables[binding.Name] = binding;
        }

        return tables;
    }

    /// <summary>Tagged values (the corpus's argument form) as the CLR values the client encodes.</summary>
    internal static object? Value(JsonNode? tagged)
    {
        var tag = (string)tagged!["tag"]!;
        var value = tagged["value"];
        return tag switch
        {
            "Null" => null,
            "Bool" => (bool)value!,
            "Int64" => long.Parse((string)value!, CultureInfo.InvariantCulture),
            "UInt64" => ulong.Parse((string)value!, CultureInfo.InvariantCulture),
            "Float64" => BitConverter.UInt64BitsToDouble(ulong.Parse((string)value!, NumberStyles.HexNumber, CultureInfo.InvariantCulture)),
            "String" => (string)value!,
            "Bytes" => Convert.FromHexString((string)value!),
            "Identity" => new Identity(Convert.FromHexString((string)value!)),
            "Timestamp" => new Timestamp(long.Parse((string)value!, CultureInfo.InvariantCulture)),
            "Array" => tagged["items"]!.AsArray().Select(Value).ToArray(),
            _ => throw new NotSupportedException($"Unknown tag '{tag}'."),
        };
    }

    private static object?[] Arguments(JsonNode? arguments) =>
        arguments is null ? [] : [.. arguments.AsArray().Select(Value)];

    private static Dictionary<string, object?>? Parameters(JsonNode? parameters) =>
        parameters is not JsonObject map ? null : map.ToDictionary(static p => p.Key, static p => Value(p.Value), StringComparer.Ordinal);

    private static RowKey EncodeKey(ColumnKind kind, JsonNode? value) => kind switch
    {
        ColumnKind.Bool => KeyCodec.EncodeBool((bool)value!),
        ColumnKind.Int8 => KeyCodec.EncodeInt8((sbyte)(int)value!),
        ColumnKind.UInt8 => KeyCodec.EncodeUInt8((byte)(int)value!),
        ColumnKind.Int16 => KeyCodec.EncodeInt16((short)(int)value!),
        ColumnKind.UInt16 => KeyCodec.EncodeUInt16((ushort)(int)value!),
        ColumnKind.Int32 => KeyCodec.EncodeInt32((int)value!),
        ColumnKind.UInt32 => KeyCodec.EncodeUInt32((uint)value!),
        ColumnKind.Int64 => KeyCodec.EncodeInt64(long.Parse((string)value!, CultureInfo.InvariantCulture)),
        ColumnKind.UInt64 => KeyCodec.EncodeUInt64(ulong.Parse((string)value!, CultureInfo.InvariantCulture)),
        ColumnKind.String => KeyCodec.EncodeString((string)value!),
        ColumnKind.Bytes => KeyCodec.EncodeBytes(Convert.FromHexString((string)value!)),
        ColumnKind.Identity => KeyCodec.EncodeIdentity(new Identity(Convert.FromHexString((string)value!))),
        ColumnKind.Timestamp => KeyCodec.EncodeTimestamp(new Timestamp(long.Parse((string)value!, CultureInfo.InvariantCulture))),
        _ => throw new NotSupportedException($"Kind {kind} is not key-encodable."),
    };

    private interface ISubscriptionHandle
    {
        Task RescopeAsync(IReadOnlyDictionary<string, object?> parameters);

        Task UnsubscribeAsync();
    }

    private interface ITableBinding
    {
        string Name { get; }

        void Attach(ClientCacheRegistry registry, Action<JsonObject> emit);

        Task<ISubscriptionHandle> SubscribeAsync(string query, IReadOnlyDictionary<string, object?>? parameters);

        JsonArray Rows();
    }

    private sealed class TableBinding<TRow>(IClientRowCodec<TRow> codec) : ITableBinding
        where TRow : struct
    {
        private ClientCacheRegistry? _registry;
        private ClientCache<TRow>? _cache;

        public string Name => codec.TableName;

        public void Attach(ClientCacheRegistry registry, Action<JsonObject> emit)
        {
            _registry = registry;
            _cache = registry.GetOrAdd(codec);
            _cache.OnInsert += row => emit(new JsonObject { ["event"] = "insert", ["table"] = Name, ["row"] = Render(row) });
            _cache.OnUpdate += (old, now) => emit(new JsonObject { ["event"] = "update", ["table"] = Name, ["old"] = Render(old), ["new"] = Render(now) });
            _cache.OnDelete += row => emit(new JsonObject { ["event"] = "delete", ["table"] = Name, ["row"] = Render(row) });
        }

        public async Task<ISubscriptionHandle> SubscribeAsync(string query, IReadOnlyDictionary<string, object?>? parameters) =>
            new Handle(await _registry!.SubscribeAsync(codec, query, parameters).ConfigureAwait(false));

        public JsonArray Rows() => [.. _cache!.Rows.Select(row => (JsonNode)Render(row))];

        /// <summary>A typed row in the corpus's canonical form, column by column in descriptor order.</summary>
        private JsonObject Render(TRow row)
        {
            var columns = new JsonObject();
            object boxed = row;
            foreach (var column in codec.Columns)
            {
                var value = typeof(TRow).GetField(column.Name)!.GetValue(boxed);
                columns[column.Name] = RenderValue(column.Kind, value);
            }

            return columns;
        }

        private static JsonNode? RenderValue(ColumnKind kind, object? value)
        {
            if (value is Enum)
                value = Convert.ChangeType(value, Enum.GetUnderlyingType(value.GetType()), CultureInfo.InvariantCulture);
            return kind switch
            {
                ColumnKind.Bool => (bool)value!,
                ColumnKind.Int8 or ColumnKind.UInt8 or ColumnKind.Int16 or ColumnKind.UInt16 or ColumnKind.Int32 or ColumnKind.UInt32 =>
                    Convert.ToInt64(value, CultureInfo.InvariantCulture),
                ColumnKind.Int64 => CorpusJson.I64((long)value!),
                ColumnKind.UInt64 => CorpusJson.U64((ulong)value!),
                ColumnKind.Float32 => CorpusJson.F32((float)value!),
                ColumnKind.Float64 => CorpusJson.F64((double)value!),
                ColumnKind.String => (string?)value,
                ColumnKind.Bytes => value is byte[] bytes ? CorpusJson.Hex(bytes) : null,
                ColumnKind.Identity => ((Identity)value!).ToString(),
                ColumnKind.Timestamp => CorpusJson.I64(((Timestamp)value!).UnixTimeMicroseconds),
                ColumnKind.ScheduleAt => CorpusJson.ScheduleAtValue((ScheduleAt)value!),
                _ => throw new NotSupportedException($"Unknown kind {kind}."),
            };
        }

        private sealed class Handle(TypedSubscription<TRow> subscription) : ISubscriptionHandle
        {
            public Task RescopeAsync(IReadOnlyDictionary<string, object?> parameters) => subscription.RescopeAsync(parameters);

            public Task UnsubscribeAsync() => subscription.UnsubscribeAsync();
        }
    }
}
