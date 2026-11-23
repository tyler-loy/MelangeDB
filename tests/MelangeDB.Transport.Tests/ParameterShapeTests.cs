using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Subscription parameters outside the client contract's closed set of value shapes (C15.2). The
/// server still coerces them, so these subscriptions work; 0.4 logs a warning (EventId 1207) so a
/// deployment can find the caller before 0.5 refuses them.
/// </summary>
public sealed class ParameterShapeTests
{
    private static readonly TableSchema Shapes = BuildSchema();

    [Theory]
    [InlineData("Level", 7L)]
    [InlineData("Level", 7UL)]
    [InlineData("At", 1_700_000_000_000_000L)]
    [InlineData("Name", "Ada")]
    [InlineData("Flag", true)]
    public void A_value_in_the_closed_set_is_not_reported(string column, object value)
    {
        Assert.Null(Find(column, value));
    }

    [Fact]
    public void Bytes_and_a_32_byte_identity_are_in_the_closed_set()
    {
        Assert.Null(Find("Blob", new byte[] { 1, 2, 3 }));
        Assert.Null(Find("Owner", new byte[32]));
    }

    [Fact]
    public void Each_coercion_the_contract_lists_as_wider_is_reported()
    {
        Assert.Equal(("p", "float"), Find("Level", 7.0));
        Assert.Equal(("p", "string"), Find("Level", "7"));
        Assert.Equal(("p", "string"), Find("At", "1700000000000000"));
        Assert.Equal(("p", "string"), Find("Blob", "AQID"));
        Assert.Equal(("p", "string"), Find("Owner", new string('a', 64)));
        Assert.Equal(("p", "bin of 16 bytes"), Find("Owner", new byte[16]));
        Assert.Equal(("p", "bool"), Find("Level", true));
    }

    [Fact]
    public void Literals_in_the_query_text_are_not_parameters()
    {
        var query = SqlSubsetParser.Parse("SELECT * FROM Shapes WHERE Level = 7", null);

        Assert.Empty(query.ParameterNames);
        Assert.Null(ParameterShapes.FindWidened(Shapes, query, null));
    }

    [Fact]
    public void Both_ends_of_a_range_are_checked()
    {
        var query = SqlSubsetParser.Parse(
            "SELECT * FROM Shapes WHERE Level BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 1L, ["hi"] = 9.5 });

        Assert.Equal(["lo", "hi"], query.ParameterNames);
        Assert.Equal(("hi", "float"), ParameterShapes.FindWidened(Shapes, query, new Dictionary<string, object?> { ["lo"] = 1L, ["hi"] = 9.5 }));
    }

    [Fact]
    public async Task A_subscription_with_a_widened_parameter_works_and_is_logged_once()
    {
        var logs = new LogCollector();
        await using var host = await TransportTestHost.StartAsync(services: services => services.AddSingleton<ILoggerProvider>(logs));
        host.Call("SetChunk", 3L, 0L, new byte[] { 1 });
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // A float into the long primary key: coerced, so the row arrives.
        var widened = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE Id = :p",
            new Dictionary<string, object?> { ["p"] = 3.0 },
            TestContext.Current.CancellationToken);
        Assert.Single(widened.Rows);
        await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE Id = :p",
            new Dictionary<string, object?> { ["p"] = 4.0 },
            TestContext.Current.CancellationToken);
        await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE Id = :p",
            new Dictionary<string, object?> { ["p"] = 3L },
            TestContext.Current.CancellationToken);

        var warning = Assert.Single(logs.Entries, entry => entry.EventId == 1207);
        Assert.Equal("ParameterOutsideContract", warning.EventName);
        Assert.Contains("Chunk.Id", warning.Message);
        Assert.Contains("float", warning.Message);
    }

    private static (string Name, string Shape)? Find(string column, object value)
    {
        var parameters = new Dictionary<string, object?> { ["p"] = value };
        var query = SqlSubsetParser.Parse($"SELECT * FROM Shapes WHERE {column} = :p", parameters);
        return ParameterShapes.FindWidened(Shapes, query, parameters);
    }

    private static TableSchema BuildSchema()
    {
        ColumnSchema Column(string name, ColumnKind kind, Type type, bool key = false) => new()
        {
            Name = name,
            ClrType = type,
            Kind = kind,
            IsPrimaryKey = key,
            GetValue = static _ => null,
            SetValue = static (_, _) => { },
        };

        return new TableSchema(
            typeof(object),
            "Shapes",
            [
                Column("Id", ColumnKind.UInt64, typeof(ulong), key: true),
                Column("Level", ColumnKind.Int32, typeof(int)),
                Column("At", ColumnKind.Timestamp, typeof(Timestamp)),
                Column("Name", ColumnKind.String, typeof(string)),
                Column("Flag", ColumnKind.Bool, typeof(bool)),
                Column("Blob", ColumnKind.Bytes, typeof(byte[])),
                Column("Owner", ColumnKind.Identity, typeof(Identity)),
            ],
            isPublic: true);
    }

    private sealed record Entry(int EventId, string? EventName, string Message);

    private sealed class LogCollector : ILoggerProvider
    {
        private readonly List<Entry> _entries = [];

        public IReadOnlyList<Entry> Entries
        {
            get
            {
                lock (_entries)
                {
                    return [.. _entries];
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new Logger(this);

        public void Dispose()
        {
        }

        private sealed class Logger(LogCollector owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                lock (owner._entries)
                {
                    owner._entries.Add(new Entry(eventId.Id, eventId.Name, formatter(state, exception)));
                }
            }
        }
    }
}
