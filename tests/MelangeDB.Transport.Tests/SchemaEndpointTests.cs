using System.Net;
using MelangeDB.Cli;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The development schema endpoint and the exporter that consumes it. The load-bearing property
/// is byte identity: the endpoint, the assembly constant, and the exporter's two source paths all
/// yield the generator's verbatim JSON, so a manifest exported from a DLL and one fetched from a
/// running dev server are the same file.
/// </summary>
public class SchemaEndpointTests
{
    [Fact]
    public async Task Schema_endpoint_is_dark_outside_development_by_default()
    {
        // The test host runs in the Production environment and sets no override.
        await using var host = await TransportTestHost.StartAsync();
        using var http = new HttpClient();
        using var response = await http.GetAsync(new Uri(host.HttpBase, "/melange/schema"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Config_override_turns_the_endpoint_on_outside_development()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:SchemaEndpointEnabled"] = "true",
        });
        using var http = new HttpClient();
        using var response = await http.GetAsync(new Uri(host.HttpBase, "/melange/schema"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.StartsWith("application/json", response.Content.Headers.ContentType?.ToString());

        var served = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        var embedded = SchemaExporter.ReadFromAssembly(typeof(Chunk).Assembly);
        Assert.Equal(embedded, served);
        Assert.Contains("\"Chunk\"", served);
        Assert.DoesNotContain("SecretTable", served);
    }

    [Fact]
    public async Task Config_override_false_keeps_the_endpoint_dark()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:SchemaEndpointEnabled"] = "false",
        });
        using var http = new HttpClient();
        using var response = await http.GetAsync(new Uri(host.HttpBase, "/melange/schema"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Exporter_fetch_and_assembly_read_are_byte_identical()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:SchemaEndpointEnabled"] = "true",
        });

        // A bare base URL exercises the exporter's default-path append — the workflow the tool
        // documents: point it at the running local dev server, get melange-schema.json.
        var fetched = await SchemaExporter.FetchAsync(host.HttpBase, TestContext.Current.CancellationToken);
        var read = SchemaExporter.ReadFromAssembly(typeof(Chunk).Assembly);
        Assert.Equal(read, fetched);
    }

    [Fact]
    public async Task Served_manifest_carries_its_hash_as_the_etag()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:SchemaEndpointEnabled"] = "true",
        });
        using var http = new HttpClient();
        using var response = await http.GetAsync(new Uri(host.HttpBase, "/melange/schema"), TestContext.Current.CancellationToken);
        var etag = Assert.Single(response.Headers.GetValues("ETag")).Trim('"');
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains($"\"schemaHash\": \"{etag}\"", body);
    }
}
