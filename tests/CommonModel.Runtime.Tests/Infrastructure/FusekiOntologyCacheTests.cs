using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class FusekiOntologyCacheTests
{
    // ── Null / empty EndpointUrl skips HTTP ───────────────────────────────────

    [Fact]
    public async Task RefreshAsync_NullEndpointUrl_SkipsHttp_MarksLoaded()
    {
        var sut = MakeSut(null);
        await sut.RefreshAsync();
        (await sut.ContainsAsync("any:iri")).Should().BeFalse();
    }

    [Fact]
    public async Task RefreshAsync_EmptyEndpointUrl_SkipsHttp()
    {
        var sut = MakeSut("");
        await sut.RefreshAsync();
        (await sut.ContainsAsync("any:iri")).Should().BeFalse();
    }

    // ── SPARQL result parsing ─────────────────────────────────────────────────

    [Fact]
    public async Task RefreshAsync_ValidSparqlJson_PopulatesCache()
    {
        var sparqlJson = BuildSparqlResponse([
            ("http://example.org/Pump",  "Pump",  null,                    "owl:Class"),
            ("http://example.org/hasTag","has tag","http://example.org/X", "owl:ObjectProperty")
        ]);

        var sut = MakeSut("http://fuseki.local", sparqlJson);
        await sut.RefreshAsync();

        var pump = await sut.GetByIriAsync("http://example.org/Pump");
        pump.Should().NotBeNull();
        pump!.Label.Should().Be("Pump");
        pump.Type.Should().Be("class");

        var tag = await sut.GetByIriAsync("http://example.org/hasTag");
        tag.Should().NotBeNull();
        tag!.Type.Should().Be("property");
        tag.ParentIri.Should().Be("http://example.org/X");
    }

    [Fact]
    public async Task RefreshAsync_ParsesIndividual_TypeLabel()
    {
        var sparqlJson = BuildSparqlResponse([
            ("http://example.org/Pump1", "Pump1", null, "owl:NamedIndividual")
        ]);

        var sut = MakeSut("http://fuseki.local", sparqlJson);
        await sut.RefreshAsync();

        var entry = await sut.GetByIriAsync("http://example.org/Pump1");
        entry!.Type.Should().Be("individual");
    }

    [Fact]
    public async Task RefreshAsync_SecondRefresh_ClearsPreviousEntries()
    {
        var first  = BuildSparqlResponse([("http://example.org/A", "A", null, "owl:Class")]);
        var second = BuildSparqlResponse([("http://example.org/B", "B", null, "owl:Class")]);

        int call = 0;
        var handler = new DelegatingHandlerStub(_ =>
        {
            var body = call++ == 0 ? first : second;
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json")
            };
        });

        var sut = MakeSutWithHandler("http://fuseki.local", handler);

        await sut.RefreshAsync();
        (await sut.ContainsAsync("http://example.org/A")).Should().BeTrue();

        await sut.RefreshAsync();
        (await sut.ContainsAsync("http://example.org/A")).Should().BeFalse();
        (await sut.ContainsAsync("http://example.org/B")).Should().BeTrue();
    }

    [Fact]
    public async Task FindByLabelAsync_AfterRefresh_IsCaseInsensitive()
    {
        var sparqlJson = BuildSparqlResponse([("http://example.org/Pump", "Pump", null, "owl:Class")]);
        var sut = MakeSut("http://fuseki.local", sparqlJson);
        await sut.RefreshAsync();

        var results = await sut.FindByLabelAsync("pump");
        results.Should().HaveCount(1);
    }

    // ── HTTP failure handling ─────────────────────────────────────────────────

    [Fact]
    public async Task RefreshAsync_HttpFails_CacheRemainsEmpty_DoesNotThrow()
    {
        var handler = new DelegatingHandlerStub(_ =>
            new HttpResponseMessage(HttpStatusCode.InternalServerError));

        var sut = MakeSutWithHandler("http://fuseki.local", handler);

        Func<Task> act = () => sut.RefreshAsync();
        await act.Should().NotThrowAsync();
        (await sut.ContainsAsync("any:iri")).Should().BeFalse();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static FusekiOntologyCache MakeSut(string? endpointUrl, string? sparqlJson = null)
    {
        if (sparqlJson is null)
        {
            return new FusekiOntologyCache(
                Options.Create(new OntologyCacheOptions { EndpointUrl = endpointUrl }),
                new HttpClient(),
                NullLogger<FusekiOntologyCache>.Instance);
        }

        var handler = new DelegatingHandlerStub(_ =>
            new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(sparqlJson, System.Text.Encoding.UTF8, "application/json")
            });

        return MakeSutWithHandler(endpointUrl, handler);
    }

    private static FusekiOntologyCache MakeSutWithHandler(
        string? endpointUrl,
        DelegatingHandler handler) =>
        new(
            Options.Create(new OntologyCacheOptions { EndpointUrl = endpointUrl }),
            new HttpClient(handler) { BaseAddress = null },
            NullLogger<FusekiOntologyCache>.Instance);

    private static string BuildSparqlResponse(
        (string iri, string? label, string? parent, string type)[] rows)
    {
        var bindings = rows.Select(r =>
        {
            var obj = new Dictionary<string, object>
            {
                ["iri"]  = new { type = "uri",     value = r.iri },
                ["type"] = new { type = "uri",     value = r.type }
            };
            if (r.label  is not null) obj["label"]  = new { type = "literal", value = r.label };
            if (r.parent is not null) obj["parent"] = new { type = "uri",     value = r.parent };
            return obj;
        });

        return JsonSerializer.Serialize(new
        {
            results = new { bindings }
        });
    }

    // Minimal DelegatingHandler stub for unit tests
    private sealed class DelegatingHandlerStub : DelegatingHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;
        public DelegatingHandlerStub(Func<HttpRequestMessage, HttpResponseMessage> handler) =>
            _handler = handler;
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(_handler(request));
    }
}
