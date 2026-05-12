using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class FusekiOntologyCache : IOntologyCache
{
    private readonly OntologyCacheOptions _options;
    private readonly HttpClient _http;
    private readonly ILogger<FusekiOntologyCache> _logger;

    private readonly ConcurrentDictionary<string, OntologyEntry> _byIri  = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, List<OntologyEntry>> _byLabel =
        new(StringComparer.OrdinalIgnoreCase);

    private volatile bool _loaded;

    public FusekiOntologyCache(
        IOptions<OntologyCacheOptions> options,
        HttpClient http,
        ILogger<FusekiOntologyCache> logger)
    {
        _options = options.Value;
        _http    = http;
        _logger  = logger;
    }

    public async Task<OntologyEntry?> GetByIriAsync(string iri, CancellationToken ct = default)
    {
        await EnsureLoadedAsync(ct);
        _byIri.TryGetValue(iri, out var entry);
        return entry;
    }

    public async Task<IReadOnlyList<OntologyEntry>> FindByLabelAsync(string label, CancellationToken ct = default)
    {
        await EnsureLoadedAsync(ct);
        return _byLabel.TryGetValue(label, out var list)
            ? list
            : Array.Empty<OntologyEntry>();
    }

    public Task<bool> ContainsAsync(string iri, CancellationToken ct = default) =>
        Task.FromResult(_byIri.ContainsKey(iri));

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        if (_options.EndpointUrl is null or "")
        {
            _logger.LogDebug("OntologyCache: EndpointUrl not configured — skipping Fuseki load");
            _loaded = true;
            return;
        }

        _logger.LogInformation("OntologyCache: refreshing from {Endpoint}", _options.EndpointUrl);

        try
        {
            var entries = await QueryFusekiAsync(ct);

            _byIri.Clear();
            _byLabel.Clear();

            foreach (var entry in entries)
            {
                _byIri[entry.Iri] = entry;
                if (entry.Label is not null)
                    _byLabel.GetOrAdd(entry.Label, _ => new()).Add(entry);
            }

            _loaded = true;
            _logger.LogInformation("OntologyCache: loaded {Count} entries", _byIri.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "OntologyCache: refresh failed");
            _loaded = true; // mark loaded to avoid retry storm on first lookup
        }
    }

    private async Task EnsureLoadedAsync(CancellationToken ct)
    {
        if (!_loaded && _options.LoadOnStartup && _options.EndpointUrl is not null)
            await RefreshAsync(ct);
    }

    private async Task<IEnumerable<OntologyEntry>> QueryFusekiAsync(CancellationToken ct)
    {
        var sparql = BuildSparqlQuery();
        var url = $"{_options.EndpointUrl!.TrimEnd('/')}/sparql";

        using var content = new FormUrlEncodedContent(
            [KeyValuePair.Create("query", sparql)]);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(
            "application/x-www-form-urlencoded");

        var request = new HttpRequestMessage(HttpMethod.Post, url) { Content = content };
        request.Headers.Accept.ParseAdd("application/sparql-results+json");

        var response = await _http.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);
        return ParseBindings(json);
    }

    private string BuildSparqlQuery()
    {
        var graphClause = _options.GraphIri is not null ? $"FROM <{_options.GraphIri}>" : "";

        // Raw string for the SPARQL body — curly braces are SPARQL syntax, not interpolation.
        const string body = """
            PREFIX owl:  <http://www.w3.org/2002/07/owl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT DISTINCT ?iri ?label ?parent ?type
            WHERE {
              VALUES ?type { owl:Class owl:ObjectProperty owl:DatatypeProperty owl:NamedIndividual }
              ?iri a ?type .
              OPTIONAL { ?iri rdfs:label ?label . FILTER(lang(?label) = "en" || lang(?label) = "") }
              OPTIONAL { ?iri rdfs:subClassOf ?parent . FILTER(!isBlank(?parent)) }
            }
            ORDER BY ?iri
            """;

        return string.IsNullOrEmpty(graphClause)
            ? body
            : body.Replace(
                "SELECT DISTINCT ?iri ?label ?parent ?type",
                $"SELECT DISTINCT ?iri ?label ?parent ?type {graphClause}");
    }

    private static IEnumerable<OntologyEntry> ParseBindings(JsonElement json)
    {
        if (!json.TryGetProperty("results", out var results) ||
            !results.TryGetProperty("bindings", out var bindings))
            yield break;

        foreach (var binding in bindings.EnumerateArray())
        {
            var iri = GetValue(binding, "iri");
            if (iri is null) continue;

            var rawType = GetValue(binding, "type") ?? "";
            var typeLabel = rawType.Contains("Class",          StringComparison.OrdinalIgnoreCase) ? "class"
                          : rawType.Contains("Individual",     StringComparison.OrdinalIgnoreCase) ? "individual"
                          : "property";

            yield return new OntologyEntry
            {
                Iri      = iri,
                Label    = GetValue(binding, "label"),
                ParentIri = GetValue(binding, "parent"),
                Type     = typeLabel
            };
        }
    }

    private static string? GetValue(JsonElement binding, string variable)
    {
        return binding.TryGetProperty(variable, out var prop)
            && prop.TryGetProperty("value", out var val)
            ? val.GetString()
            : null;
    }
}
