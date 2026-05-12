using System.Collections.Concurrent;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Tests.TestHelpers;

public sealed class InMemoryOntologyCache : IOntologyCache
{
    private readonly ConcurrentDictionary<string, OntologyEntry> _byIri =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, List<OntologyEntry>> _byLabel =
        new(StringComparer.OrdinalIgnoreCase);

    public void Seed(params OntologyEntry[] entries)
    {
        foreach (var e in entries)
        {
            _byIri[e.Iri] = e;
            if (e.Label is not null)
                _byLabel.GetOrAdd(e.Label, _ => new()).Add(e);
        }
    }

    public Task<OntologyEntry?> GetByIriAsync(string iri, CancellationToken ct = default)
    {
        _byIri.TryGetValue(iri, out var entry);
        return Task.FromResult(entry);
    }

    public Task<IReadOnlyList<OntologyEntry>> FindByLabelAsync(string label, CancellationToken ct = default)
    {
        IReadOnlyList<OntologyEntry> result = _byLabel.TryGetValue(label, out var list)
            ? list
            : Array.Empty<OntologyEntry>();
        return Task.FromResult(result);
    }

    public Task<bool> ContainsAsync(string iri, CancellationToken ct = default) =>
        Task.FromResult(_byIri.ContainsKey(iri));

    public Task RefreshAsync(CancellationToken ct = default)
    {
        _byIri.Clear();
        _byLabel.Clear();
        return Task.CompletedTask;
    }

    public int Count => _byIri.Count;
}
