using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface IOntologyCache
{
    Task<OntologyEntry?> GetByIriAsync(string iri, CancellationToken ct = default);
    Task<IReadOnlyList<OntologyEntry>> FindByLabelAsync(string label, CancellationToken ct = default);
    Task<bool> ContainsAsync(string iri, CancellationToken ct = default);
    Task RefreshAsync(CancellationToken ct = default);
}
