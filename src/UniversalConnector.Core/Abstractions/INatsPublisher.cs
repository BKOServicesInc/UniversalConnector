using UniversalConnector.Core.Models;

namespace UniversalConnector.Core.Abstractions;

public interface INatsPublisher : IAsyncDisposable
{
    Task PublishAsync(
        DataChangeEvent evt,
        string? subjectOverride = null,
        IReadOnlyDictionary<string, string>? additionalHeaders = null,
        CancellationToken ct = default);
}
