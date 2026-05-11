using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface INatsPublisher : IAsyncDisposable
{
    Task PublishAsync(
        DataChangeEvent evt,
        string? subjectOverride = null,
        IReadOnlyDictionary<string, string>? additionalHeaders = null,
        CancellationToken ct = default);
}
