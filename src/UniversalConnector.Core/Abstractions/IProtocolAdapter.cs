using UniversalConnector.Core.Descriptors;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Core.Abstractions;

public sealed class RawChangeRecord
{
    public required string EntityPath { get; init; }
    public required ChangeType ChangeType { get; init; }
    public DateTimeOffset? SourceTimestamp { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, object?> PreviousFields { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, string> AdapterMetadata { get; init; } = new Dictionary<string, string>();
}

public interface IProtocolAdapter : IAsyncDisposable
{
    string SourceType { get; }
    Task OpenAsync(ConnectorDescriptor descriptor, CancellationToken ct);
    Task CloseAsync(CancellationToken ct);
    IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(ConnectorDescriptor descriptor, CancellationToken ct);
    IReadOnlyList<string> Validate(ConnectorDescriptor descriptor);
}
