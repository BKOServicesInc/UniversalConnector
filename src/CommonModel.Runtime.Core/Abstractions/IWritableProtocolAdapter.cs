using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public enum WriteOperation
{
    Create,
    Update,
    Delete
}

public sealed class WriteCommand
{
    public required string DriverId { get; init; }
    public required string SourceType { get; init; }
    public required string EntityType { get; init; }
    public required WriteOperation Operation { get; init; }
    public required IReadOnlyDictionary<string, object?> PrimaryKey { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
    public string CorrelationId { get; init; } = Ulid.NewUlid().ToString();
}

public sealed class WriteResult
{
    public required bool Success { get; init; }
    public string? Error { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; } = new Dictionary<string, object?>();
    public string ReplicaSessionId { get; init; } = "";

    public static WriteResult Ok(string replicaSessionId, IReadOnlyDictionary<string, object?>? fields = null) =>
        new() { Success = true, ReplicaSessionId = replicaSessionId, Fields = fields ?? new Dictionary<string, object?>() };

    public static WriteResult Fail(string error) =>
        new() { Success = false, Error = error };
}

public interface IWritableProtocolAdapter
{
    IReadOnlyList<string> SupportedEntityTypes { get; }
    Task<WriteResult> ApplyAsync(ConnectorDescriptor descriptor, WriteCommand command, CancellationToken ct);
}
