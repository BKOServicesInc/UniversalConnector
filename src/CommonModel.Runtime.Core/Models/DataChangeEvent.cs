namespace CommonModel.Runtime.Core.Models;

public enum ChangeType
{
    Insert,
    Update,
    Delete,
    Snapshot,
    SchemaChange,
    Heartbeat
}

public sealed record DataChangeEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString();
    public DateTimeOffset DetectedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? SourceTimestamp { get; init; }
    public required string SourceType { get; init; }
    public required string ConnectorId { get; init; }
    public required string EntityPath { get; init; }
    public ChangeType ChangeType { get; init; }
    public IReadOnlyDictionary<string, object?> PrimaryKey { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, object?> Payload { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, object?>? PreviousPayload { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
    public long SequenceNumber { get; init; }
    public string SchemaVersion { get; init; } = "1.0";
}
