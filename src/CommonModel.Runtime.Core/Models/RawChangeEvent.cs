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

public sealed record RawChangeEvent
{
    public string EventId { get; init; } = Ulid.NewUlid().ToString();
    public DateTimeOffset DetectedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? SourceTimestamp { get; init; }
    public required string SourceType { get; init; }
    public required string DriverId { get; init; }
    public string Context { get; init; } = "";
    public required string EntityPath { get; init; }
    public ChangeType ChangeType { get; init; }
    public IReadOnlyDictionary<string, object?> PrimaryKey { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, object?> Fields { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, object?>? PreviousFields { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
    public long SequenceNumber { get; init; }
}
