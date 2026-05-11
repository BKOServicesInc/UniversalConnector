namespace CommonModel.Runtime.Core.Models;

public enum ConnectorState
{
    Disconnected,
    Connecting,
    Connected,
    Streaming,
    Reconnecting,
    Failed
}

public sealed class ConnectorHealthReport
{
    public required string ConnectorId { get; init; }
    public required string SourceType { get; init; }
    public ConnectorState State { get; init; }
    public DateTimeOffset LastChecked { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? LastEventAt { get; init; }
    public long TotalEventsEmitted { get; init; }
    public int ConsecutiveFailures { get; init; }
    public string? LastError { get; init; }
}
