namespace CommonModel.Runtime.Core.Models;

public enum DriverState
{
    Disconnected,
    Connecting,
    Connected,
    Streaming,
    Reconnecting,
    Failed
}

public sealed class HealthStatus
{
    public required string DriverId { get; init; }
    public required string SourceType { get; init; }
    public DriverState State { get; init; }
    public DateTimeOffset LastChecked { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? LastEventAt { get; init; }
    public long TotalEventsEmitted { get; init; }
    public int ConsecutiveFailures { get; init; }
    public string? LastError { get; init; }
}
