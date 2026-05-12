namespace CommonModel.Runtime.Core.Models;

public sealed record LifecycleEvent
{
    public required string DriverId { get; init; }
    public required DriverState State { get; init; }
    public DriverState? PreviousState { get; init; }
    public string? TriggeringAction { get; init; }
    public string? CommandId { get; init; }
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
}
