namespace CommonModel.Runtime.Core.Models;

public sealed record LifecycleCommand
{
    public string CommandId { get; init; } = Ulid.NewUlid().ToString();
    public required string DriverId { get; init; }
    // "start" | "stop" | "restart"
    public required string Action { get; init; }
    public string? RequestedBy { get; init; }
    public DateTimeOffset IssuedAt { get; init; } = DateTimeOffset.UtcNow;
}
