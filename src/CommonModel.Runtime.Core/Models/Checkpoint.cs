namespace CommonModel.Runtime.Core.Models;

public sealed record Checkpoint
{
    public required string DriverId { get; init; }
    public required string EntityPath { get; init; }
    public required string Position { get; init; }
    public DateTimeOffset UpdatedAt { get; init; } = DateTimeOffset.UtcNow;
}
