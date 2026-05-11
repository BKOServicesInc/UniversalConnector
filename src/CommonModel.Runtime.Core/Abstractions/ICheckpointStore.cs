using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface ICheckpointStore
{
    Task<Checkpoint?> GetAsync(string driverId, string entityPath, CancellationToken ct = default);
    Task SaveAsync(Checkpoint checkpoint, CancellationToken ct = default);
}
