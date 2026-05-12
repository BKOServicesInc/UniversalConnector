using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface IDriverLifecycleController
{
    IReadOnlyDictionary<string, HealthStatus> GetAllHealth();
    /// <returns>false if the driver is not found or not in a stoppable state.</returns>
    Task<bool> StopAsync(string driverId, CancellationToken ct = default);
    /// <returns>false if the driver is not found or already running.</returns>
    Task<bool> StartAsync(string driverId, CancellationToken ct = default);
    Task<bool> RestartAsync(string driverId, CancellationToken ct = default);
}
