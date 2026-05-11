using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface ISourceDriver : IAsyncDisposable
{
    string DriverId { get; }
    string SourceType { get; }
    Task ConnectAsync(CancellationToken ct);
    IAsyncEnumerable<RawChangeEvent> StreamChangesAsync(CancellationToken ct);
    Task DisconnectAsync(CancellationToken ct);
    HealthStatus GetHealth();
}
