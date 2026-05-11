using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface IDataSourceConnector : IAsyncDisposable
{
    string ConnectorId { get; }
    string SourceType { get; }
    Task ConnectAsync(CancellationToken ct);
    IAsyncEnumerable<DataChangeEvent> StreamChangesAsync(CancellationToken ct);
    Task DisconnectAsync(CancellationToken ct);
    ConnectorHealthReport GetHealthReport();
}
