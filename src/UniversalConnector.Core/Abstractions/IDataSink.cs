using UniversalConnector.Core.Models;

namespace UniversalConnector.Core.Abstractions;

public interface IDataSink : IAsyncDisposable
{
    Task WriteAsync(DataChangeEvent evt, CancellationToken ct = default);
}
