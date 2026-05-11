using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using UniversalConnector.Core.Abstractions;

namespace UniversalConnector.Host;

public sealed class ConnectorPipelineService : BackgroundService
{
    private readonly IConnectorRegistry _registry;
    private readonly INatsPublisher _publisher;
    private readonly ILogger<ConnectorPipelineService> _logger;

    public ConnectorPipelineService(
        IConnectorRegistry registry,
        INatsPublisher publisher,
        ILogger<ConnectorPipelineService> logger)
    {
        _registry = registry;
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connectors = _registry.ResolveAll();

        if (connectors.Count == 0)
        {
            _logger.LogWarning("No connectors resolved. Check descriptor files or connector configuration.");
            return;
        }

        _logger.LogInformation("Starting {Count} connector(s)", connectors.Count);

        var tasks = connectors.Select(c => RunConnectorAsync(c, stoppingToken)).ToArray();
        await Task.WhenAll(tasks);

        _logger.LogInformation("All connector tasks completed");
    }

    private async Task RunConnectorAsync(IDataSourceConnector connector, CancellationToken ct)
    {
        _logger.LogInformation("Starting connector '{ConnectorId}' ({SourceType})",
            connector.ConnectorId, connector.SourceType);

        try
        {
            await connector.ConnectAsync(ct);

            await foreach (var evt in connector.StreamChangesAsync(ct))
            {
                try
                {
                    await _publisher.PublishAsync(evt, ct: ct);
                    _logger.LogDebug("Published {ChangeType} event for {ConnectorId}/{EntityPath}",
                        evt.ChangeType, evt.ConnectorId, evt.EntityPath);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Failed to publish event {EventId}", evt.EventId);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Connector '{ConnectorId}' stopped gracefully", connector.ConnectorId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Connector '{ConnectorId}' terminated with error", connector.ConnectorId);
        }
        finally
        {
            try
            {
                // Use CancellationToken.None — stoppingToken is already cancelled at this point
                await connector.DisconnectAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error during disconnect for '{ConnectorId}'", connector.ConnectorId);
            }

            await connector.DisposeAsync();
        }
    }
}
