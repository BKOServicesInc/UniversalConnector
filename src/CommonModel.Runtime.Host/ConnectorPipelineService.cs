using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Host;

public sealed class ConnectorPipelineService : BackgroundService
{
    private readonly IConnectorRegistry _registry;
    private readonly INatsPublisher _publisher;
    private readonly ICheckpointStore _checkpoints;
    private readonly ILogger<ConnectorPipelineService> _logger;

    public ConnectorPipelineService(
        IConnectorRegistry registry,
        INatsPublisher publisher,
        ICheckpointStore checkpoints,
        ILogger<ConnectorPipelineService> logger)
    {
        _registry = registry;
        _publisher = publisher;
        _checkpoints = checkpoints;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var drivers = _registry.ResolveAll();

        if (drivers.Count == 0)
        {
            _logger.LogWarning("No drivers resolved. Check descriptor files or driver configuration.");
            return;
        }

        _logger.LogInformation("Starting {Count} driver(s)", drivers.Count);

        var tasks = drivers.Select(d => RunDriverAsync(d, stoppingToken)).ToArray();
        await Task.WhenAll(tasks);

        _logger.LogInformation("All driver tasks completed");
    }

    private async Task RunDriverAsync(ISourceDriver driver, CancellationToken ct)
    {
        _logger.LogInformation("Starting driver '{DriverId}' ({SourceType})",
            driver.DriverId, driver.SourceType);

        try
        {
            await driver.ConnectAsync(ct);

            await foreach (var evt in driver.StreamChangesAsync(ct))
            {
                try
                {
                    await _publisher.PublishAsync(evt, ct: ct);

                    var position = evt.SourceTimestamp?.ToString("O") ?? evt.EventId;
                    await _checkpoints.SaveAsync(new Checkpoint
                    {
                        DriverId   = evt.DriverId,
                        EntityPath = evt.EntityPath,
                        Position   = position
                    }, ct);

                    _logger.LogDebug("Published {ChangeType} event for {DriverId}/{EntityPath}",
                        evt.ChangeType, evt.DriverId, evt.EntityPath);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Failed to publish event {EventId}", evt.EventId);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Driver '{DriverId}' stopped gracefully", driver.DriverId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Driver '{DriverId}' terminated with error", driver.DriverId);
        }
        finally
        {
            try
            {
                await driver.DisconnectAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error during disconnect for '{DriverId}'", driver.DriverId);
            }

            await driver.DisposeAsync();
        }
    }
}
