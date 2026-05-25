using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Host;

public sealed class ConnectorPipelineService : BackgroundService, IDriverLifecycleController
{
    private readonly IConnectorRegistry _registry;
    private readonly IEventPipeline _pipeline;
    private readonly ILogger<ConnectorPipelineService> _logger;

    // Populated once in ExecuteAsync; stable reference used by restart logic.
    private IReadOnlyList<ISourceDriver> _allDrivers = Array.Empty<ISourceDriver>();
    private CancellationToken _hostToken;

    private sealed record DriverInfo(
        ISourceDriver Driver,
        CancellationTokenSource Cts,
        Task Loop);

    private readonly ConcurrentDictionary<string, DriverInfo> _infos = new();

    public ConnectorPipelineService(
        IConnectorRegistry registry,
        IEventPipeline pipeline,
        ILogger<ConnectorPipelineService> logger)
    {
        _registry = registry;
        _pipeline = pipeline;
        _logger   = logger;
    }

    // ── IDriverLifecycleController ────────────────────────────────────────────

    public IReadOnlyDictionary<string, HealthStatus> GetAllHealth() =>
        _allDrivers.ToDictionary(d => d.DriverId, d => d.GetHealth());

    public async Task<bool> StopAsync(string driverId, CancellationToken ct = default)
    {
        if (!_infos.TryGetValue(driverId, out var info)) return false;
        if (info.Loop.IsCompleted) return false;

        info.Cts.Cancel();

        try
        {
            await info.Loop.WaitAsync(TimeSpan.FromSeconds(15), ct);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Driver '{DriverId}' did not stop within 15 seconds", driverId);
        }
        catch (OperationCanceledException) { }

        return true;
    }

    public Task<bool> StartAsync(string driverId, CancellationToken ct = default)
    {
        if (_infos.TryGetValue(driverId, out var existing) && !existing.Loop.IsCompleted)
            return Task.FromResult(false);

        var driver = _allDrivers.FirstOrDefault(d => d.DriverId == driverId);
        if (driver is null) return Task.FromResult(false);

        _ = LaunchDriverAsync(driver, _hostToken);
        return Task.FromResult(true);
    }

    public async Task<bool> RestartAsync(string driverId, CancellationToken ct = default)
    {
        await StopAsync(driverId, ct);
        await Task.Delay(200, ct);
        return await StartAsync(driverId, ct);
    }

    // ── BackgroundService ─────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _hostToken  = stoppingToken;
        _allDrivers = _registry.ResolveAll();

        if (_allDrivers.Count == 0)
        {
            _logger.LogWarning("No drivers resolved. Check descriptor files or driver configuration.");
            return;
        }

        _logger.LogInformation("Starting {Count} driver(s)", _allDrivers.Count);

        var tasks = _allDrivers.Select(d => LaunchDriverAsync(d, stoppingToken)).ToArray();
        await Task.WhenAll(tasks);

        _logger.LogInformation("All driver tasks completed");
    }

    private async Task LaunchDriverAsync(ISourceDriver driver, CancellationToken parentCt)
    {
        while (!parentCt.IsCancellationRequested)
        {
            using var cts  = CancellationTokenSource.CreateLinkedTokenSource(parentCt);
            var task       = RunDriverAsync(driver, cts.Token);
            _infos[driver.DriverId] = new DriverInfo(driver, cts, task);
            await task;

            if (parentCt.IsCancellationRequested) return;

            // Don't restart a driver that explicitly reached Failed state (max failures).
            if (driver.GetHealth().State == DriverState.Failed)
            {
                _logger.LogWarning(
                    "Driver '{DriverId}' reached Failed state — not restarting", driver.DriverId);
                return;
            }

            _logger.LogWarning(
                "Driver '{DriverId}' exited unexpectedly (state: {State}) — restarting in 5s",
                driver.DriverId, driver.GetHealth().State);

            try { await Task.Delay(TimeSpan.FromSeconds(5), parentCt); }
            catch (OperationCanceledException) { return; }
        }
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
                _logger.LogDebug(
                    "Pipeline receiving {ChangeType} for {DriverId}/{EntityPath} (eventId={EventId})",
                    evt.ChangeType, evt.DriverId, evt.EntityPath, evt.EventId);
                try
                {
                    await _pipeline.ProcessAsync(evt, ct);
                    _logger.LogDebug("Pipeline returned for eventId={EventId}", evt.EventId);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Pipeline failed for event {EventId}", evt.EventId);
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
            try { await driver.DisconnectAsync(CancellationToken.None); }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error during disconnect for '{DriverId}'", driver.DriverId);
            }

            await driver.DisposeAsync();
        }
    }
}
