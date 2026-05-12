using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class HealthHeartbeatService : BackgroundService
{
    private readonly IDriverLifecycleController _controller;
    private readonly HeartbeatOptions _options;
    private readonly NatsOptions _natsOptions;
    private readonly ILogger<HealthHeartbeatService> _logger;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public HealthHeartbeatService(
        IDriverLifecycleController controller,
        IOptions<HeartbeatOptions> options,
        IOptions<NatsOptions> natsOptions,
        ILogger<HealthHeartbeatService> logger)
    {
        _controller  = controller;
        _options     = options.Value;
        _natsOptions = natsOptions.Value;
        _logger      = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var conn = new NatsConnection(NatsOpts.Default with
        {
            Url = string.Join(",", _natsOptions.Servers)
        });

        try
        {
            await conn.ConnectAsync();
            var js = _options.UseJetStream ? new NatsJSContext(conn) : null;

            _logger.LogInformation(
                "HealthHeartbeatService: publishing every {Interval}s to '{Prefix}.*'",
                _options.IntervalSeconds, _options.SubjectPrefix);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(_options.IntervalSeconds), stoppingToken);
                await PublishAllAsync(conn, js, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "HealthHeartbeatService terminated unexpectedly");
        }
    }

    private async Task PublishAllAsync(
        NatsConnection conn,
        NatsJSContext? js,
        CancellationToken ct)
    {
        var allHealth = _controller.GetAllHealth();
        if (allHealth.Count == 0) return;

        foreach (var (driverId, health) in allHealth)
        {
            var subject = BuildSubject(driverId);
            var bytes   = JsonSerializer.SerializeToUtf8Bytes(health, JsonOpts);

            try
            {
                if (js is not null)
                {
                    var ack = await js.PublishAsync(subject, bytes, cancellationToken: ct);
                    ack.EnsureSuccess();
                }
                else
                {
                    await conn.PublishAsync(subject, bytes, cancellationToken: ct);
                }

                _logger.LogDebug("Heartbeat published for '{DriverId}' (state: {State})",
                    driverId, health.State);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                if (js is not null)
                {
                    // JetStream publish failed (stream may not exist) — fall back to core NATS
                    _logger.LogDebug(ex,
                        "JetStream heartbeat failed for '{DriverId}', falling back to core NATS", driverId);
                    try
                    {
                        await conn.PublishAsync(subject, bytes, cancellationToken: ct);
                    }
                    catch (Exception fallbackEx)
                    {
                        _logger.LogWarning(fallbackEx,
                            "Core NATS heartbeat fallback also failed for '{DriverId}'", driverId);
                    }
                }
                else
                {
                    _logger.LogWarning(ex, "Heartbeat publish failed for '{DriverId}'", driverId);
                }
            }
        }
    }

    public string BuildSubject(string driverId) =>
        $"{_options.SubjectPrefix}.{driverId}".ToLowerInvariant();
}
