using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class DriverLifecycleService : BackgroundService
{
    private const string CommandSubject  = "cdc.commands.*";
    private const string LifecyclePrefix = "cdc.lifecycle";

    private readonly IDriverLifecycleController _controller;
    private readonly LifecycleFsm _fsm;
    private readonly NatsConnectionFactory _factory;
    private readonly ILogger<DriverLifecycleService> _logger;

    public DriverLifecycleService(
        IDriverLifecycleController controller,
        LifecycleFsm fsm,
        NatsConnectionFactory factory,
        ILogger<DriverLifecycleService> logger)
    {
        _controller = controller;
        _fsm        = fsm;
        _factory    = factory;
        _logger     = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var conn = new NatsConnection(_factory.BuildOpts());

        try
        {
            await conn.ConnectAsync();
            var js = new NatsJSContext(conn);
            _logger.LogInformation("DriverLifecycleService: listening on '{Subject}'", CommandSubject);

            await foreach (var msg in conn.SubscribeAsync<byte[]>(CommandSubject, cancellationToken: stoppingToken))
            {
                await HandleCommandAsync(conn, js, msg, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DriverLifecycleService terminated unexpectedly");
        }
    }

    private async Task HandleCommandAsync(
        NatsConnection conn,
        NatsJSContext js,
        NatsMsg<byte[]> msg,
        CancellationToken ct)
    {
        LifecycleCommand? cmd;
        try
        {
            cmd = msg.Data is null
                ? null
                : JsonSerializer.Deserialize<LifecycleCommand>(msg.Data, JsonOpts.Default);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize lifecycle command from subject '{Subject}'", msg.Subject);
            return;
        }

        if (cmd is null)
        {
            _logger.LogWarning("Received null lifecycle command on '{Subject}'", msg.Subject);
            return;
        }

        var action = cmd.Action.ToLowerInvariant();
        var allHealth = _controller.GetAllHealth();

        if (!allHealth.TryGetValue(cmd.DriverId, out var health))
        {
            _logger.LogWarning("Lifecycle command '{Action}' for unknown driver '{DriverId}'",
                action, cmd.DriverId);
            return;
        }

        var currentState = health.State;

        if (!_fsm.CanApply(currentState, action))
        {
            _logger.LogWarning(
                "Lifecycle command '{Action}' rejected for driver '{DriverId}': invalid in state {State}. Valid: [{Valid}]",
                action, cmd.DriverId, currentState,
                string.Join(", ", _fsm.ValidActionsFor(currentState)));
            return;
        }

        _logger.LogInformation("Applying lifecycle command '{Action}' to driver '{DriverId}' (state: {State})",
            action, cmd.DriverId, currentState);

        bool success;
        try
        {
            success = action switch
            {
                "stop"    => await _controller.StopAsync(cmd.DriverId, ct),
                "start"   => await _controller.StartAsync(cmd.DriverId, ct),
                "restart" => await _controller.RestartAsync(cmd.DriverId, ct),
                _ => false
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing lifecycle command '{Action}' on '{DriverId}'",
                action, cmd.DriverId);
            return;
        }

        if (!success)
        {
            _logger.LogWarning("Lifecycle command '{Action}' for '{DriverId}' returned false", action, cmd.DriverId);
            return;
        }

        // Publish state-change notification
        var newHealth   = _controller.GetAllHealth();
        var newState    = newHealth.TryGetValue(cmd.DriverId, out var nh) ? nh.State : currentState;
        var evt = new LifecycleEvent
        {
            DriverId        = cmd.DriverId,
            State           = newState,
            PreviousState   = currentState,
            TriggeringAction = action,
            CommandId       = cmd.CommandId
        };

        var evtBytes   = JsonSerializer.SerializeToUtf8Bytes(evt, JsonOpts.Default);
        var evtSubject = $"{LifecyclePrefix}.{cmd.DriverId}".ToLowerInvariant();

        try
        {
            // Prefer JetStream for durable lifecycle events; fall back to core NATS if no stream exists.
            try
            {
                var ack = await js.PublishAsync(evtSubject, evtBytes, cancellationToken: ct);
                ack.EnsureSuccess();
            }
            catch
            {
                await conn.PublishAsync(evtSubject, evtBytes, cancellationToken: ct);
            }

            _logger.LogInformation(
                "Driver '{DriverId}' transitioned {From} → {To} (command: {Action})",
                cmd.DriverId, currentState, newState, action);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish lifecycle event for '{DriverId}'", cmd.DriverId);
        }
    }
}

file static class JsonOpts
{
    internal static readonly JsonSerializerOptions Default = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}
