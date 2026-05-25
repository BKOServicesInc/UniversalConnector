// Reverse-channel hosted service.
//
// Subscribes to the NATS command subjects declared by every descriptor whose
// adapter implements IWritableProtocolAdapter, decodes the Envelope, and calls
// the adapter's ApplyAsync. Because the adapter is resolved purely by
// sourceType from WritableAdapterRegistry, adding a second PI server (or any
// other writable target) is a pure config exercise — no code changes here.

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Drivers.Generic.Engine;
using CommonModel.Runtime.Infrastructure;
using CommonModel.Runtime.Infrastructure.Wire;

namespace CommonModel.Runtime.Host;

public sealed class ReverseChannelService : BackgroundService
{
    private readonly DescriptorStore _descriptors;
    private readonly WritableAdapterRegistry _writers;
    private readonly NatsConnectionFactory _factory;
    private readonly ILogger<ReverseChannelService> _logger;

    public ReverseChannelService(
        DescriptorStore descriptors,
        WritableAdapterRegistry writers,
        NatsConnectionFactory factory,
        ILogger<ReverseChannelService> logger)
    {
        _descriptors = descriptors;
        _writers     = writers;
        _factory     = factory;
        _logger      = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriptions = _descriptors.GetAll()
            .Where(d => d.Enabled && d.Nats.CommandSubjects.Count > 0)
            .Where(d => _writers.TryResolve(d.SourceType, out _))
            .ToList();

        if (subscriptions.Count == 0)
        {
            _logger.LogInformation(
                "Reverse channel idle — no enabled descriptors declare commandSubjects " +
                "for a writable sourceType. Registered writable sourceTypes: {Types}",
                string.Join(", ", _writers.RegisteredSourceTypes));
            return;
        }

        var conn = await _factory.GetSharedConnectionAsync(stoppingToken);
        var tasks = subscriptions.Select(d => RunSubscriptionAsync(conn, d, stoppingToken));
        await Task.WhenAll(tasks);
    }

    private async Task RunSubscriptionAsync(
        NatsConnection conn, ConnectorDescriptor descriptor, CancellationToken ct)
    {
        if (!_writers.TryResolve(descriptor.SourceType, out var adapter) || adapter is null)
            return;

        _logger.LogInformation(
            "Reverse channel for '{Driver}' subscribing to: {Subjects}",
            descriptor.DriverId, string.Join(", ", descriptor.Nats.CommandSubjects));

        foreach (var subject in descriptor.Nats.CommandSubjects)
        {
            _ = Task.Run(() => SubscribeOneAsync(conn, descriptor, adapter, subject, ct), ct);
        }

        // Hold this task open until cancellation; per-subject tasks run concurrently.
        try { await Task.Delay(Timeout.Infinite, ct); }
        catch (OperationCanceledException) { }
    }

    private async Task SubscribeOneAsync(
        NatsConnection conn,
        ConnectorDescriptor descriptor,
        IWritableProtocolAdapter adapter,
        string subject,
        CancellationToken ct)
    {
        try
        {
            await foreach (var msg in conn.SubscribeAsync<byte[]>(subject, cancellationToken: ct))
            {
                if (msg.Data is null || msg.Data.Length == 0) continue;

                try
                {
                    var envelope = Envelope.Parser.ParseFrom(msg.Data);
                    var cmd = ToCommand(envelope, descriptor.DriverId, descriptor.SourceType);
                    if (cmd is null)
                    {
                        _logger.LogDebug(
                            "Skipping envelope on '{Subject}' — not addressed to '{Driver}' (target={Target})",
                            subject, descriptor.DriverId,
                            envelope.Metadata.TryGetValue("targetDriverId", out var t) ? t : "(none)");
                        continue;
                    }

                    var result = await adapter.ApplyAsync(descriptor, cmd, ct);
                    if (result.Success)
                    {
                        _logger.LogInformation(
                            "Reverse-applied {Op} {EntityType} on '{Driver}' (corr={Corr}, replicaSession={Sid})",
                            cmd.Operation, cmd.EntityType, descriptor.DriverId,
                            cmd.CorrelationId, result.ReplicaSessionId);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "Reverse {Op} {EntityType} on '{Driver}' failed: {Error}",
                            cmd.Operation, cmd.EntityType, descriptor.DriverId, result.Error);
                    }
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    _logger.LogError(ex,
                        "Failed to apply reverse command on subject '{Subject}' for driver '{Driver}'",
                        subject, descriptor.DriverId);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Reverse-channel subscription on '{Subject}' (driver '{Driver}') terminated",
                subject, descriptor.DriverId);
        }
    }

    private static WriteCommand? ToCommand(Envelope envelope, string driverId, string sourceType)
    {
        // Only consume events explicitly targeted at this driver or at this sourceType.
        if (envelope.Metadata.TryGetValue("targetDriverId", out var target) &&
            !string.IsNullOrWhiteSpace(target) &&
            !string.Equals(target, driverId, StringComparison.OrdinalIgnoreCase))
            return null;

        if (envelope.Metadata.TryGetValue("targetSourceType", out var targetSrc) &&
            !string.IsNullOrWhiteSpace(targetSrc) &&
            !string.Equals(targetSrc, sourceType, StringComparison.OrdinalIgnoreCase))
            return null;

        var op = ParseOperation(envelope.ChangeType);
        if (op is null) return null;

        // EntityPath uses "entityType/name" — split into entityType / name. The
        // primary key falls back to the second segment when not present in
        // primary_key map.
        var slash       = envelope.EntityPath.IndexOf('/');
        var entityType  = slash > 0 ? envelope.EntityPath[..slash] : envelope.EntityPath;
        var entityName  = slash > 0 ? envelope.EntityPath[(slash + 1)..] : envelope.EntityPath;

        var pk = new Dictionary<string, object?>();
        foreach (var (k, v) in envelope.PrimaryKey) pk[k] = v;
        if (!pk.ContainsKey("name")) pk["name"] = entityName;

        var fields = new Dictionary<string, object?>();
        foreach (var (k, v) in envelope.Fields) fields[k] = v;

        var metadata = new Dictionary<string, string>();
        foreach (var (k, v) in envelope.Metadata) metadata[k] = v;

        return new WriteCommand
        {
            DriverId      = driverId,
            SourceType    = sourceType,
            EntityType    = entityType,
            Operation     = op.Value,
            PrimaryKey    = pk,
            Fields        = fields,
            Metadata      = metadata,
            CorrelationId = envelope.EventId
        };
    }

    private static WriteOperation? ParseOperation(string changeType) =>
        changeType?.ToLowerInvariant() switch
        {
            "insert" or "create"    => WriteOperation.Create,
            "update" or "snapshot"  => WriteOperation.Update,
            "delete"                => WriteOperation.Delete,
            _                       => null
        };
}
