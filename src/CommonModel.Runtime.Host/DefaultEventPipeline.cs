using Microsoft.Extensions.Logging;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Host;

public sealed class DefaultEventPipeline : IEventPipeline
{
    private readonly INatsPublisher _publisher;
    private readonly ICheckpointStore _checkpoints;
    private readonly ILogger<DefaultEventPipeline> _logger;

    public DefaultEventPipeline(
        INatsPublisher publisher,
        ICheckpointStore checkpoints,
        ILogger<DefaultEventPipeline> logger)
    {
        _publisher   = publisher;
        _checkpoints = checkpoints;
        _logger      = logger;
    }

    public async Task ProcessAsync(RawChangeEvent evt, CancellationToken ct = default)
    {
        await _publisher.PublishAsync(evt, subjectOverride: evt.SubjectHint, ct: ct);

        var position = evt.SourceTimestamp?.ToString("O") ?? evt.EventId;
        await _checkpoints.SaveAsync(new Checkpoint
        {
            DriverId   = evt.DriverId,
            EntityPath = evt.EntityPath,
            Position   = position
        }, ct);

        _logger.LogDebug("Processed {ChangeType} event for {DriverId}/{EntityPath}",
            evt.ChangeType, evt.DriverId, evt.EntityPath);
    }
}
