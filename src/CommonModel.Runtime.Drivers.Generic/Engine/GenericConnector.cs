using Microsoft.Extensions.Logging;
using System.Composition;
using System.Runtime.CompilerServices;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Drivers.Generic.Mapping;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

[Export(typeof(ISourceDriver))]
public sealed class GenericConnector : BaseConnector
{
    private readonly ConnectorDescriptor _descriptor;
    private readonly IProtocolAdapter _adapter;
    private readonly FieldMapper _fieldMapper;

    // Snapshot cache: keyed by "entityPath:pk1Value:pk2Value..."
    // Populated after each record is emitted so the next change for the same
    // entity row can carry a previous_fields even when the adapter uses polling.
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _snapshots = new();

    public GenericConnector(
        ConnectorDescriptor descriptor,
        IProtocolAdapter adapter,
        FieldMapper fieldMapper,
        ILogger<GenericConnector> logger)
        : base(logger)
    {
        _descriptor = descriptor;
        _adapter = adapter;
        _fieldMapper = fieldMapper;
    }

    public override string DriverId => _descriptor.ConnectorId;
    public override string SourceType => _descriptor.SourceType;

    protected override int MaxConsecutiveFailures => _descriptor.Resilience.MaxConsecutiveFailures;
    protected override int RetryDelaySeconds => Math.Max(_descriptor.Resilience.RetryDelaySeconds, 1);
    protected override double BackoffMultiplier => _descriptor.Resilience.BackoffMultiplier;
    protected override int MaxRetryDelaySeconds => _descriptor.Resilience.MaxRetryDelaySeconds;

    protected override Task ConnectCoreAsync(CancellationToken ct) =>
        _adapter.OpenAsync(_descriptor, ct);

    protected override Task DisconnectCoreAsync(CancellationToken ct) =>
        _adapter.CloseAsync(ct);

    protected override async IAsyncEnumerable<RawChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var raw in _adapter.StreamRawChangesAsync(_descriptor, ct))
        {
            var entityConfig = _descriptor.Watch.Entities
                .FirstOrDefault(e => e.Name.Equals(raw.EntityPath, StringComparison.OrdinalIgnoreCase));

            // If the adapter already supplied PreviousFields (e.g. Postgres CDC) use them directly.
            // For polling adapters PreviousFields is always empty — fall back to the snapshot cache
            // so that the previous state of the same row is available as previous_fields.
            var snapshotKey = BuildSnapshotKey(raw.EntityPath, raw.Fields, entityConfig?.PrimaryKey);
            var previousFields = raw.PreviousFields.Count > 0
                ? raw.PreviousFields
                : (_snapshots.TryGetValue(snapshotKey, out var cached) ? cached : raw.PreviousFields);

            var (primaryKey, fields, prevFields) = _fieldMapper.Apply(
                raw.Fields,
                previousFields,
                _descriptor.FieldMapping,
                entityConfig);

            var metadata = BuildMetadata(raw.AdapterMetadata, _descriptor.Nats.AdditionalHeaders);

            yield return new RawChangeEvent
            {
                SourceType = _descriptor.SourceType,
                DriverId = _descriptor.ConnectorId,
                EntityPath = raw.EntityPath,
                ChangeType = raw.ChangeType,
                SourceTimestamp = raw.SourceTimestamp,
                PrimaryKey = primaryKey,
                Fields = fields,
                PreviousFields = prevFields.Count > 0 ? prevFields : null,
                Metadata = metadata
            };

            // Update the snapshot with the current fields so the next change to this
            // row has something to diff against. Skip deletes — the row is gone.
            if (raw.ChangeType != ChangeType.Delete)
                _snapshots[snapshotKey] = raw.Fields;
            else
                _snapshots.Remove(snapshotKey);
        }
    }

    private static string BuildSnapshotKey(
        string entityPath,
        IReadOnlyDictionary<string, object?> fields,
        IReadOnlyList<string>? pkColumns)
    {
        if (pkColumns is null || pkColumns.Count == 0)
            return entityPath; // no PK defined — all rows share one slot; not ideal but safe

        var keyParts = pkColumns
            .Select(pk => fields.TryGetValue(pk, out var v) ? v?.ToString() ?? "∅" : "∅");
        return $"{entityPath}:{string.Join(":", keyParts)}";
    }

    private static IReadOnlyDictionary<string, string> BuildMetadata(
        IReadOnlyDictionary<string, string> adapterMeta,
        Dictionary<string, string> natsHeaders)
    {
        var merged = new Dictionary<string, string>(adapterMeta, StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in natsHeaders)
            merged[k] = v;
        return merged;
    }

    public override async ValueTask DisposeAsync()
    {
        await _adapter.DisposeAsync();
        await base.DisposeAsync();
    }
}
