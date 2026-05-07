using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Text.Json;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Configuration;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Host.Services;

public sealed class PostgresDataSink : IDataSink
{
    private readonly PostgresSinkOptions _options;
    private readonly ILogger<PostgresDataSink> _logger;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false
    };

    private const string InsertSql = """
        INSERT INTO data_changes (
            event_id, detected_at, source_timestamp,
            source_type, connector_id, entity_path,
            change_type, primary_key, payload,
            previous_payload, metadata,
            sequence_number, schema_version
        ) VALUES (
            @EventId, @DetectedAt, @SourceTimestamp,
            @SourceType, @ConnectorId, @EntityPath,
            @ChangeType, @PrimaryKey::jsonb, @Payload::jsonb,
            @PreviousPayload::jsonb, @Metadata::jsonb,
            @SequenceNumber, @SchemaVersion
        )
        ON CONFLICT (event_id) DO NOTHING;
        """;

    public PostgresDataSink(
        IOptions<PostgresSinkOptions> options,
        ILogger<PostgresDataSink> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task WriteAsync(DataChangeEvent evt, CancellationToken ct = default)
    {
        if (!_options.Enabled)
            return;

        try
        {
            var parameters = new
            {
                EventId        = Guid.TryParse(evt.EventId, out var parsedId) ? parsedId : Guid.NewGuid(),
                evt.DetectedAt,
                evt.SourceTimestamp,
                evt.SourceType,
                evt.ConnectorId,
                evt.EntityPath,
                ChangeType     = (short)evt.ChangeType,
                PrimaryKey     = JsonSerializer.Serialize(evt.PrimaryKey, JsonOptions),
                Payload        = JsonSerializer.Serialize(evt.Payload, JsonOptions),
                PreviousPayload = evt.PreviousPayload is not null
                                    ? JsonSerializer.Serialize(evt.PreviousPayload, JsonOptions)
                                    : null,
                Metadata       = JsonSerializer.Serialize(evt.Metadata, JsonOptions),
                evt.SequenceNumber,
                evt.SchemaVersion
            };

            await using var connection = new NpgsqlConnection(_options.ConnectionString);
            await connection.OpenAsync(ct);
            await connection.ExecuteAsync(new CommandDefinition(InsertSql, parameters, cancellationToken: ct));

            _logger.LogDebug(
                "Persisted event {EventId} ({ChangeType}) for {ConnectorId}/{EntityPath}",
                evt.EventId, evt.ChangeType, evt.ConnectorId, evt.EntityPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to persist event {EventId} to data_changes. Event will not be retried.",
                evt.EventId);
            // Intentionally swallowed — sink failure must not affect NATS publish
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;


}
