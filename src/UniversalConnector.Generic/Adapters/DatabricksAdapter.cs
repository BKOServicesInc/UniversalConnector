using Microsoft.Extensions.Logging;
using System.Data.Odbc;
using System.Runtime.CompilerServices;
using System.Xml;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Descriptors;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Generic.Adapters;

public sealed class DatabricksAdapter : BaseProtocolAdapter
{
    private readonly ILogger<DatabricksAdapter> _logger;
    private string _connectionString = "";
    private readonly Dictionary<string, long> _versions = new();

    public DatabricksAdapter(ILogger<DatabricksAdapter> logger) => _logger = logger;

    public override string SourceType => "databricks";

    protected override Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var c = descriptor.Connection;
        _connectionString =
            $"Driver={{Simba Spark ODBC Driver}};Host={c.Host};Port={c.Port ?? 443};" +
            $"HTTPPath={c.HttpPath};SSL=1;ThriftTransport=2;AuthMech=3;UID=token;PWD={c.ApiToken}";
        return Task.CompletedTask;
    }

    protected override Task CloseCoreAsync(CancellationToken ct) => Task.CompletedTask;

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var mode = descriptor.ChangeDetection.Mode.ToLowerInvariant();

        if (mode == "cdc")
        {
            await foreach (var r in StreamDeltaCdfAsync(descriptor, ct))
                yield return r;
        }
        else
        {
            await foreach (var r in StreamPollingAsync(descriptor, ct))
                yield return r;
        }
    }

    private async IAsyncEnumerable<RawChangeRecord> StreamDeltaCdfAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var entities = descriptor.Watch.Entities.Select(e => e.Name).ToList();

        while (!ct.IsCancellationRequested)
        {
            foreach (var table in entities)
            {
                if (!_versions.TryGetValue(table, out var fromVersion))
                    fromVersion = descriptor.ChangeDetection.StartingVersion >= 0
                        ? descriptor.ChangeDetection.StartingVersion
                        : await GetLatestVersion(table, ct);

                using var conn = new OdbcConnection(_connectionString);
                await conn.OpenAsync(ct);

                var sql = $"SELECT * FROM table_changes('{table}', {fromVersion})";
                using var cmd = new OdbcCommand(sql, conn);
                using var reader = await cmd.ExecuteReaderAsync(ct);

                long maxVersion = fromVersion;
                while (await reader.ReadAsync(ct))
                {
                    var opCol = "_change_type";
                    var versionCol = "_commit_version";

                    var fields = new Dictionary<string, object?>();
                    string? operation = null;
                    long version = fromVersion;

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var name = reader.GetName(i);
                        if (name == opCol) { operation = reader.GetString(i); continue; }
                        if (name == versionCol) { version = reader.GetInt64(i); continue; }
                        if (name == "_commit_timestamp" || name == "_change_type_sequence") continue;
                        fields[name] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }

                    // Skip update_preimage rows per spec
                    if (string.Equals(operation, "update_preimage", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (version > maxVersion) maxVersion = version;

                    var changeType = operation switch
                    {
                        "insert" => ChangeType.Insert,
                        "update_postimage" => ChangeType.Update,
                        "delete" => ChangeType.Delete,
                        _ => ChangeType.Snapshot
                    };

                    yield return new RawChangeRecord
                    {
                        EntityPath = table,
                        ChangeType = changeType,
                        Fields = fields
                    };
                }

                _versions[table] = maxVersion + 1;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async IAsyncEnumerable<RawChangeRecord> StreamPollingAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var watermarkCol = descriptor.ChangeDetection.WatermarkColumn;
        var entities = descriptor.Watch.Entities.Select(e => e.Name).ToList();
        var watermarks = new Dictionary<string, DateTimeOffset>();

        while (!ct.IsCancellationRequested)
        {
            foreach (var table in entities)
            {
                if (!watermarks.TryGetValue(table, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                using var conn = new OdbcConnection(_connectionString);
                await conn.OpenAsync(ct);

                var sql = $"SELECT * FROM {table} WHERE {watermarkCol} > '{since:yyyy-MM-dd HH:mm:ss}' ORDER BY {watermarkCol}";
                using var cmd = new OdbcCommand(sql, conn);
                using var reader = await cmd.ExecuteReaderAsync(ct);

                DateTimeOffset maxWm = since;
                while (await reader.ReadAsync(ct))
                {
                    var fields = new Dictionary<string, object?>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        fields[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

                    if (fields.TryGetValue(watermarkCol, out var wm) && wm is DateTime dt)
                    {
                        var wmOffset = new DateTimeOffset(dt, TimeSpan.Zero);
                        if (wmOffset > maxWm) maxWm = wmOffset;
                    }

                    yield return new RawChangeRecord
                    {
                        EntityPath = table,
                        ChangeType = ChangeType.Snapshot,
                        Fields = fields
                    };
                }

                watermarks[table] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task<long> GetLatestVersion(string table, CancellationToken ct)
    {
        using var conn = new OdbcConnection(_connectionString);
        await conn.OpenAsync(ct);
        using var cmd = new OdbcCommand($"DESCRIBE HISTORY {table} LIMIT 1", conn);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            for (int i = 0; i < reader.FieldCount; i++)
                if (reader.GetName(i) == "version")
                    return reader.GetInt64(i);
        }
        return 0;
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor) => Array.Empty<string>();
}
