using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Runtime.CompilerServices;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Connectors.Relational;

public sealed class PostgresConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string ConnectionString { get; set; } = "";
    public string WatermarkColumn { get; set; } = "updated_at";
    public int PollIntervalSeconds { get; set; } = 30;
    public List<string> Tables { get; set; } = new();
}

public sealed class PostgresConnector : BaseConnector
{
    private readonly PostgresConnectorOptions _options;
    private NpgsqlDataSource? _dataSource;
    private readonly Dictionary<string, DateTimeOffset> _watermarks = new();

    public PostgresConnector(IOptions<PostgresConnectorOptions> options, ILogger<PostgresConnector> logger)
        : base(logger) => _options = options.Value;

    public override string ConnectorId => _options.ConnectorId;
    public override string SourceType => "postgres";

    protected override int MaxConsecutiveFailures => _options.MaxConsecutiveFailures;
    protected override int RetryDelaySeconds => _options.RetryDelaySeconds;

    protected override Task ConnectCoreAsync(CancellationToken ct)
    {
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
        return Task.CompletedTask;
    }

    protected override async Task DisconnectCoreAsync(CancellationToken ct)
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
            _dataSource = null;
        }
    }

    protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            foreach (var table in _options.Tables)
            {
                if (!_watermarks.TryGetValue(table, out var since))
                    since = DateTimeOffset.UtcNow.AddHours(-1);

                await using var conn = await _dataSource!.OpenConnectionAsync(ct);
                var sql = $"SELECT * FROM {table} WHERE {_options.WatermarkColumn} > @since ORDER BY {_options.WatermarkColumn}";
                await using var cmd = new NpgsqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("since", since.UtcDateTime);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                DateTimeOffset maxWm = since;
                while (await reader.ReadAsync(ct))
                {
                    var fields = new Dictionary<string, object?>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        fields[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

                    if (fields.TryGetValue(_options.WatermarkColumn, out var wm) && wm is DateTime dt)
                    {
                        var wmOffset = new DateTimeOffset(dt, TimeSpan.Zero);
                        if (wmOffset > maxWm) maxWm = wmOffset;
                    }

                    var pk = new Dictionary<string, object?>();
                    yield return new DataChangeEvent
                    {
                        SourceType = SourceType,
                        ConnectorId = ConnectorId,
                        EntityPath = table,
                        ChangeType = ChangeType.Snapshot,
                        PrimaryKey = pk,
                        Payload = fields
                    };
                }

                _watermarks[table] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_dataSource is not null) await _dataSource.DisposeAsync();
        await base.DisposeAsync();
    }
}
