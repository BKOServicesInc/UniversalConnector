using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Connectors.Relational;

public sealed class SqlServerConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string ConnectionString { get; set; } = "";
    public string WatermarkColumn { get; set; } = "updated_at";
    public int PollIntervalSeconds { get; set; } = 30;
    public List<string> Tables { get; set; } = new();
}

public sealed class SqlServerConnector : BaseConnector
{
    private readonly SqlServerConnectorOptions _options;

    public SqlServerConnector(IOptions<SqlServerConnectorOptions> options, ILogger<SqlServerConnector> logger)
        : base(logger) => _options = options.Value;

    public override string ConnectorId => _options.ConnectorId;
    public override string SourceType => "sqlserver";

    protected override int MaxConsecutiveFailures => _options.MaxConsecutiveFailures;

    protected override Task ConnectCoreAsync(CancellationToken ct) => Task.CompletedTask;
    protected override Task DisconnectCoreAsync(CancellationToken ct) => Task.CompletedTask;

    protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);
        var watermarks = new Dictionary<string, DateTimeOffset>();

        while (!ct.IsCancellationRequested)
        {
            foreach (var table in _options.Tables)
            {
                if (!watermarks.TryGetValue(table, out var since))
                    since = DateTimeOffset.UtcNow.AddHours(-1);

                await using var conn = new SqlConnection(_options.ConnectionString);
                await conn.OpenAsync(ct);
                await using var cmd = new SqlCommand(
                    $"SELECT * FROM {table} WHERE {_options.WatermarkColumn} > @since ORDER BY {_options.WatermarkColumn}", conn);
                cmd.Parameters.AddWithValue("@since", since.UtcDateTime);
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

                    yield return new DataChangeEvent
                    {
                        SourceType = SourceType,
                        ConnectorId = ConnectorId,
                        EntityPath = table,
                        ChangeType = ChangeType.Snapshot,
                        PrimaryKey = new Dictionary<string, object?>(),
                        Payload = fields
                    };
                }

                watermarks[table] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }
}
