using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Composition;
using System.Data.Odbc;
using System.Runtime.CompilerServices;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Analytics;

public sealed class DatabricksConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string Host { get; set; } = "";
    public string HttpPath { get; set; } = "";
    public string ApiToken { get; set; } = "";
    public int Port { get; set; } = 443;
    public string WatermarkColumn { get; set; } = "_updated_at";
    public int PollIntervalSeconds { get; set; } = 60;
    public List<string> Tables { get; set; } = new();
}

[Export(typeof(ISourceDriver))]
public sealed class DatabricksConnector : BaseConnector
{
    private readonly DatabricksConnectorOptions _options;
    private string _connectionString = "";

    public DatabricksConnector(IOptions<DatabricksConnectorOptions> options, ILogger<DatabricksConnector> logger)
        : base(logger) => _options = options.Value;

    public override string DriverId => _options.DriverId;
    public override string SourceType => "databricks";

    protected override Task ConnectCoreAsync(CancellationToken ct)
    {
        _connectionString =
            $"Driver={{Simba Spark ODBC Driver}};Host={_options.Host};Port={_options.Port};" +
            $"HTTPPath={_options.HttpPath};SSL=1;ThriftTransport=2;AuthMech=3;UID=token;PWD={_options.ApiToken}";
        return Task.CompletedTask;
    }

    protected override Task DisconnectCoreAsync(CancellationToken ct) => Task.CompletedTask;

    protected override async IAsyncEnumerable<RawChangeEvent> PollOrStreamAsync(
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

                using var conn = new OdbcConnection(_connectionString);
                await conn.OpenAsync(ct);
                using var cmd = new OdbcCommand(
                    $"SELECT * FROM {table} WHERE {_options.WatermarkColumn} > '{since:yyyy-MM-dd HH:mm:ss}'", conn);
                using var reader = await cmd.ExecuteReaderAsync(ct);

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

                    yield return new RawChangeEvent
                    {
                        SourceType = SourceType,
                        DriverId = DriverId,
                        Context = _options.Context,
                        EntityPath = table,
                        ChangeType = ChangeType.Snapshot,
                        SourceTimestamp = maxWm,
                        PrimaryKey = new Dictionary<string, object?>(),
                        Fields = fields
                    };
                }

                watermarks[table] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }
}
