using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Neo4j.Driver;
using System.Runtime.CompilerServices;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Connectors.Graph;

public sealed class Neo4jConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string Uri { get; set; } = "bolt://localhost:7687";
    public string Username { get; set; } = "neo4j";
    public string Password { get; set; } = "";
    public string WatermarkProperty { get; set; } = "updatedAt";
    public int PollIntervalSeconds { get; set; } = 30;
    public List<string> Labels { get; set; } = new();
}

public sealed class Neo4jConnector : BaseConnector
{
    private readonly Neo4jConnectorOptions _options;
    private IDriver? _driver;

    public Neo4jConnector(IOptions<Neo4jConnectorOptions> options, ILogger<Neo4jConnector> logger)
        : base(logger) => _options = options.Value;

    public override string ConnectorId => _options.ConnectorId;
    public override string SourceType => "neo4j";

    protected override Task ConnectCoreAsync(CancellationToken ct)
    {
        _driver = GraphDatabase.Driver(_options.Uri, AuthTokens.Basic(_options.Username, _options.Password));
        return Task.CompletedTask;
    }

    protected override async Task DisconnectCoreAsync(CancellationToken ct)
    {
        if (_driver is not null)
        {
            await _driver.DisposeAsync();
            _driver = null;
        }
    }

    protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);
        var watermarks = new Dictionary<string, DateTimeOffset>();

        while (!ct.IsCancellationRequested)
        {
            await using var session = _driver!.AsyncSession();
            foreach (var label in _options.Labels)
            {
                if (!watermarks.TryGetValue(label, out var since))
                    since = DateTimeOffset.UtcNow.AddHours(-1);

                var cursor = await session.RunAsync(
                    $"MATCH (n:{label}) WHERE n.{_options.WatermarkProperty} > $since RETURN n",
                    new { since = since.ToUnixTimeMilliseconds() });

                DateTimeOffset maxWm = since;
                while (await cursor.FetchAsync())
                {
                    var node = cursor.Current["n"].As<INode>();
                    var fields = node.Properties.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value);

                    if (fields.TryGetValue(_options.WatermarkProperty, out var wm))
                    {
                        var wmOffset = wm switch
                        {
                            ZonedDateTime zdt => zdt.ToDateTimeOffset(),
                            long ms => DateTimeOffset.FromUnixTimeMilliseconds(ms),
                            _ => since
                        };
                        if (wmOffset > maxWm) maxWm = wmOffset;
                    }

                    yield return new DataChangeEvent
                    {
                        SourceType = SourceType,
                        ConnectorId = ConnectorId,
                        EntityPath = label,
                        ChangeType = ChangeType.Snapshot,
                        PrimaryKey = new Dictionary<string, object?> { ["id"] = node.ElementId },
                        Payload = fields
                    };
                }

                watermarks[label] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_driver is not null) await _driver.DisposeAsync();
        await base.DisposeAsync();
    }
}
