using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using System.Runtime.CompilerServices;
using System.Xml;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class Neo4jAdapter : BaseProtocolAdapter
{
    private readonly ILogger<Neo4jAdapter> _logger;
    private IDriver? _driver;
    private readonly Dictionary<string, DateTimeOffset> _watermarks = new();

    public Neo4jAdapter(ILogger<Neo4jAdapter> logger) => _logger = logger;

    public override string SourceType => "neo4j";

    protected override Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var c = descriptor.Connection;
        _driver = GraphDatabase.Driver(
            c.Uri ?? "bolt://localhost:7687",
            AuthTokens.Basic(c.Username ?? "", c.Password ?? ""));
        return Task.CompletedTask;
    }

    protected override async Task CloseCoreAsync(CancellationToken ct)
    {
        if (_driver is not null)
        {
            await _driver.DisposeAsync();
            _driver = null;
        }
    }

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var watermarkProp = descriptor.ChangeDetection.WatermarkColumn;
        var entities = await ResolveEntities(descriptor, ct);

        while (!ct.IsCancellationRequested)
        {
            await using var session = _driver!.AsyncSession();

            foreach (var entityName in entities)
            {
                if (!_watermarks.TryGetValue(entityName, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                bool isRelationship = entityName.StartsWith("REL:", StringComparison.OrdinalIgnoreCase);
                var typeName = isRelationship ? entityName[4..] : entityName;
                var alias = isRelationship ? "r" : "n";

                var entityFilter = descriptor.Watch.Entities
                    .FirstOrDefault(e => e.Name.Equals(entityName, StringComparison.OrdinalIgnoreCase));
                var extraFilter = entityFilter?.Filter is { Length: > 0 } f ? $" AND {f}" : "";

                var matchClause = isRelationship
                    ? $"MATCH ()-[{alias}:{typeName}]->()"
                    : $"MATCH ({alias}:{typeName})";

                // Requires the watermark property to be a Neo4j `datetime` type.
                // For `localdatetime` properties, change to: localdatetime({epochMillis: $since}) is not valid;
                // instead store as `datetime` in Neo4j or use epoch-ms integers with `$since` directly.
                var cypher = $"{matchClause} WHERE {alias}.{watermarkProp} > datetime({{epochMillis: $since}}){extraFilter} " +
                             $"RETURN {alias} ORDER BY {alias}.{watermarkProp}";

                var result = await session.RunAsync(cypher, new { since = since.ToUnixTimeMilliseconds() });
                DateTimeOffset maxWm = since;

                await foreach (var record in result)
                {
                    var node = isRelationship
                        ? record["r"].As<IRelationship>().Properties
                        : record["n"].As<INode>().Properties;

                    var fields = node.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value);

                    if (fields.TryGetValue(watermarkProp, out var wm))
                    {
                        var wmOffset = wm switch
                        {
                            DateTimeOffset dto => dto,
                            ZonedDateTime zdt => zdt.ToDateTimeOffset(),
                            LocalDateTime ldt => new DateTimeOffset(ldt.Year, ldt.Month, ldt.Day, ldt.Hour, ldt.Minute, ldt.Second, TimeSpan.Zero)
                                .AddTicks(ldt.Nanosecond / 100),
                            long ms => DateTimeOffset.FromUnixTimeMilliseconds(ms),
                            _ => since
                        };
                        if (wmOffset > maxWm) maxWm = wmOffset;
                    }

                    yield return new RawChangeRecord
                    {
                        EntityPath = entityName,
                        ChangeType = ChangeType.Snapshot,
                        SourceTimestamp = maxWm,
                        Fields = fields
                    };
                }

                _watermarks[entityName] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task<List<string>> ResolveEntities(ConnectorDescriptor d, CancellationToken ct)
    {
        if (!d.Watch.AutoDiscover)
            return d.Watch.Entities.Select(e => e.Name).ToList();

        await using var session = _driver!.AsyncSession();
        var cursor = await session.RunAsync("CALL db.labels()");
        var labels = new List<string>();
        while (await cursor.FetchAsync())
            labels.Add(cursor.Current["label"].As<string>());
        return labels;
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor) => Array.Empty<string>();

    public override async ValueTask DisposeAsync()
    {
        if (_driver is not null)
            await _driver.DisposeAsync();
        await base.DisposeAsync();
    }
}
