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

    // Snapshot diff for delete detection:
    //   _knownKeys[entityName]            = set of PK strings seen in the last full poll cycle
    //   _lastKnownFields[entityName][pk]  = last-known field values for that PK (used as Fields on DELETE)
    private readonly Dictionary<string, HashSet<string>>                          _knownKeys        = new();
    private readonly Dictionary<string, Dictionary<string, Dictionary<string, object?>>> _lastKnownFields = new();

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
                var alias    = isRelationship ? "r" : "n";

                var entityConfig = descriptor.Watch.Entities
                    .FirstOrDefault(e => e.Name.Equals(entityName, StringComparison.OrdinalIgnoreCase));
                var extraFilter = entityConfig?.Filter is { Length: > 0 } f ? $" AND {f}" : "";

                var matchClause = isRelationship
                    ? $"MATCH ()-[{alias}:{typeName}]->()"
                    : $"MATCH ({alias}:{typeName})";

                // ── Step 1: full scan (no watermark) to detect deletes ───────────────────
                // We must query ALL nodes/relationships of this type each cycle so we can
                // diff the current PK set against the previous one and emit DELETE events
                // for anything that has disappeared.
                var fullCypher = $"{matchClause}{(extraFilter.Length > 0 ? $" WHERE {extraFilter[5..]}" : "")} " +
                                 $"RETURN {alias}";

                var fullResult     = await session.RunAsync(fullCypher);
                var currentKeys    = new HashSet<string>();
                var currentRecords = new List<(string pkString, Dictionary<string, object?> fields)>();

                await foreach (var record in fullResult)
                {
                    var props  = isRelationship
                        ? record["r"].As<IRelationship>().Properties
                        : record["n"].As<INode>().Properties;
                    var fields   = props.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value);
                    var pkString = BuildPkString(entityConfig, fields);
                    currentKeys.Add(pkString);
                    currentRecords.Add((pkString, fields));

                    // Keep last-known fields up to date for future delete payloads
                    if (!_lastKnownFields.TryGetValue(entityName, out var fieldCache))
                        _lastKnownFields[entityName] = fieldCache = new();
                    fieldCache[pkString] = fields;
                }

                // ── Step 2: emit DELETE for any PK that vanished since last cycle ────────
                if (_knownKeys.TryGetValue(entityName, out var previousKeys))
                {
                    var fieldCache = _lastKnownFields.GetValueOrDefault(entityName)
                                     ?? new Dictionary<string, Dictionary<string, object?>>();
                    foreach (var deletedPk in previousKeys.Except(currentKeys))
                    {
                        var deletedFields = fieldCache.GetValueOrDefault(deletedPk)
                                            ?? new Dictionary<string, object?>();
                        _logger.LogInformation(
                            "Neo4j: detected deleted {Entity} pk={Pk}", entityName, deletedPk);
                        yield return new RawChangeRecord
                        {
                            EntityPath = entityName,
                            ChangeType = ChangeType.Delete,
                            Fields     = deletedFields
                        };
                        fieldCache.Remove(deletedPk);
                    }
                }

                _knownKeys[entityName] = currentKeys;

                // ── Step 3: emit Insert/Update for changed records (watermark filter) ────
                DateTimeOffset maxWm = since;

                foreach (var (_, fields) in currentRecords)
                {
                    if (!fields.TryGetValue(watermarkProp, out var wm)) continue;

                    var wmOffset = wm switch
                    {
                        DateTimeOffset dto => dto,
                        ZonedDateTime  zdt => zdt.ToDateTimeOffset(),
                        LocalDateTime  ldt => new DateTimeOffset(ldt.Year, ldt.Month, ldt.Day,
                                                 ldt.Hour, ldt.Minute, ldt.Second, TimeSpan.Zero)
                                                 .AddTicks(ldt.Nanosecond / 100),
                        long ms            => DateTimeOffset.FromUnixTimeMilliseconds(ms),
                        _                  => since
                    };

                    if (wmOffset <= since) continue;   // not changed since last poll
                    if (wmOffset > maxWm) maxWm = wmOffset;

                    yield return new RawChangeRecord
                    {
                        EntityPath      = entityName,
                        ChangeType      = ChangeType.Snapshot,   // Insert/Update resolved by GenericConnector
                        SourceTimestamp = wmOffset,
                        Fields          = fields
                    };
                }

                _watermarks[entityName] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    private static string BuildPkString(EntityConfig? entityConfig, Dictionary<string, object?> fields)
    {
        var keys = entityConfig?.PrimaryKey is { Count: > 0 } pk ? pk : fields.Keys.ToList();
        return string.Join("|", keys.Select(k => fields.TryGetValue(k, out var v) ? v?.ToString() ?? "" : ""));
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
