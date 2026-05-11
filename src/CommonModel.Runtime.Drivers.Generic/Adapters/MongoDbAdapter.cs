using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Runtime.CompilerServices;
using System.Xml;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class MongoDbAdapter : BaseProtocolAdapter
{
    private readonly ILogger<MongoDbAdapter> _logger;
    private MongoClient? _client;
    private IMongoDatabase? _database;

    // CDC: resume token persisted in memory for the lifetime of this session
    private BsonDocument? _resumeToken;

    // Polling: per-collection watermark
    private readonly Dictionary<string, DateTimeOffset> _watermarks = new();

    public MongoDbAdapter(ILogger<MongoDbAdapter> logger) => _logger = logger;

    public override string SourceType => "mongodb";

    protected override Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var uri = descriptor.Connection.Uri
            ?? throw new InvalidOperationException("mongodb requires connection.uri");
        var dbName = descriptor.Connection.Database
            ?? throw new InvalidOperationException("mongodb requires connection.database");

        _client = new MongoClient(uri);
        _database = _client.GetDatabase(dbName);

        _logger.LogInformation("MongoDB client opened for database '{Database}'", dbName);
        return Task.CompletedTask;
    }

    protected override Task CloseCoreAsync(CancellationToken ct)
    {
        // MongoClient manages its own connection pool; disposing releases it
        _client?.Dispose();
        _client = null;
        _database = null;
        return Task.CompletedTask;
    }

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var mode = descriptor.ChangeDetection.Mode;

        if (mode.Equals("cdc", StringComparison.OrdinalIgnoreCase))
        {
            await foreach (var r in StreamCdcAsync(descriptor, ct))
                yield return r;
        }
        else
        {
            await foreach (var r in StreamPollingAsync(descriptor, ct))
                yield return r;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CDC — MongoDB Change Streams
    // ─────────────────────────────────────────────────────────────────────────

    private async IAsyncEnumerable<RawChangeRecord> StreamCdcAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var collections = await ResolveCollections(descriptor, ct);

        // Watch at the database level so a single stream covers all watched collections.
        // A pipeline filter restricts events to only the requested collection names.
        var filter = Builders<ChangeStreamDocument<BsonDocument>>.Filter.In(
            "ns.coll", collections);

        var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .Match(filter);

        var options = new ChangeStreamOptions
        {
            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
            FullDocumentBeforeChange = ChangeStreamFullDocumentBeforeChangeOption.WhenAvailable,
            ResumeAfter = _resumeToken,
            BatchSize = 100
        };

        _logger.LogInformation("Opening MongoDB change stream on database '{Database}', collections: {Collections}",
            _database!.DatabaseNamespace.DatabaseName, string.Join(", ", collections));

        // IChangeStreamCursor implements IDisposable, not IAsyncDisposable
        using var cursor = await _database!.WatchAsync(pipeline, options, ct);

        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var change in cursor.Current)
            {
                _resumeToken = change.ResumeToken;

                var changeType = change.OperationType switch
                {
                    ChangeStreamOperationType.Insert  => ChangeType.Insert,
                    ChangeStreamOperationType.Update  => ChangeType.Update,
                    ChangeStreamOperationType.Replace => ChangeType.Update,
                    ChangeStreamOperationType.Delete  => ChangeType.Delete,
                    _                                 => (ChangeType?)null
                };

                if (changeType is null)
                    continue;

                var fields         = BsonToFields(change.FullDocument);
                var previousFields = BsonToFields(change.FullDocumentBeforeChange);

                // For deletes FullDocument is null — use DocumentKey to carry _id
                if (changeType == ChangeType.Delete && fields.Count == 0)
                    fields = BsonToFields(change.DocumentKey);

                var entityPath = change.CollectionNamespace?.CollectionName ?? "unknown";

                var sourceTimestamp = change.ClusterTime is not null
                    ? DateTimeOffset.FromUnixTimeSeconds(change.ClusterTime.Timestamp)
                    : (DateTimeOffset?)null;

                yield return new RawChangeRecord
                {
                    EntityPath      = entityPath,
                    ChangeType      = changeType.Value,
                    SourceTimestamp = sourceTimestamp,
                    Fields          = fields,
                    PreviousFields  = previousFields,
                    AdapterMetadata = new Dictionary<string, string>
                    {
                        ["resumeToken"] = change.ResumeToken?.ToString() ?? ""
                    }
                };
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Polling — watermark-based query
    // ─────────────────────────────────────────────────────────────────────────

    private async IAsyncEnumerable<RawChangeRecord> StreamPollingAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval     = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var watermarkCol = descriptor.ChangeDetection.WatermarkColumn;
        var collections  = await ResolveCollections(descriptor, ct);

        while (!ct.IsCancellationRequested)
        {
            foreach (var collectionName in collections)
            {
                if (!_watermarks.TryGetValue(collectionName, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                var collection = _database!.GetCollection<BsonDocument>(collectionName);
                var filter     = Builders<BsonDocument>.Filter.Gt(watermarkCol, since.UtcDateTime);
                var sort       = Builders<BsonDocument>.Sort.Ascending(watermarkCol);

                DateTimeOffset maxWatermark = since;

                using var cursor = await collection.Find(filter).Sort(sort).ToCursorAsync(ct);
                while (await cursor.MoveNextAsync(ct))
                {
                    foreach (var doc in cursor.Current)
                    {
                        var fields = BsonToFields(doc);

                        if (fields.TryGetValue(watermarkCol, out var wm))
                        {
                            var wmOffset = wm switch
                            {
                                DateTime dt        => new DateTimeOffset(dt, TimeSpan.Zero),
                                DateTimeOffset dto => dto,
                                _                  => since
                            };
                            if (wmOffset > maxWatermark) maxWatermark = wmOffset;
                        }

                        yield return new RawChangeRecord
                        {
                            EntityPath      = collectionName,
                            ChangeType      = ChangeType.Snapshot,
                            SourceTimestamp = maxWatermark,
                            Fields          = fields
                        };
                    }
                }

                _watermarks[collectionName] = maxWatermark;
            }

            await Task.Delay(interval, ct);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<List<string>> ResolveCollections(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        if (!descriptor.Watch.AutoDiscover)
            return descriptor.Watch.Entities.Select(e => e.Name).ToList();

        var names = new List<string>();
        using var cursor = await _database!.ListCollectionNamesAsync(cancellationToken: ct);
        while (await cursor.MoveNextAsync(ct))
            names.AddRange(cursor.Current);

        return names;
    }

    private static Dictionary<string, object?> BsonToFields(BsonDocument? doc)
    {
        if (doc is null) return new Dictionary<string, object?>();
        var fields = new Dictionary<string, object?>();
        foreach (var element in doc)
            fields[element.Name] = BsonValueToObject(element.Value);
        return fields;
    }

    private static object? BsonValueToObject(BsonValue value) => value.BsonType switch
    {
        BsonType.ObjectId  => value.AsObjectId.ToString(),
        BsonType.String    => value.AsString,
        BsonType.Int32     => value.AsInt32,
        BsonType.Int64     => value.AsInt64,
        BsonType.Double    => value.AsDouble,
        BsonType.Decimal128 => (double)value.AsDecimal128,
        BsonType.Boolean   => value.AsBoolean,
        BsonType.DateTime  => DateTimeOffset.FromUnixTimeMilliseconds(value.AsBsonDateTime.MillisecondsSinceEpoch),
        BsonType.Document  => BsonToFields(value.AsBsonDocument),
        BsonType.Array     => value.AsBsonArray.Select(BsonValueToObject).ToList(),
        BsonType.Null      => null,
        BsonType.Undefined => null,
        _                  => value.ToString()
    };

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor) =>
        Array.Empty<string>();

    public override async ValueTask DisposeAsync()
    {
        _client?.Dispose();
        await base.DisposeAsync();
    }
}
