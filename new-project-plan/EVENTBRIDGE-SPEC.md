# EventBridge — Specification

## Purpose

EventBridge is the central processing microservice. It receives `ChangeRequest` payloads
over HTTP from any connector (ConnectorHub, Debezium, or custom), applies enrichment and
field mapping, serializes to protobuf, and publishes to NATS JetStream.

It is the **only** component that talks to NATS or knows about protobuf.

---

## Solution Structure

```
EventBridge/
├── src/
│   ├── EventBridge.Core/
│   │   ├── Models/
│   │   │   ├── ChangeRequest.cs          ← same DTO as ConnectorHub sends
│   │   │   ├── ChangeEvent.cs            ← enriched domain model (internal)
│   │   │   ├── ChangeType.cs
│   │   │   ├── Checkpoint.cs
│   │   │   └── OntologyEntry.cs
│   │   ├── Abstractions/
│   │   │   ├── IChangePipeline.cs
│   │   │   ├── INatsPublisher.cs
│   │   │   ├── ICheckpointStore.cs
│   │   │   ├── IOntologyCache.cs
│   │   │   ├── IFieldMappingService.cs
│   │   │   └── IDebeziumTranslator.cs
│   │   └── Configuration/
│   │       ├── NatsOptions.cs
│   │       ├── HeartbeatOptions.cs
│   │       ├── OntologyCacheOptions.cs
│   │       └── EventBridgeOptions.cs
│   │
│   ├── EventBridge.Infrastructure/
│   │   ├── Nats/
│   │   │   ├── NatsConnectionFactory.cs
│   │   │   ├── NatsPublisher.cs          ← protobuf Envelope → NATS
│   │   │   ├── NatsCheckpointStore.cs
│   │   │   └── StartupSelfTestService.cs
│   │   ├── Protos/
│   │   │   └── envelope.proto            ← same proto as before
│   │   ├── Mapping/
│   │   │   ├── FieldMappingService.cs    ← moved from ConnectorHub
│   │   │   ├── MappingRuleLoader.cs      ← loads mapping-rules.yaml
│   │   │   └── SubjectTemplateResolver.cs
│   │   └── Ontology/
│   │       ├── FusekiOntologyCache.cs
│   │       └── OntologyCacheRefreshService.cs
│   │
│   └── EventBridge.Api/
│       ├── Program.cs
│       ├── Extensions/
│       │   └── ServiceCollectionExtensions.cs
│       ├── Controllers/
│       │   ├── ChangeController.cs       ← POST /api/changes
│       │   └── DebeziumController.cs     ← POST /api/debezium
│       ├── Middleware/
│       │   └── ApiKeyMiddleware.cs
│       ├── Pipeline/
│       │   ├── DefaultChangePipeline.cs
│       │   ├── EnvelopeBuilder.cs
│       │   └── DebeziumTranslator.cs
│       ├── Health/
│       │   └── NatsHealthCheck.cs
│       └── appsettings.json
│
├── config/
│   ├── mapping-rules.yaml               ← field mapping rules per driverId
│   └── debezium-mappings.yaml           ← context/config per Debezium connector name
│
├── tests/
│   └── EventBridge.Tests/
├── docker/
│   └── nats/
│       └── nats.conf
└── docker-compose.yml
```

---

## API Endpoints

### `POST /api/changes`

Receives `ChangeRequest` JSON from any .NET connector.

```
Headers:
  Content-Type: application/json
  X-Api-Key: <key>

Body: ChangeRequest (see CONTRACT.md)

Responses:
  202 { requestId, eventId, subject, accepted: true }
  400 { requestId, errors[] }
  401
  409 (duplicate requestId)
  500
```

### `POST /api/debezium`

Receives Debezium Server HTTP sink payload (CloudEvents / Debezium JSON).
Translates to `ChangeRequest` via `DebeziumTranslator`, then runs the same pipeline.

```
Headers:
  Content-Type: application/json
  X-Api-Key: <key>

Body: Debezium change event (see CONTRACT.md §2)

Responses: same as /api/changes
```

### `GET /health`

Standard .NET health check endpoint. Checks NATS connectivity.

### `GET /metrics`

Prometheus metrics (optional, via `prometheus-net`):
- `eb_events_received_total{driver_id, source_type}`
- `eb_events_published_total{driver_id}`
- `eb_events_dlq_total`
- `eb_publish_duration_seconds`

---

## Processing Pipeline (`DefaultChangePipeline`)

```csharp
public async Task<PipelineResult> ProcessAsync(ChangeRequest req, CancellationToken ct)
{
    // 1. Idempotency check
    if (await _idempotency.IsProcessedAsync(req.RequestId)) return Duplicate;

    // 2. Load field mapping rules for this driverId
    var rules = _mappingLoader.GetRules(req.DriverId);

    // 3. Apply field mapping (rename, cast, exclude, isKey)
    var (primaryKey, fields, prevFields) = _fieldMapper.Apply(
        req.Fields, req.PreviousFields, rules);

    // 4. Resolve Insert vs Update (via checkpoint store)
    var resolvedChangeType = ResolveChangeType(req.ChangeType, req.DriverId,
                                               req.EntityPath, primaryKey);

    // 5. Ontology enrichment (no-op if cache disabled)
    fields = await _ontology.EnrichAsync(fields, req.Context);

    // 6. Build domain ChangeEvent
    var evt = new ChangeEvent { ... };

    // 7. Build protobuf Envelope
    var envelope = _envelopeBuilder.Build(evt);

    // 8. Compute NATS subject
    var subject = _subjectResolver.Resolve(evt);

    // 9. Publish to NATS
    await _publisher.PublishAsync(envelope, subject, ct);

    // 10. Save checkpoint
    await _checkpoint.SaveAsync(evt.DriverId, evt.EntityPath,
                                evt.SourceTimestamp?.ToString("O") ?? evt.EventId, ct);

    // 11. Mark as processed (idempotency)
    await _idempotency.MarkProcessedAsync(req.RequestId);

    return new PipelineResult { EventId = evt.EventId, Subject = subject };
}
```

---

## Field Mapping Service

Moved from `FieldMapper` in ConnectorHub. Same logic, now driven by `mapping-rules.yaml`.

### `config/mapping-rules.yaml`

```yaml
# Rules are applied per driverId. Rules under "*" apply to all drivers.

"*":
  - source: created_at
    type: timestamp
  - source: updated_at
    type: timestamp

"pg-aveva":
  - source: install_date
    type: timestamp
  - source: last_maintenance_date
    type: timestamp
  - source: asset_id
    isKey: true

"pg-orders":
  - source: id
    isKey: true
  - source: total_amount
    type: double

"neo4j-graph":
  - source: createdAt
    type: timestamp
  - source: updatedAt
    type: timestamp
```

---

## Debezium Translator (`DebeziumTranslator`)

Converts Debezium JSON payload → `ChangeRequest`.

```csharp
public ChangeRequest Translate(JsonElement debeziumPayload, string? overrideDriverId = null)
{
    var payload = debeziumPayload.GetProperty("payload");
    var source  = payload.GetProperty("source");

    var op = payload.GetProperty("op").GetString();  // c/u/d/r
    var changeType = op switch {
        "c" => "Insert",
        "u" => "Update",
        "d" => "Delete",
        "r" => "Snapshot",
        _   => "Snapshot"
    };

    var driverId   = overrideDriverId ?? source.GetProperty("name").GetString()!;
    var schema     = source.TryGetProperty("schema", out var s) ? s.GetString() : "public";
    var table      = source.GetProperty("table").GetString()!;
    var entityPath = $"{schema}.{table}";
    var context    = _mappings.GetContext(driverId);  // from debezium-mappings.yaml

    var fields         = ExtractFields(payload, "after");
    var previousFields = ExtractFields(payload, "before");
    var tsMs           = source.GetProperty("ts_ms").GetInt64();
    var sourceTs       = DateTimeOffset.FromUnixTimeMilliseconds(tsMs);

    return new ChangeRequest
    {
        RequestId       = Ulid.NewUlid().ToString(),
        DriverId        = driverId,
        SourceType      = source.GetProperty("connector").GetString()!,
        Context         = context,
        EntityPath      = entityPath,
        ChangeType      = changeType,
        SourceTimestamp = sourceTs,
        Fields          = fields,
        PreviousFields  = previousFields.Count > 0 ? previousFields : null,
        Metadata        = new Dictionary<string,string>
        {
            ["debezium_version"] = source.GetProperty("version").GetString()!,
            ["snapshot"]         = source.TryGetProperty("snapshot", out var snap)
                                    ? snap.GetString() ?? "false" : "false"
        }
    };
}
```

### `config/debezium-mappings.yaml`

```yaml
# Maps Debezium connector name → EventBridge context and optional overrides

"pg-orders-debezium":
  context: ctx:order-management
  sourceType: postgres

"sqlserver-crm-debezium":
  context: ctx:crm
  sourceType: sqlserver

"mysql-products-debezium":
  context: ctx:catalog
  sourceType: mysql
```

---

## NATS Publisher

Same implementation as UniversalConnector `NatsPublisher`:

- Builds protobuf `Envelope` from `ChangeEvent`
- Logs JSON at `Debug` level (`Google.Protobuf.JsonFormatter`)
- Retry: 4 attempts, delays `[100ms, 1s, 10s]`
- Circuit breaker: 5 failures → open 30s → half-open
- DLQ: `cdc.dlq.{original-subject}` via core NATS

NATS headers on every message (unchanged from UniversalConnector):
```
eventId, driverId, context, sourceType, changeType, content-type
```

---

## Idempotency Store

Short-lived cache to prevent duplicate processing when a connector retries.

Default: in-memory `ConcurrentDictionary<string, DateTimeOffset>` with 5-minute TTL.
Production option: NATS KV bucket `eb-idempotency` with 5-minute TTL.

Configured via:
```json
"Idempotency": {
  "Provider": "Memory",   // "Memory" | "NatsKv"
  "TtlMinutes": 5
}
```

---

## Resolve Insert vs Update

Without ConnectorHub's snapshot cache, EventBridge uses the checkpoint store:

```csharp
private string ResolveChangeType(string raw, string driverId,
                                  string entityPath, IReadOnlyDictionary<string,string> pk)
{
    if (raw != "Snapshot") return raw;   // CDC adapters set exact type

    // Polling adapters emit "Snapshot" — resolve via checkpoint
    var checkpointKey = $"{driverId}:{entityPath}:{string.Join(":", pk.Values)}";
    var exists = _checkpoint.ExistsAsync(checkpointKey).GetAwaiter().GetResult();
    return exists ? "Update" : "Insert";
}
```

---

## Heartbeat Service

`HealthHeartbeatService` moves to EventBridge (not ConnectorHub).

EventBridge aggregates health status from all connectors that have forwarded events
recently and publishes to `cdc.health.eventbridge` every 30 seconds.

ConnectorHub exposes its own `/health` HTTP endpoint; EventBridge polls it optionally.

---

## `appsettings.json`

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "EventBridge": "Debug"
    }
  },
  "Nats": {
    "Servers": ["nats://localhost:4222"],
    "SubjectPrefix": "cdc",
    "UseJetStream": true,
    "DlqSubjectPrefix": "cdc.dlq",
    "CheckpointBucket": "cm-checkpoints",
    "StopOnCriticalFailure": false
  },
  "OntologyCache": {
    "EndpointUrl": null,
    "LoadOnStartup": false,
    "RefreshSubject": "cdc.ontology.refresh"
  },
  "Heartbeat": {
    "IntervalSeconds": 30,
    "SubjectPrefix": "cdc.health",
    "UseJetStream": false
  },
  "Idempotency": {
    "Provider": "Memory",
    "TtlMinutes": 5
  },
  "EventBridge": {
    "ApiKeys": ["change-me-in-production"],
    "MappingRulesPath": "config/mapping-rules.yaml",
    "DebeziumMappingsPath": "config/debezium-mappings.yaml"
  }
}
```

---

## NuGet packages

```xml
<!-- EventBridge.Api -->
<PackageReference Include="Microsoft.AspNetCore"          Version="10.*" />

<!-- EventBridge.Infrastructure -->
<PackageReference Include="NATS.Net"                      Version="2.7.*" />
<PackageReference Include="Google.Protobuf"               Version="3.27.*" />
<PackageReference Include="Grpc.Tools"                    Version="2.65.*" PrivateAssets="All"/>
<PackageReference Include="YamlDotNet"                    Version="17.*" />

<!-- Optional metrics -->
<PackageReference Include="prometheus-net.AspNetCore"     Version="8.*" />
```

---

## Docker Compose

```yaml
services:
  eventbridge:
    build:
      context: .
      dockerfile: Dockerfile
    image: eventbridge:latest
    container_name: eventbridge
    restart: unless-stopped
    environment:
      DOTNET_ENVIRONMENT: Production
      Nats__Servers__0: nats://nats:4222
      EventBridge__ApiKeys__0: ${EVENTBRIDGE_API_KEY}
    ports:
      - "5100:8080"
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8080/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 10
    networks:
      - connector-net
```

---

## Testing

| Test file | What is tested |
|-----------|---------------|
| `DefaultChangePipelineTests` | Full pipeline: mapping → ontology → envelope → publish |
| `DebeziumTranslatorTests` | All `op` types, null `before`, field extraction |
| `FieldMappingServiceTests` | Rename, cast, exclude, isKey, staticValue |
| `NatsPublisherTests` | Circuit breaker, retry, DLQ |
| `ApiKeyMiddlewareTests` | Missing key, wrong key, valid key |
| `ChangeControllerTests` | 202/400/409/500 response codes |
| `DebeziumControllerTests` | Translation + pipeline integration |
| `IdempotencyStoreTests` | Duplicate detection, TTL expiry |
| `CheckpointStoreTests` | Save/load round-trip |
| `MappingRuleLoaderTests` | YAML loading, wildcard merge, per-driver override |
