# ConnectorHub — Specification

## Purpose

ConnectorHub is a .NET 10 Worker Service that detects data changes across heterogeneous
sources and forwards them to EventBridge via HTTP POST.

It has **one job**: detect → forward. All enrichment, serialization, and publishing
is handled by EventBridge.

---

## What Changes from UniversalConnector

### Removed
- `CommonModel.Runtime.Infrastructure` project entirely (NATS, protobuf, checkpoints, ontology)
- `NatsPublisher`, `NatsCheckpointStore`, `NatsConnectionFactory`
- `StartupSelfTestService` (NATS pre-flight checks)
- `HealthHeartbeatService` (heartbeats now sent by EventBridge)
- `DriverLifecycleService` (lifecycle commands routed through EventBridge)
- `FusekiOntologyCache`, `OntologyCacheRefreshService`
- `DefaultEventPipeline` (replaced by `HttpForwardingPipeline`)
- All protobuf / `envelope.proto` references
- NuGet: `NATS.Net`, `Google.Protobuf`, all NATS packages
- `FieldMapper` — field mapping moves to EventBridge

### Kept (unchanged)
- All protocol adapters: `PostgresAdapter`, `SqlServerAdapter`, `Neo4jAdapter`,
  `MongoDbAdapter`, `DatabricksAdapter`, `HttpRestAdapter`
- `BaseConnector`, `BaseProtocolAdapter`
- `ConnectorDescriptor` YAML system (`DescriptorLoader`, `DescriptorStore`,
  `DescriptorValidator`, `DescriptorBootstrapService`)
- `GenericConnector` engine (minus field mapping and snapshot/insert/update resolution)
- `ConnectorRegistry`, `AdapterRegistry`
- `LifecycleFsm`, `ConnectorPipelineService`
- `RawChangeRecord`, `RawChangeEvent`, `ChangeType`, `DriverState`
- Resilience / retry / circuit breaker in `BaseConnector`

### Changed
- `GenericConnector.PollOrStreamAsync` — instead of calling `FieldMapper.Apply()` and
  building a `RawChangeEvent`, it builds a `ChangeRequest` DTO and calls
  `IHttpChangeForwarder.ForwardAsync()`
- `ConnectorDescriptor.Nats` section → replaced by `ConnectorDescriptor.EventBridge`
  section (just a URL + api key reference)
- `ConnectorPipelineService` — no `IEventPipeline`; uses `IHttpChangeForwarder` directly
- `appsettings.json` — removes all `Nats` and `OntologyCache` sections; adds `EventBridge`

---

## Solution Structure

```
ConnectorHub/
├── src/
│   ├── ConnectorHub.Core/
│   │   ├── Abstractions/
│   │   │   ├── IProtocolAdapter.cs
│   │   │   ├── ISourceDriver.cs
│   │   │   ├── IConnectorRegistry.cs
│   │   │   ├── IHttpChangeForwarder.cs       ← NEW
│   │   │   └── IDriverLifecycleController.cs
│   │   ├── Models/
│   │   │   ├── RawChangeRecord.cs
│   │   │   ├── ChangeRequest.cs              ← NEW (replaces RawChangeEvent)
│   │   │   ├── ChangeType.cs
│   │   │   └── DriverState.cs
│   │   ├── Descriptors/
│   │   │   └── ConnectorDescriptor.cs        (EventBridgeOptions replaces NatsOptions)
│   │   └── Configuration/
│   │       └── ConnectorHubOptions.cs
│   │
│   ├── ConnectorHub.Drivers/
│   │   ├── Engine/
│   │   │   ├── BaseConnector.cs
│   │   │   ├── BaseProtocolAdapter.cs
│   │   │   ├── GenericConnector.cs           (simplified — no FieldMapper)
│   │   │   ├── ConnectorRegistry.cs
│   │   │   ├── AdapterRegistry.cs
│   │   │   ├── MultiSourceGenericFactory.cs
│   │   │   └── LifecycleFsm.cs
│   │   ├── Adapters/
│   │   │   ├── PostgresAdapter.cs
│   │   │   ├── SqlServerAdapter.cs
│   │   │   ├── Neo4jAdapter.cs
│   │   │   ├── MongoDbAdapter.cs
│   │   │   ├── DatabricksAdapter.cs
│   │   │   └── HttpRestAdapter.cs
│   │   └── Loading/
│   │       ├── DescriptorLoader.cs
│   │       ├── DescriptorStore.cs
│   │       ├── DescriptorValidator.cs
│   │       └── DescriptorBootstrapService.cs
│   │
│   └── ConnectorHub.Host/
│       ├── Program.cs
│       ├── Extensions/
│       │   └── ServiceCollectionExtensions.cs
│       ├── Services/
│       │   ├── ConnectorPipelineService.cs
│       │   └── HttpChangeForwarder.cs        ← NEW
│       └── appsettings.json
│
├── tests/
│   └── ConnectorHub.Tests/
├── connectors/                               (YAML descriptors)
├── docker/
└── docker-compose.yml
```

---

## New: `ChangeRequest` model

```csharp
public sealed record ChangeRequest
{
    public string RequestId { get; init; } = Ulid.NewUlid().ToString();
    public required string DriverId { get; init; }
    public required string SourceType { get; init; }
    public string? Context { get; init; }
    public required string EntityPath { get; init; }
    public required string ChangeType { get; init; }    // enum .ToString()
    public DateTimeOffset DetectedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? SourceTimestamp { get; init; }
    public IReadOnlyDictionary<string, string> PrimaryKey { get; init; }
    public IReadOnlyDictionary<string, string> Fields { get; init; }
    public IReadOnlyDictionary<string, string>? PreviousFields { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; }
}
```

All field values are serialized as `string` (same as protobuf `map<string,string>`).
`null` field values are serialized as the literal string `""`.

---

## New: `IHttpChangeForwarder`

```csharp
public interface IHttpChangeForwarder
{
    Task ForwardAsync(ChangeRequest request, CancellationToken ct);
}
```

Implementation `HttpChangeForwarder`:
- Injects `IHttpClientFactory` (named client `"eventbridge"`)
- Serializes `ChangeRequest` to JSON (`System.Text.Json`)
- Posts to `{EventBridgeUrl}/api/changes` with `X-Api-Key` header
- On `202` → success
- On `409` → log warning and return (idempotent duplicate)
- On `4xx` → throw `InvalidOperationException` (descriptor / config error)
- On `5xx` or network error → throw (triggers BaseConnector retry loop)
- Logs at `Debug` the full JSON payload (same debug pattern as NatsPublisher)

---

## Updated: `ConnectorDescriptor` — `eventBridge` section

```yaml
eventBridge:
  url: "http://eventbridge:8080"      # EventBridge base URL
  apiKey: "${EVENTBRIDGE_API_KEY}"    # Resolved from env var
  timeoutSeconds: 10                  # HTTP timeout (default: 10)
  subjectTemplate: null               # Optional subject hint forwarded as metadata
  additionalMetadata:
    domain: asset-management          # Extra key/values added to metadata{}
```

The old `nats:` section is removed from descriptors.
The old `fieldMapping:` section is removed — field mapping is now in EventBridge.

---

## Updated: `appsettings.json`

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "ConnectorHub": "Debug"
    }
  },
  "GenericConnector": {
    "DescriptorDirectory": "connectors",
    "FailOnDescriptorError": true
  },
  "EventBridge": {
    "DefaultUrl": "http://localhost:5100",
    "DefaultApiKey": ""
  }
}
```

Each descriptor overrides `url` and `apiKey` independently — useful when multiple
EventBridge instances are deployed per environment.

---

## Updated: `GenericConnector.PollOrStreamAsync`

```csharp
protected override async IAsyncEnumerable<RawChangeEvent> PollOrStreamAsync(
    [EnumeratorCancellation] CancellationToken ct)
{
    await foreach (var raw in _adapter.StreamRawChangesAsync(_descriptor, ct))
    {
        var request = new ChangeRequest
        {
            DriverId        = _descriptor.DriverId,
            SourceType      = _descriptor.SourceType,
            Context         = _descriptor.Context,
            EntityPath      = raw.EntityPath,
            ChangeType      = raw.ChangeType.ToString(),
            SourceTimestamp = raw.SourceTimestamp,
            PrimaryKey      = BuildStringDict(raw.Fields, entityConfig?.PrimaryKey),
            Fields          = ToStringDict(raw.Fields),
            PreviousFields  = raw.PreviousFields.Count > 0
                                ? ToStringDict(raw.PreviousFields)
                                : null,
            Metadata        = BuildMetadata(raw.AdapterMetadata,
                                            _descriptor.EventBridge.AdditionalMetadata)
        };

        await _forwarder.ForwardAsync(request, ct);

        // Still yield a minimal event so ConnectorPipelineService can track health
        yield return new RawChangeEvent { ... };
    }
}
```

> **Note:** `ChangeType.Snapshot` resolution (Insert vs Update) is removed from
> ConnectorHub. It moves to EventBridge, which has access to the checkpoint store
> and can make the determination based on whether the event's PK has been seen before.

---

## Connector YAML — Updated example (`postgres-aveva.yaml`)

```yaml
driverId: pg-aveva
context: ctx:asset-management
sourceType: postgres
description: AVEVA assets database — Postgres polling
enabled: true

connection:
  host: "localhost"
  port: 5433
  database: "aveva_db"
  username: "connector"
  password: "12345"

changeDetection:
  mode: polling
  pollIntervalSeconds: 30
  watermarkColumn: updated_at
  lookbackDuration: PT1H

watch:
  autoDiscover: false
  entities:
    - name: public.assets
      primaryKey: [asset_id]
    - name: public.locations
      primaryKey: [location_id]

eventBridge:
  url: "http://localhost:5100"
  apiKey: "${EVENTBRIDGE_API_KEY}"
  additionalMetadata:
    domain: asset-management

resilience:
  maxConsecutiveFailures: 5
  retryDelaySeconds: 10
  backoffMultiplier: 1.5
  maxRetryDelaySeconds: 120
```

---

## NuGet packages (ConnectorHub.Host.csproj)

```xml
<PackageReference Include="Microsoft.Extensions.Hosting"      Version="10.*" />
<PackageReference Include="Npgsql"                            Version="10.*" />
<PackageReference Include="Microsoft.Data.SqlClient"          Version="7.*"  />
<PackageReference Include="Neo4j.Driver"                      Version="6.*"  />
<PackageReference Include="MongoDB.Driver"                    Version="3.*"  />
<PackageReference Include="YamlDotNet"                        Version="17.*" />
```

NATS and protobuf packages are **not** referenced.

---

## Health endpoint

ConnectorHub exposes a minimal health endpoint (no NATS dependency):
```
GET http://localhost:8080/health
```

Returns:
```json
{
  "status": "Healthy",
  "drivers": {
    "pg-aveva":   { "state": "Streaming", "eventsForwarded": 142 },
    "neo4j-graph": { "state": "Streaming", "eventsForwarded": 38 }
  }
}
```

---

## Testing

Tests follow the same structure as UniversalConnector.
Key new test areas:
- `HttpChangeForwarderTests` — verifies JSON payload, headers, retry on 5xx, skip on 409
- `GenericConnectorTests` — verifies `ChangeRequest` is built correctly from `RawChangeRecord`
- `DescriptorLoaderTests` — verifies `eventBridge` section loads; old `nats` section absent
