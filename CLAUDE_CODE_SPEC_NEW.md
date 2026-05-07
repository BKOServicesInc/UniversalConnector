# UniversalConnector — Claude Code Project Specification

> **Read this file first.** It is the authoritative reference for the current state of this
> codebase. Use it to understand architecture, conventions, known issues, and how to extend
> the project before making any changes.

---

## 1. What this project is

A **.NET 10 Worker Service** that connects to any supported data source purely from YAML
configuration files (descriptors), detects data changes via CDC or polling, and:

1. Publishes a canonical `DataChangeEvent` JSON message to **NATS** (JetStream-capable).
2. Persists every event to a **PostgreSQL** table (`CDCDB.data_changes`) via Dapper.

Zero code changes are required when adding new source instances — only a new YAML descriptor
is needed.

**Supported source types:** `postgres`, `sqlserver`, `neo4j`, `databricks`, `mongodb`,
`seeq`, `avevapi`, `sharepoint`, `sap`.

---

## 2. Solution structure

```
UniversalConnector/
├── src/
│   ├── UniversalConnector.Core/              # Shared contracts, models, descriptors
│   │   ├── Abstractions/
│   │   │   ├── IDataSourceConnector.cs
│   │   │   ├── IConnectorRegistry.cs
│   │   │   ├── IProtocolAdapter.cs           # + RawChangeRecord sealed class
│   │   │   ├── IDataSink.cs                  # WriteAsync(DataChangeEvent) interface
│   │   │   ├── INatsPublisher.cs
│   │   │   └── BaseConnector.cs              # retry loop, health, sequence numbering
│   │   ├── Configuration/
│   │   │   └── ConnectorOptions.cs           # NatsOptions, PostgresSinkOptions
│   │   ├── Descriptors/
│   │   │   ├── ConnectorDescriptor.cs        # full descriptor model (all sources)
│   │   │   └── IDescriptorLoader.cs          # loader interface + result/validation types
│   │   └── Models/
│   │       ├── DataChangeEvent.cs            # canonical output event (sealed record)
│   │       └── ConnectorHealthReport.cs
│   │
│   ├── UniversalConnector.Nats/              # NATS publisher
│   │   └── NatsPublisher.cs                  # implements INatsPublisher
│   │
│   ├── UniversalConnector.Generic/           # Descriptor-driven engine (primary path)
│   │   ├── Adapters/
│   │   │   ├── BaseProtocolAdapter.cs        # open/close guard, DisposeAsync
│   │   │   ├── PostgresAdapter.cs            # WAL CDC (pgoutput) + watermark polling
│   │   │   ├── SqlServerAdapter.cs           # Change Tracking CDC + watermark polling
│   │   │   ├── Neo4jAdapter.cs               # Bolt watermark polling (nodes + rels)
│   │   │   ├── DatabricksAdapter.cs          # Delta CDF + ODBC watermark polling
│   │   │   ├── MongoDbAdapter.cs             # Change Streams CDC + watermark polling
│   │   │   └── HttpRestAdapter.cs            # SharePoint/SAP/Seeq/AVEVA PI
│   │   ├── Configuration/
│   │   │   └── GenericConnectorOptions.cs    # DescriptorDirectory, FailOnDescriptorError
│   │   ├── Engine/
│   │   │   ├── AdapterRegistry.cs            # sourceType → IProtocolAdapter lookup
│   │   │   ├── DescriptorBootstrapService.cs # IHostedService: loads files at startup
│   │   │   ├── DescriptorLoader.cs           # file/string loader + env-var interpolation
│   │   │   ├── DescriptorStore.cs            # in-memory cache of loaded descriptors
│   │   │   ├── DescriptorValidator.cs        # validation rules
│   │   │   ├── GenericConnector.cs           # BaseConnector impl; snapshot cache for previous_payload
│   │   │   ├── GenericConnectorFactory.cs    # IConnectorFactory (sourceType = "*")
│   │   │   └── MultiSourceGenericFactory.cs  # resolves descriptor → adapter → GenericConnector
│   │   ├── Extensions/
│   │   │   └── GenericConnectorExtensions.cs # AddGenericConnector() DI helper
│   │   └── Mapping/
│   │       └── FieldMapper.cs                # applies fieldMapping rules from descriptor
│   │
│   ├── UniversalConnector.Connectors/        # DEAD CODE — never registered in DI
│   │   └── (concrete connectors — not in active use; see Known Issues §11)
│   │
│   └── UniversalConnector.Host/              # Executable Worker host
│       ├── ConnectorPipelineService.cs       # BackgroundService: streams + publishes + sinks
│       ├── ConnectorRegistry.cs              # resolves connectors from DescriptorStore
│       ├── Program.cs
│       ├── appsettings.json
│       ├── Properties/launchSettings.json
│       ├── Extensions/
│       │   └── ServiceCollectionExtensions.cs
│       └── Services/
│           └── PostgresDataSink.cs           # implements IDataSink via Dapper
│
├── connectors/                               # YAML descriptor files (one per source)
│   ├── postgres-orders.yaml
│   ├── sqlserver-crm.yaml
│   ├── neo4j-graph.yaml
│   ├── databricks-lakehouse.yaml
│   ├── mongodb-assets.yaml
│   ├── sharepoint-docs.yaml
│   ├── sap-s4hana.yaml
│   ├── seeq-plant.yaml
│   └── avevapi-historian.yaml
│
├── debezium-server/                          # Debezium Server config (planned, not yet built)
│   └── application.properties
│
├── docker-compose.yml                        # PostgreSQL + SQL Server infra
├── docker-compose.nats.yml                   # NATS, MongoDB, Neo4j, mongo-express, NUI
├── Dockerfile
└── _FIRST.md                                 # ← this file
```

---

## 3. Technology stack

| Concern | Technology | Version |
|---|---|---|
| Runtime | .NET Worker Service | 10.0 |
| Messaging | NATS (NATS.Net) | 2.x |
| PostgreSQL driver | Npgsql | 10.0.2 |
| Postgres replication | Npgsql.Replication (pgoutput) | 10.0.2 |
| SQL Server | Microsoft.Data.SqlClient | 7.0.1 |
| Neo4j | Neo4j.Driver | 6.0.0 |
| MongoDB | MongoDB.Driver | 3.x |
| ODBC (Databricks) | System.Data.Odbc | 10.0.7 |
| Micro-ORM | Dapper | 2.x |
| YAML parsing | YamlDotNet | 17.1.0 |
| DI / Hosting | Microsoft.Extensions.Hosting | 10.0.7 |

---

## 4. Key models

### `DataChangeEvent` (Core/Models)
```csharp
public sealed record DataChangeEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString();
    public DateTimeOffset DetectedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? SourceTimestamp { get; init; }
    public required string SourceType { get; init; }
    public required string ConnectorId { get; init; }
    public required string EntityPath { get; init; }
    public ChangeType ChangeType { get; init; }          // enum: Insert|Update|Delete|Snapshot|SchemaChange|Heartbeat
    public IReadOnlyDictionary<string, object?> PrimaryKey { get; init; }
    public IReadOnlyDictionary<string, object?> Payload { get; init; }
    public IReadOnlyDictionary<string, object?>? PreviousPayload { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; }
    public long SequenceNumber { get; init; }
    public string SchemaVersion { get; init; } = "1.0";
}
```

### `RawChangeRecord` (Core/Abstractions/IProtocolAdapter.cs)
Internal transfer object between adapter and `GenericConnector`. Never published externally.

```csharp
public sealed class RawChangeRecord
{
    public required string EntityPath { get; init; }
    public required ChangeType ChangeType { get; init; }
    public DateTimeOffset? SourceTimestamp { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; }        // current state
    public IReadOnlyDictionary<string, object?> PreviousFields { get; init; } // before state (CDC only)
    public IReadOnlyDictionary<string, string> AdapterMetadata { get; init; }
}
```

---

## 5. Interfaces

### `IProtocolAdapter`
```csharp
public interface IProtocolAdapter : IAsyncDisposable
{
    string SourceType { get; }
    Task OpenAsync(ConnectorDescriptor descriptor, CancellationToken ct);
    Task CloseAsync(CancellationToken ct);
    IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(ConnectorDescriptor descriptor, CancellationToken ct);
    IReadOnlyList<string> Validate(ConnectorDescriptor descriptor);
}
```

### `INatsPublisher`
```csharp
public interface INatsPublisher : IAsyncDisposable
{
    Task PublishAsync(
        DataChangeEvent evt,
        string? subjectOverride = null,
        IReadOnlyDictionary<string, string>? additionalHeaders = null,
        CancellationToken ct = default);
}
```
NATS subject pattern: `{SubjectPrefix}.{sourceType}.{connectorId}.{changeType}` (all lowercase).
Default prefix: `universal-connector`.

### `IDataSink`
```csharp
public interface IDataSink : IAsyncDisposable
{
    Task WriteAsync(DataChangeEvent evt, CancellationToken ct = default);
}
```
Implemented by `PostgresDataSink`. Sink failures are swallowed (logged at Error) and must never
block or throw to the NATS publish path.

### `IDescriptorLoader`
```csharp
public interface IDescriptorLoader
{
    Task<IReadOnlyList<DescriptorLoadResult>> LoadFromDirectoryAsync(string directoryPath, CancellationToken ct);
    Task<DescriptorLoadResult> LoadFromFileAsync(string filePath, CancellationToken ct);
    DescriptorLoadResult LoadFromString(string content, string format = "yaml", string filePath = "");
    DescriptorValidationResult Validate(ConnectorDescriptor descriptor);
}
```

---

## 6. Configuration

### `appsettings.json` (full current state)
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.Hosting.Lifetime": "Information",
      "UniversalConnector": "Debug"
    }
  },
  "Nats": {
    "Servers": ["nats://localhost:4222"],
    "SubjectPrefix": "universal-connector",
    "UseJetStream": false
  },
  "GenericConnector": {
    "DescriptorDirectory": "C:\\Repos\\UniversalConnector\\connectors",
    "FailOnDescriptorError": false
  },
  "PostgresSink": {
    "Enabled": true,
    "ConnectionString": "Host=localhost;Port=5432;Database=cdcdb;Username=postgres;Password=12345"
  }
}
```

### `NatsOptions` (Core/Configuration/ConnectorOptions.cs)
```csharp
public class NatsOptions
{
    public string[] Servers { get; set; } = ["nats://localhost:4222"];
    public string SubjectPrefix { get; set; } = "universal-connector";
    public bool UseJetStream { get; set; } = false;
    public string? CredsFile { get; set; }
}
```

### `PostgresSinkOptions`
```csharp
public class PostgresSinkOptions
{
    public bool Enabled { get; set; } = true;
    public string ConnectionString { get; set; } = "";
}
```

> **Known issue (I-10):** `GenericConnectorOptions` is currently only bound to config when
> `AddGenericConnector(opts => configuration.GetSection("GenericConnector").Bind(opts))` is
> called explicitly. The `ServiceCollectionExtensions.AddUniversalConnector` does NOT currently
> pass the bind action, so `DescriptorDirectory` defaults to the hardcoded Windows path. Fix:
> pass the configuration bind action from `AddUniversalConnector`.

---

## 7. Descriptor YAML schema

Every file in `connectors/` deserializes into `ConnectorDescriptor`.

```yaml
connectorId: string          # required, unique
sourceType: string           # required — see §9 for valid values
description: string          # optional
enabled: bool                # default true; false = skipped at startup

connection:
  connectionString: string   # full ADO.NET string (overrides individual fields)
  host: string
  port: int
  database: string
  username: string
  password: string           # use "${ENV_VAR}" — see interpolation below
  baseUrl: string            # HTTP-based sources
  apiToken: string
  tenantId: string           # AAD / SharePoint
  clientId: string
  clientSecret: string
  httpPath: string           # Databricks SQL warehouse
  uri: string                # MongoDB / Neo4j connection URI
  verifySsl: bool            # default true
  sslCertPath: string
  sapClient: string          # SAP client number
  piServerName: string       # AVEVA PI server name

changeDetection:
  mode: cdc|polling|delta    # required (valid values depend on sourceType — see §9)
  pollIntervalSeconds: int   # default 30
  watermarkColumn: string    # default "updated_at"
  lookbackDuration: string   # ISO 8601, default "PT1H"
  replicationSlot: string    # postgres CDC, default "uc_slot"
  publication: string        # postgres CDC, default "uc_pub"
  startingVersion: long      # databricks delta, default -1 (latest)
  autoEnableChangeTracking: bool  # sqlserver, default true

watch:
  autoDiscover: bool         # discover all entities (adapter-specific)
  entities:
    - name: string           # table / label / tag / list / subject
      primaryKey: [string]   # PK column/property names
      filter: string         # optional WHERE / $filter / Cypher clause
      changeDetectionOverride: string

fieldMapping:
  - source: string           # required: source field name
    target: string           # output field name (default = source)
    type: string             # string|int|long|double|bool|timestamp|date
    exclude: bool            # drop from output
    isKey: bool              # promote to primaryKey dict
    staticValue: any         # inject constant

nats:
  subjectOverride: string    # override computed subject
  serializationFormat: json  # default json
  additionalHeaders:
    key: value               # merged into every event's Metadata dict

resilience:
  maxConsecutiveFailures: int   # default 5
  retryDelaySeconds: int        # default 10
  backoffMultiplier: double     # default 1.5
  maxRetryDelaySeconds: int     # default 120
```

**Environment variable interpolation:** any `${VAR_NAME}` anywhere in any string value is
replaced at load time. If the variable is not set, an `InvalidOperationException` is thrown
and the descriptor fails to load (logged; startup continues unless `FailOnDescriptorError: true`).

---

## 8. Pipeline flow

```
Startup
  └─ DescriptorBootstrapService.StartAsync()
       └─ DescriptorLoader.LoadFromDirectoryAsync("connectors/")
            ├─ interpolate env vars
            ├─ deserialize YAML → ConnectorDescriptor
            ├─ validate (DescriptorValidator)
            └─ register in DescriptorStore (enabled descriptors only)

ConnectorPipelineService.ExecuteAsync() [BackgroundService]
  └─ ConnectorRegistry.ResolveAll()
       └─ MultiSourceGenericFactory.Create(connectorId) per descriptor
            └─ new GenericConnector(descriptor, adapter, fieldMapper)
  └─ per connector (parallel Task):
       ├─ connector.ConnectAsync()          → adapter.OpenAsync()
       ├─ connector.StreamChangesAsync()    → BaseConnector retry loop
       │    └─ GenericConnector.PollOrStreamAsync()
       │         ├─ adapter.StreamRawChangesAsync()  → yields RawChangeRecord
       │         ├─ snapshot cache: inject PreviousFields if adapter didn't (polling path)
       │         └─ FieldMapper.Apply() → (primaryKey, payload, previousPayload)
       │              └─ yield DataChangeEvent
       ├─ INatsPublisher.PublishAsync(evt)
       └─ IDataSink.WriteAsync(evt)         → PostgresDataSink → data_changes table
```

---

## 9. Adapter reference

### PostgresAdapter (`sourceType: postgres`)
| Mode | Mechanism |
|---|---|
| `cdc` | Npgsql logical replication (pgoutput). Validates `wal_level = logical` before slot creation. `FullUpdateMessage` → `PreviousFields` populated. `UpdateMessage` (no REPLICA IDENTITY FULL) → `PreviousFields` empty (snapshot cache fills it). |
| `polling` | `SELECT * FROM {table} WHERE {watermarkCol} > @since ORDER BY {watermarkCol}` |

### SqlServerAdapter (`sourceType: sqlserver`)
| Mode | Mechanism |
|---|---|
| `cdc` | `CHANGETABLE(CHANGES {table}, @from)` polled every interval. Auto-enables CT on DB + tables when `autoEnableChangeTracking: true`. Tracks `_versions[table]`. No `PreviousFields` — snapshot cache fills on second change. |
| `polling` | Watermark column poll. |

### Neo4jAdapter (`sourceType: neo4j`)
- Polling only. Nodes: `MATCH (n:Label) WHERE n.{watermark} > datetime({epochMillis: $since})`.
- Relationships: entity name prefixed `REL:` (e.g. `REL:LOCATED_AT`). Uses `()-[r:TYPE]->()`.
- `autoDiscover: true` → `CALL db.labels()`.
- **Known issue (W-16):** relationship `primaryKey: [id]` resolves to `∅` — use `_elementId`
  as the PK column name instead, or inject it via `AdapterMetadata`.

### MongoDbAdapter (`sourceType: mongodb`)
| Mode | Mechanism |
|---|---|
| `cdc` | `database.WatchAsync()` with `FullDocumentOption.UpdateLookup` + `FullDocumentBeforeChange.WhenAvailable`. Resume token held in memory. Requires replica set + pre-image capture enabled on collections. |
| `polling` | `collection.Find(filter).Sort(sort).ToCursorAsync()` with watermark. |

`BsonDocument` is flattened to `Dictionary<string, object?>`: ObjectId→string, DateTime→DateTimeOffset, nested doc→dict, array→List.

### DatabricksAdapter (`sourceType: databricks`)
| Mode | Mechanism |
|---|---|
| `cdc` | `table_changes('{table}', {version})`. Skips `update_preimage` rows. Tracks `_versions[table] = maxVersion + 1`. |
| `polling` | Watermark column poll via Simba Spark ODBC. |

### HttpRestAdapter (`sourceType: sharepoint | sap | seeq | avevapi`)
One class, registered 4× with different `SourceType` values via factory lambda.

| Source | Mode | Auth | Mechanism |
|---|---|---|---|
| `sharepoint` | `delta` | AAD client-credentials | Microsoft Graph `items/delta`, caches `@odata.deltaLink` per list |
| `sap` | `delta` | Basic + `sap-client` header | OData V4 `$trackChanges`, caches `@odata.deltaLink` |
| `seeq` | `polling` | POST `/api/v1/auth/login` → `sq-auth` header | GET `/api/v1/signals/{id}/samples` |
| `avevapi` | `polling` | Basic/NTLM | GET `/piwebapi/streams/{webId}/recorded` |

---

## 10. Snapshot cache (`GenericConnector`)

Polling adapters never populate `PreviousFields`. `GenericConnector.PollOrStreamAsync` maintains
an in-memory `Dictionary<string, IReadOnlyDictionary<string, object?>> _snapshots` to fill this
gap for all adapters:

- **Key:** `"{entityPath}:{pk1Value}:{pk2Value}:..."` built from `entityConfig.PrimaryKey` columns.
- **On each record:** if `raw.PreviousFields.Count == 0`, look up `_snapshots[key]` and use it.
- **After yield:** store current `raw.Fields` as the new snapshot.
- **On Delete:** remove the key from `_snapshots`.
- **CDC adapters** (e.g. Postgres WAL): `raw.PreviousFields.Count > 0` → snapshot bypassed.

> **Known issue (W-6):** `_snapshots` has no eviction limit. For high-volume or append-only
> sources this can grow without bound. Add an LRU cap if memory becomes a concern.

---

## 11. PostgresSink

Every published `DataChangeEvent` is inserted into `CDCDB.data_changes` via Dapper.

### Table DDL
```sql
CREATE TABLE data_changes (
    event_id          UUID         NOT NULL,
    detected_at       TIMESTAMPTZ  NOT NULL,
    source_timestamp  TIMESTAMPTZ,
    source_type       VARCHAR(100) NOT NULL,
    connector_id      VARCHAR(255) NOT NULL,
    entity_path       VARCHAR(500) NOT NULL,
    change_type       SMALLINT     NOT NULL,
    primary_key       JSONB        NOT NULL,
    payload           JSONB        NOT NULL,
    previous_payload  JSONB,
    metadata          JSONB        NOT NULL DEFAULT '{}',
    sequence_number   BIGINT       NOT NULL,
    schema_version    VARCHAR(20)  NOT NULL DEFAULT '1.0',
    CONSTRAINT pk_data_changes PRIMARY KEY (event_id)
);

CREATE INDEX idx_data_changes_detected_at   ON data_changes (detected_at DESC);
CREATE INDEX idx_data_changes_connector_id  ON data_changes (connector_id);
CREATE INDEX idx_data_changes_source_type   ON data_changes (source_type);
CREATE INDEX idx_data_changes_change_type   ON data_changes (change_type);
CREATE INDEX idx_data_changes_entity_path   ON data_changes (entity_path);
CREATE INDEX idx_data_changes_primary_key   ON data_changes USING GIN (primary_key);
CREATE INDEX idx_data_changes_payload       ON data_changes USING GIN (payload);
CREATE INDEX idx_data_changes_prev_payload  ON data_changes USING GIN (previous_payload)
    WHERE previous_payload IS NOT NULL;
```

### Useful queries
```sql
-- All changes for a specific primary key
SELECT * FROM data_changes
WHERE primary_key @> '{"assetId": "ASSET-011"}'::jsonb
ORDER BY detected_at DESC;

-- Add GIN index on primary_key for above query performance
CREATE INDEX idx_data_changes_primary_key ON data_changes USING GIN (primary_key);
```

### Mapping: `DataChangeEvent` → row
| Event property | Column | Transform |
|---|---|---|
| `EventId` (string) | `event_id UUID` | `Guid.TryParse` → fallback `Guid.NewGuid()` |
| `DetectedAt` | `detected_at` | direct |
| `SourceTimestamp` | `source_timestamp` | direct, nullable |
| `SourceType` | `source_type` | direct |
| `ConnectorId` | `connector_id` | direct |
| `EntityPath` | `entity_path` | direct |
| `ChangeType` (enum) | `change_type SMALLINT` | `(short)` cast |
| `PrimaryKey` (dict) | `primary_key JSONB` | `JsonSerializer.Serialize` + `::jsonb` cast |
| `Payload` (dict) | `payload JSONB` | `JsonSerializer.Serialize` + `::jsonb` cast |
| `PreviousPayload` (dict?) | `previous_payload JSONB` | `JsonSerializer.Serialize` or `null` |
| `Metadata` (dict) | `metadata JSONB` | `JsonSerializer.Serialize` + `::jsonb` cast |
| `SequenceNumber` | `sequence_number` | direct |
| `SchemaVersion` | `schema_version` | direct |

INSERT uses `ON CONFLICT (event_id) DO NOTHING` — idempotent on retry.
CancellationToken passed via Dapper `CommandDefinition`.

---

## 12. DI registration

### `AddGenericConnector()` (GenericConnectorExtensions.cs)
```csharp
services.AddOptions<GenericConnectorOptions>();          // bind manually if needed
services.AddSingleton<DescriptorValidator>();
services.AddSingleton<IDescriptorLoader, DescriptorLoader>();
services.AddSingleton<DescriptorStore>();
services.AddSingleton<FieldMapper>();
services.AddSingleton<AdapterRegistry>();               // ILogger<AdapterRegistry> injected automatically

// Protocol adapters (one singleton per sourceType)
services.AddSingleton<IProtocolAdapter, PostgresAdapter>();
services.AddSingleton<IProtocolAdapter, SqlServerAdapter>();
services.AddSingleton<IProtocolAdapter, Neo4jAdapter>();
services.AddSingleton<IProtocolAdapter, DatabricksAdapter>();
services.AddSingleton<IProtocolAdapter, MongoDbAdapter>();
services.AddSingleton<IProtocolAdapter>(sp =>
    new HttpRestAdapter(sp.GetRequiredService<ILoggerFactory>(), "sharepoint"));
services.AddSingleton<IProtocolAdapter>(sp =>
    new HttpRestAdapter(sp.GetRequiredService<ILoggerFactory>(), "sap"));
services.AddSingleton<IProtocolAdapter>(sp =>
    new HttpRestAdapter(sp.GetRequiredService<ILoggerFactory>(), "seeq"));
services.AddSingleton<IProtocolAdapter>(sp =>
    new HttpRestAdapter(sp.GetRequiredService<ILoggerFactory>(), "avevapi"));

services.AddHostedService<DescriptorBootstrapService>();
services.AddSingleton<IConnectorFactory, MultiSourceGenericFactory>();
```

### `AddUniversalConnector()` (ServiceCollectionExtensions.cs — Host project)
```csharp
services.Configure<NatsOptions>(configuration.GetSection("Nats"));
services.AddSingleton<INatsPublisher, NatsPublisher>();

services.Configure<PostgresSinkOptions>(configuration.GetSection("PostgresSink"));
services.AddSingleton<IDataSink, PostgresDataSink>();

services.AddGenericConnector();

services.AddSingleton<IConnectorRegistry, ConnectorRegistry>();
services.AddHostedService<ConnectorPipelineService>();
```

---

## 13. Docker Compose

### `docker-compose.nats.yml` — development infra
| Service | Image | Ports | Purpose |
|---|---|---|---|
| `nats` | `nats:alpine` | 4222, 8222 | NATS broker with JetStream (`-js`) and monitoring (`-m 8222`) |
| `nui` | `ghcr.io/nats-nui/nui:latest` | 31311 | NATS management UI → http://localhost:31311 |
| `nats-box` | `natsio/nats-box` | — | kept alive for `docker exec` debugging |
| `mongo` | `mongo:7` | 27017 | MongoDB (standalone, no replica set) |
| `mongo-express` | `mongo-express` | 8081 | MongoDB UI → http://localhost:8081 |
| `neo4j` | `neo4j:5` | 7474, 7687 | Neo4j (auth: `neo4j/YourStrongPassword123!`) |

> **Known issue (I-9):** MongoDB is started without `--replSet rs0`. Change Streams CDC
> will fail. Enable polling mode (`mode: polling`) in the MongoDB descriptor, or add a
> replica set init service if CDC is required.

### `docker-compose.yml` — PostgreSQL + SQL Server
> **Known issue (I-5):** The Postgres service creates user `connector` but the descriptor
> `postgres-orders.yaml` authenticates as `postgres`. Either change the compose service to
> `POSTGRES_USER: postgres` or update the descriptor.
>
> **Known issue (I-7):** `universal-connector` service declares `depends_on: sqlserver` but
> no `sqlserver` service is defined. Remove the dependency or add a SQL Server service.

---

## 14. Validation rules (DescriptorValidator)

| Rule | Level |
|---|---|
| `connectorId` empty | Error |
| `sourceType` not in known set | Error |
| `changeDetection.mode` invalid for `sourceType` | Error |
| Required connection fields missing (see below) | Error |
| `watch.entities` empty AND `autoDiscover: false` | Warning |
| `fieldMapping` rule with empty `source` | Error |
| `fieldMapping` rule with `exclude: true` AND `isKey: true` | Error |
| CDC mode for postgres without `replicationSlot` | Warning (defaults to `uc_slot`) |
| `resilience.retryDelaySeconds < 1` | Warning (uses 1s minimum) |

**Required connection fields per sourceType:**

| sourceType | Required |
|---|---|
| `postgres` | `host` + `database` + `username` OR `connectionString` |
| `sqlserver` | `host` + `database` OR `connectionString` |
| `neo4j` | `uri` + `username` |
| `databricks` | `host` + `httpPath` + `apiToken` |
| `mongodb` | `uri` + `database` |
| `seeq` | `baseUrl` + `username` |
| `avevapi` | `baseUrl` + `piServerName` |
| `sharepoint` | `tenantId` + `clientId` + `clientSecret` + `baseUrl` |
| `sap` | `baseUrl` + `username` |

**Supported modes per sourceType:**

| sourceType | Valid modes |
|---|---|
| `postgres` | `cdc`, `polling` |
| `sqlserver` | `cdc`, `polling` |
| `neo4j` | `polling` |
| `databricks` | `cdc`, `polling` |
| `mongodb` | `cdc`, `polling` |
| `sharepoint` | `delta` |
| `sap` | `delta` |
| `seeq` | `polling` |
| `avevapi` | `polling` |

---

## 15. Adding a new source type

1. Create `{Name}Adapter.cs` in `UniversalConnector.Generic/Adapters/` implementing
   `IProtocolAdapter` with the appropriate `SourceType` string.
2. Register in `GenericConnectorExtensions.cs`:
   `services.AddSingleton<IProtocolAdapter, {Name}Adapter>();`
3. Add to `DescriptorValidator`: `KnownSourceTypes`, `SupportedModes[sourceType]`,
   and connection validation case.
4. Create a descriptor YAML in `connectors/` using `sourceType: {name}`.
5. No other changes required.

---

## 16. Known issues (from audit — not yet fixed)

Issues marked Critical were fixed. Remaining:

### Warnings (should fix before production)

| ID | File | Issue |
|---|---|---|
| W-2 | `NatsPublisher.cs:41` | Publishing `string` via NATS.Net 2.x may double-serialize JSON. Use `UTF8.GetBytes(json)` instead. |
| W-3 | `DescriptorValidator.cs` | SAP/SharePoint only allow `"delta"` mode but `HttpRestAdapter` ignores `mode` entirely. |
| W-4 | `PostgresAdapter.cs:175` | `SourceTimestamp` set to stale `maxWatermark` instead of the current row's value (same in SqlServerAdapter, MongoDbAdapter). |
| W-5 | `SqlServerAdapter.cs:67` | Empty `JOIN ON` clause when entity has no `primaryKey` → SQL syntax error at runtime. Add validator check. |
| W-6 | `GenericConnector.cs:19` | `_snapshots` dict grows without bound. Add LRU eviction cap for production. |
| W-7 | `BaseProtocolAdapter.cs:34` | `DisposeAsync` does not call `CloseCoreAsync` — connections leak if disposed without `CloseAsync`. |
| W-9 | `ConnectorPipelineService.cs:57` | NATS publish failure also skips sink write. Both should be independent try/catch blocks. |
| W-10 | `postgres-orders.yaml` | Plaintext `password: "12345"` in source control. Use `${POSTGRES_PASSWORD}`. |
| W-11 | `GenericConnectorOptions.cs` | Default `DescriptorDirectory` is hardcoded Windows path. Change to relative `"connectors"`. |
| W-12 | `HttpRestAdapter.cs:33` | First `HttpClient` is leaked when SSL bypass is applied. |
| W-14 | `SqlServerAdapter.cs:60` | `StartingVersion` descriptor field silently ignored. |
| W-15 | `DescriptorValidator.cs` | No validation that SQL Server CDC entities define at least one PK column. |
| W-16 | `neo4j-graph.yaml:24` | Relationship `primaryKey: [id]` not in `IRelationship.Properties` — all share one snapshot slot. |

### Info (low priority)

| ID | Issue |
|---|---|
| I-1 | `UniversalConnector.Connectors` project is entirely dead code — never registered in DI. |
| I-4 | `SemaphoreSlim` race on dispose in `NatsPublisher`. |
| I-5 | Postgres compose user `connector` vs descriptor user `postgres` mismatch. |
| I-7 | `docker-compose.yml` `depends_on: sqlserver` references an undefined service. |
| I-9 | MongoDB no `--replSet` → CDC mode fails. |
| I-10 | `GenericConnectorOptions` not bound to config from `AddUniversalConnector`. |
| I-14 | NATS subject not sanitized — dots or `>` in `connectorId` produce invalid subjects. |

---

## 17. Planned / not yet implemented

- **Debezium integration** — `UniversalConnector.Debezium` project with `DebeziumAdapter`
  that subscribes to Debezium Server NATS JetStream subjects and maps the `before`/`after`
  envelope to `RawChangeRecord`. See the Debezium plan in conversation history.
- **GIN index on `primary_key`** — DDL not yet applied to the running database:
  `CREATE INDEX idx_data_changes_primary_key ON data_changes USING GIN (primary_key);`
- **Secrets management** — all passwords currently plaintext. Replace with env-var
  interpolation (`${VAR}`) and inject via Docker env / Kubernetes secrets.

---

## 18. Testing checklist

- [ ] `DescriptorLoader.LoadFromString` correctly propagates `filePath` to failure results
- [ ] `DescriptorLoader` correctly interpolates `${ENV_VAR}` tokens; fails with filename in error when var missing
- [ ] `DescriptorLoader` returns `Fail` (with filename) for malformed YAML
- [ ] `DescriptorValidator` rejects incompatible mode/sourceType combinations
- [ ] `FieldMapper` correctly excludes, renames, casts, promotes, and injects static fields
- [ ] `PostgresAdapter` validates `wal_level = logical` before starting CDC
- [ ] `PostgresAdapter` creates replication slot/publication if not exists
- [ ] `SqlServerAdapter` enables Change Tracking on first connect when `autoEnableChangeTracking: true`
- [ ] `GenericConnector` snapshot cache: second poll of same row has non-null `PreviousPayload`
- [ ] `GenericConnector` snapshot cache: CDC adapter records bypass snapshot (use adapter's PreviousFields)
- [ ] `GenericConnector` snapshot cache: Delete removes key; subsequent Insert starts fresh
- [ ] `PostgresDataSink` INSERT is idempotent (duplicate `event_id` → no error)
- [ ] `PostgresDataSink` sink failure does not throw to caller
- [ ] `PostgresDataSink` respects CancellationToken via `CommandDefinition`
- [ ] `AdapterRegistry` logs warning and skips (does not throw) on duplicate `SourceType`
- [ ] `ConnectorPipelineService` calls `DisconnectAsync(CancellationToken.None)` on shutdown
- [ ] NATS subject format: `universal-connector.{sourceType}.{connectorId}.{changeType}`
- [ ] Events from all active source types produce valid `DataChangeEvent` JSON
- [ ] `DescriptorBootstrapService` populates `DescriptorStore` before `ConnectorPipelineService` starts
