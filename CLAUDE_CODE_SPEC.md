# Universal Connector — Generic Engine: Claude Code Build Spec

## Purpose of this document

This is an implementation spec for Claude Code. Hand it to `claude` in your project root and it will implement the full descriptor-driven generic connector engine from scratch, or extend the existing hardcoded connector solution in `src/`.

---

## 1. What you are building

A **.NET 9 Worker Service** that connects to any of the following data sources purely from configuration files (YAML/JSON descriptors), detects data changes, and publishes a canonical `DataChangeEvent` JSON message to Apache Kafka — with **zero code changes** when adding new source instances.

Supported source types: `postgres`, `sqlserver`, `neo4j`, `databricks`, `seeq`, `avevapi`, `sharepoint`, `sap`.

---

## 2. Solution structure

```
UniversalConnector/
├── src/
│   ├── UniversalConnector.Core/           # Shared contracts, models, descriptors
│   │   ├── Abstractions/
│   │   │   ├── IDataSourceConnector.cs    # connector lifecycle interface
│   │   │   ├── IKafkaPublisher.cs         # Kafka publishing interface
│   │   │   ├── IConnectorRegistry.cs      # registry + factory interfaces
│   │   │   ├── IProtocolAdapter.cs        # thin transport adapter interface
│   │   │   └── BaseConnector.cs           # retry loop, health, sequence
│   │   ├── Descriptors/
│   │   │   ├── ConnectorDescriptor.cs     # full descriptor model (all sources)
│   │   │   └── IDescriptorLoader.cs       # loader interface + result types
│   │   ├── Models/
│   │   │   ├── DataChangeEvent.cs         # canonical output event
│   │   │   └── ConnectorHealthReport.cs
│   │   └── Configuration/
│   │       └── ConnectorOptions.cs        # base options for hardcoded path
│   │
│   ├── UniversalConnector.Kafka/          # Confluent.Kafka publisher
│   │   └── KafkaPublisher.cs
│   │
│   ├── UniversalConnector.Connectors/     # Hardcoded connector path (backward compat)
│   │   ├── Relational/  PostgresConnector.cs, SqlServerConnector.cs
│   │   ├── Graph/       Neo4jConnector.cs
│   │   ├── Analytics/   DatabricksConnector.cs
│   │   ├── Historian/   SeeqConnector.cs
│   │   ├── Industrial/  AvevapiConnector.cs
│   │   ├── Collaboration/ SharePointConnector.cs
│   │   └── ERP/         SapConnector.cs
│   │
│   ├── UniversalConnector.Generic/        # ← NEW: descriptor-driven engine
│   │   ├── Adapters/
│   │   │   ├── BaseProtocolAdapter.cs     # abstract base with open/close guard
│   │   │   ├── PostgresAdapter.cs         # WAL CDC + watermark polling
│   │   │   ├── SqlServerAdapter.cs        # Change Tracking
│   │   │   ├── Neo4jAdapter.cs            # Bolt watermark polling
│   │   │   ├── DatabricksAdapter.cs       # Delta CDF + ODBC polling
│   │   │   └── HttpRestAdapter.cs         # SharePoint Graph / SAP OData / Seeq / PI
│   │   ├── Engine/
│   │   │   ├── GenericConnector.cs        # BaseConnector impl, delegates to adapter
│   │   │   ├── AdapterRegistry.cs         # sourceType → IProtocolAdapter lookup
│   │   │   ├── GenericConnectorFactory.cs # IConnectorFactory for descriptor path
│   │   │   ├── DescriptorStore.cs         # in-memory cache of loaded descriptors
│   │   │   ├── DescriptorLoader.cs        # file/string loader with env-var interpolation
│   │   │   ├── DescriptorValidator.cs     # cross-cutting validation rules
│   │   │   └── DescriptorBootstrapService.cs  # IHostedService: loads files at startup
│   │   ├── Mapping/
│   │   │   └── FieldMapper.cs             # applies fieldMapping rules from descriptor
│   │   ├── Configuration/
│   │   │   └── GenericConnectorOptions.cs # DescriptorDirectory, FailOnDescriptorError
│   │   └── Extensions/
│   │       └── GenericConnectorExtensions.cs  # AddGenericConnector() DI method
│   │
│   └── UniversalConnector.Host/           # Executable Worker host
│       ├── ConnectorRegistry.cs
│       ├── ConnectorPipelineService.cs
│       ├── Program.cs
│       ├── appsettings.json
│       └── Extensions/
│           └── ServiceCollectionExtensions.cs
│
├── connectors/                            # ← Descriptor YAML files live here
│   ├── postgres-orders.yaml
│   ├── sqlserver-crm.yaml
│   ├── neo4j-graph.yaml
│   ├── databricks-lakehouse.yaml
│   ├── sharepoint-docs.yaml
│   ├── sap-s4hana.yaml
│   ├── seeq-plant.yaml
│   └── avevapi-historian.yaml
│
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## 3. Descriptor YAML schema (complete)

Every descriptor file must deserialize into `ConnectorDescriptor`. The full schema:

```yaml
# ── Identity ───────────────────────────────────────────────────────────
connectorId: string          # required, unique (e.g. "pg-orders")
sourceType: string           # required: postgres|sqlserver|neo4j|databricks|seeq|avevapi|sharepoint|sap
description: string          # optional, human-readable
enabled: bool                # default: true

# ── Connection ──────────────────────────────────────────────────────────
connection:
  connectionString: string   # ADO.NET/JDBC full string (overrides individual fields)
  host: string
  port: int
  database: string
  username: string
  password: string           # supports "${ENV_VAR}" interpolation
  baseUrl: string            # HTTP-based sources
  apiToken: string           # Databricks PAT, Seeq token
  tenantId: string           # AAD / SharePoint
  clientId: string
  clientSecret: string
  httpPath: string           # Databricks SQL warehouse path
  uri: string                # Neo4j bolt URI
  verifySsl: bool            # default: true
  sslCertPath: string
  sapClient: string          # SAP client number
  piServerName: string       # AVEVA PI server name

# ── Change detection ────────────────────────────────────────────────────
changeDetection:
  mode: cdc|polling|delta|streaming   # required

  # Shared
  pollIntervalSeconds: int            # default: 30
  watermarkColumn: string             # default: "updated_at"
  lookbackDuration: string            # ISO 8601 duration, default: "PT1H"

  # CDC (Postgres)
  replicationSlot: string             # default: "uc_slot"
  publication: string                 # default: "uc_pub"

  # Delta feed (Databricks)
  startingVersion: long               # -1 = latest

  # SQL Server
  autoEnableChangeTracking: bool      # default: true

# ── Watch ───────────────────────────────────────────────────────────────
watch:
  autoDiscover: bool                  # discover all entities (adapter-specific)
  entities:
    - name: string                    # table / label / tag / list / entity set path
      primaryKey: [string]            # column/property names forming the PK
      filter: string                  # optional WHERE / $filter / Cypher WHERE clause
      changeDetectionOverride: string # per-entity mode override

# ── Field mapping ────────────────────────────────────────────────────────
fieldMapping:
  - source: string           # required: source field name
    target: string           # optional: output field name (default: source)
    type: string             # optional: string|int|long|double|bool|timestamp|date
    exclude: bool            # drop this field from output
    isKey: bool              # promote to primaryKey instead of payload
    staticValue: any         # inject a constant value

# ── Kafka overrides ──────────────────────────────────────────────────────
kafka:
  topicOverride: string      # null = use global TopicStrategy
  serializationFormat: json|avro|protobuf   # default: json
  additionalHeaders:
    key: value               # injected into every Kafka message header

# ── Resilience ───────────────────────────────────────────────────────────
resilience:
  maxConsecutiveFailures: int    # default: 5
  retryDelaySeconds: int         # default: 10
  backoffMultiplier: double      # default: 1.5
  maxRetryDelaySeconds: int      # default: 120
```

**Environment variable interpolation**: any `${VAR_NAME}` in any string value is replaced by the corresponding environment variable at load time. If the variable is not set, the loader throws and the descriptor fails to load.

---

## 4. Canonical output event (unchanged from hardcoded path)

```jsonc
{
  "eventId": "uuid-v4",
  "detectedAt": "2025-05-01T12:00:00Z",
  "sourceTimestamp": "2025-05-01T11:59:59Z",  // from source, if available
  "sourceType": "postgres",
  "connectorId": "pg-orders",
  "entityPath": "public.orders",
  "changeType": "Update",      // Insert|Update|Delete|Snapshot|SchemaChange|Heartbeat
  "primaryKey": { "id": 42 },
  "payload": { "status": "shipped", "occurredAt": "2025-05-01T11:59:59Z" },
  "previousPayload": null,     // populated for Postgres WAL full-update when REPLICA IDENTITY FULL
  "metadata": { "domain": "order-management" },
  "sequenceNumber": 1042,
  "schemaVersion": "1.0"
}
```

---

## 5. Key interfaces to implement

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

public sealed class RawChangeRecord
{
    public required string EntityPath { get; init; }
    public required ChangeType ChangeType { get; init; }
    public DateTimeOffset? SourceTimestamp { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, object?> PreviousFields { get; init; } = new Dictionary<string, object?>();
    public IReadOnlyDictionary<string, string> AdapterMetadata { get; init; } = new Dictionary<string, string>();
}
```

### `IDescriptorLoader`

```csharp
public interface IDescriptorLoader
{
    Task<IReadOnlyList<DescriptorLoadResult>> LoadFromDirectoryAsync(string directoryPath, CancellationToken ct);
    Task<DescriptorLoadResult> LoadFromFileAsync(string filePath, CancellationToken ct);
    DescriptorLoadResult LoadFromString(string content, string format = "yaml");
    DescriptorValidationResult Validate(ConnectorDescriptor descriptor);
}
```

### `GenericConnector` (extends `BaseConnector`)

```csharp
// Constructor receives descriptor + resolved adapter + FieldMapper
// ConnectCoreAsync  → adapter.OpenAsync(descriptor, ct)
// DisconnectCoreAsync → adapter.CloseAsync(ct)
// PollOrStreamAsync → adapter.StreamRawChangesAsync(descriptor, ct)
//                     then for each RawChangeRecord:
//                       apply FieldMapper
//                       merge adapter metadata + kafka.additionalHeaders
//                       call BuildEvent(...)
//                       yield the DataChangeEvent
```

---

## 6. Adapter implementation notes

### PostgresAdapter
- `mode: cdc` → Npgsql logical replication (pgoutput). Ensure slot + publication exist before starting.
- `mode: polling` → `SELECT * FROM {table} WHERE {watermarkColumn} > @since ORDER BY {watermarkColumn}`, re-run every `pollIntervalSeconds`.
- `watch.autoDiscover: true` → query `pg_tables` to get all user tables.
- WAL messages: `InsertMessage`, `UpdateMessage`, `FullUpdateMessage`, `DeleteMessage`, `FullDeleteMessage`, `KeyDeleteMessage`. Map each to `ChangeType`.

### SqlServerAdapter
- `mode: cdc` → `CHANGETABLE(CHANGES ..., @from)` poll. Auto-enable CT on database + tables if `autoEnableChangeTracking: true`.
- Track `_versions[table]` = last processed `SYS_CHANGE_VERSION`.
- `mode: polling` → watermark column poll, same pattern as Postgres.

### Neo4jAdapter
- `mode: polling` only. Query nodes by label and relationships by type using `n.{watermarkColumn} > $since`.
- Entity names prefixed `REL:` indicate relationship types (e.g. `REL:WORKS_AT`).
- `watch.autoDiscover: true` → `CALL db.labels()`.

### DatabricksAdapter
- `mode: cdc` → `table_changes('{table}', {version})`. Skip `update_preimage` rows. Track `_versions[table]`.
- `mode: polling` → filter on `_updated_at` column (or `watermarkColumn`).
- Connect via Simba Spark ODBC driver string built from `connection.host`, `connection.httpPath`, `connection.apiToken`.

### HttpRestAdapter (covers sharepoint, sap, seeq, avevapi)
- Register one instance per source type with a distinct `SourceType` property.
- **SharePoint** (`mode: delta`): Microsoft Graph `/sites/{siteId}/lists/{listName}/items/delta`. Cache `@odata.deltaLink` per list in `_deltaLinks[listName]`. Auth via AAD client-credentials.
- **SAP** (`mode: delta`): OData V4 `{servicePath}/{entitySet}?$trackChanges`. Cache `@odata.deltaLink`. Auth via Basic (`username:password`). Add `sap-client` header from `connection.sapClient`.
- **Seeq** (`mode: polling`): `GET /api/v1/signals/{signalId}/samples?start=...&end=...`. Auth: POST `/api/v1/auth/login` → `token` → header `sq-auth`. Watermark per signal.
- **AVEVA PI** (`mode: polling`): `GET /piwebapi/streams/{webId}/recorded?startTime=...`. Auth via Basic or NTLM. Watermark per tag WebID.

### FieldMapper
Apply `fieldMapping` rules from descriptor in this order:
1. Exclude → skip field
2. Rename (target ≠ null) → use target name
3. Cast (type ≠ null) → convert value
4. IsKey → move to primaryKey dict
5. Static → inject constant value not from source
6. Pass-through: fields matching `entity.primaryKey` → primaryKey; rest → payload

---

## 7. Validation rules (DescriptorValidator)

| Rule | Error | Level |
|---|---|---|
| `connectorId` empty | "connectorId is required" | Error |
| `sourceType` not in known set | "sourceType '{x}' not recognised" | Error |
| `changeDetection.mode` invalid for sourceType | "mode '{x}' not supported for '{y}'" | Error |
| Required connection fields missing (see table below) | "connection.{field} required" | Error |
| `watch.entities` empty AND `autoDiscover: false` | "no entities will be captured" | Warning |
| `fieldMapping` rule with empty `source` | "source field name empty" | Error |
| `fieldMapping` rule with `exclude: true` AND `isKey: true` | "mutually exclusive" | Error |
| CDC mode for postgres without `replicationSlot` | "will default to 'uc_slot'" | Warning |
| `resilience.retryDelaySeconds < 1` | "using 1s minimum" | Warning |

**Required connection fields per sourceType:**

| sourceType | Required fields |
|---|---|
| postgres | host + database + username (OR connectionString) |
| sqlserver | host + database (OR connectionString) |
| neo4j | uri + username |
| databricks | host + httpPath + apiToken |
| seeq | baseUrl + username |
| avevapi | baseUrl + piServerName |
| sharepoint | tenantId + clientId + clientSecret + baseUrl |
| sap | baseUrl + username |

---

## 8. DI registration (`AddGenericConnector`)

```csharp
services.Configure<GenericConnectorOptions>(config.GetSection("GenericConnector"));
services.AddSingleton<DescriptorValidator>();
services.AddSingleton<IDescriptorLoader, DescriptorLoader>();
services.AddSingleton<DescriptorStore>();
services.AddSingleton<FieldMapper>();
services.AddSingleton<AdapterRegistry>();

// One IProtocolAdapter per transport
services.AddSingleton<IProtocolAdapter, PostgresAdapter>();
services.AddSingleton<IProtocolAdapter, SqlServerAdapter>();
services.AddSingleton<IProtocolAdapter, Neo4jAdapter>();
services.AddSingleton<IProtocolAdapter, DatabricksAdapter>();
// HTTP adapter registered 4× with different SourceType names
services.AddSingleton<IProtocolAdapter>(sp => new NamedHttpAdapter(lf, "sharepoint"));
services.AddSingleton<IProtocolAdapter>(sp => new NamedHttpAdapter(lf, "sap"));
services.AddSingleton<IProtocolAdapter>(sp => new NamedHttpAdapter(lf, "seeq"));
services.AddSingleton<IProtocolAdapter>(sp => new NamedHttpAdapter(lf, "avevapi"));

// Bootstrap (runs before ConnectorPipelineService)
services.AddHostedService<DescriptorBootstrapService>();

// Bridge: descriptor store → IConnectorFactory (sourceType = "*")
services.AddSingleton<IConnectorFactory, MultiSourceGenericFactory>();
```

`ConnectorRegistry.Resolve` must be updated to fall back to the `*` factory when no exact-match factory exists.

---

## 9. Startup sequence

```
1. DescriptorBootstrapService.StartAsync()
   → DescriptorLoader.LoadFromDirectoryAsync("connectors/")
     → For each *.yaml / *.yml / *.json:
         interpolate env vars
         deserialize to ConnectorDescriptor
         validate
         register in DescriptorStore
   → log: N loaded, M failed

2. ConnectorPipelineService.StartAsync()
   → reads Pipeline.Connectors from config (may be empty for pure-descriptor mode)
   → reads DescriptorStore.GetAll() for descriptor-driven connectors
   → merges both lists
   → for each enabled connector: resolves via ConnectorRegistry, connects, streams

3. Per connector loop (in parallel Task per connector)
   → IDataSourceConnector.ConnectAsync()
   → await foreach DataChangeEvent in StreamChangesAsync()
   → IKafkaPublisher.PublishAsync(event)
```

---

## 10. appsettings.json additions

```jsonc
{
  "GenericConnector": {
    // Directory scanned at startup for *.yaml / *.yml / *.json descriptor files
    "DescriptorDirectory": "connectors",

    // true = refuse to start if any descriptor fails; false = log and continue
    "FailOnDescriptorError": false
  }
}
```

---

## 11. NuGet packages required

| Package | Version | Purpose |
|---|---|---|
| `YamlDotNet` | 16.x | YAML descriptor parsing |
| `Confluent.Kafka` | 2.6.x | Kafka producer |
| `Npgsql` | 9.0.x | Postgres + logical replication |
| `Microsoft.Data.SqlClient` | 5.2.x | SQL Server |
| `Neo4j.Driver` | 5.26.x | Neo4j Bolt |
| `System.Data.Odbc` | 9.0.x | Databricks Simba ODBC |
| `Microsoft.Extensions.Hosting` | 9.0.x | Worker host |

SharePoint, SAP, Seeq, and AVEVA PI use built-in `HttpClient` — no additional packages needed.

---

## 12. Docker / deployment

The `connectors/` directory must be available at the working directory of the running process. In Docker:
```dockerfile
COPY connectors/ /app/connectors/
```
Or mount as a volume:
```yaml
volumes:
  - ./connectors:/app/connectors:ro
```

Secrets (passwords, tokens) must never appear in descriptor files. Always use `${ENV_VAR}` references and inject via Docker environment variables, Kubernetes secrets, or Azure Key Vault references.

---

## 13. Extending with a new source type

To add a new source (e.g. `mongodb`):

1. Create `MongoDbAdapter.cs` implementing `IProtocolAdapter` with `SourceType = "mongodb"`.
2. Register: `services.AddSingleton<IProtocolAdapter, MongoDbAdapter>();`
3. Add validation rules to `DescriptorValidator` for the new sourceType.
4. Create a descriptor YAML file:
   ```yaml
   connectorId: mongo-main
   sourceType: mongodb
   connection:
     host: "${MONGO_HOST}"
     ...
   ```
5. No other code changes required.

---

## 14. Testing checklist

- [ ] `DescriptorLoader` correctly interpolates `${ENV_VAR}` tokens
- [ ] `DescriptorLoader` returns `Fail` for malformed YAML
- [ ] `DescriptorValidator` rejects incompatible mode/sourceType combinations
- [ ] `DescriptorValidator` produces warnings (not errors) for optional missing fields
- [ ] `FieldMapper` correctly excludes, renames, casts, and promotes fields
- [ ] `PostgresAdapter` creates replication slot/publication if not exists
- [ ] `SqlServerAdapter` enables Change Tracking on first connect when `autoEnableChangeTracking: true`
- [ ] `HttpRestAdapter` stores and reuses delta tokens across poll cycles
- [ ] `GenericConnector.StreamChangesAsync` retries with exponential back-off on failure
- [ ] Events from all source types produce valid `DataChangeEvent` JSON
- [ ] `DescriptorBootstrapService` populates `DescriptorStore` before `ConnectorPipelineService` starts
