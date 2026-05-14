# UniversalConnector — Project Specification

> **Purpose of this document**
> Complete specification of the UniversalConnector project as of May 2026.
> Sufficient to recreate the project from scratch: architecture, data flow, all classes,
> configuration, wire format, connector descriptors, infrastructure, and test coverage.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Solution Structure](#2-solution-structure)
3. [Architecture & Data Flow](#3-architecture--data-flow)
4. [Core Abstractions & Models](#4-core-abstractions--models)
5. [Configuration](#5-configuration)
6. [Connector Descriptors](#6-connector-descriptors)
7. [Infrastructure Services](#7-infrastructure-services)
8. [Generic Connector Engine](#8-generic-connector-engine)
9. [Protocol Adapters](#9-protocol-adapters)
10. [Host Application](#10-host-application)
11. [Wire Format — Protobuf Envelope](#11-wire-format--protobuf-envelope)
12. [NATS Subject Schema](#12-nats-subject-schema)
13. [Connector YAML Files](#13-connector-yaml-files)
14. [Resilience & Circuit Breaker](#14-resilience--circuit-breaker)
15. [Lifecycle Management](#15-lifecycle-management)
16. [Health & Heartbeats](#16-health--heartbeats)
17. [Ontology Cache (Fuseki)](#17-ontology-cache-fuseki)
18. [Checkpoints](#18-checkpoints)
19. [Test Coverage](#19-test-coverage)
20. [Infrastructure — Docker & NATS](#20-infrastructure--docker--nats)
21. [Python Consumer (NATS_Consumer)](#21-python-consumer-nats_consumer)
22. [Key Design Decisions](#22-key-design-decisions)

---

## 1. Overview

**UniversalConnector** is a .NET 10 Worker Service that detects data changes across
heterogeneous sources (relational databases, graph databases, SaaS APIs, time-series
systems) and publishes them as structured events to a NATS messaging broker.

The connector is **single-purpose**: detect changes, enrich them, publish them.
Downstream processing (persistence, transformation, integration) is the responsibility
of NATS consumers, not the connector itself.

### Core technologies

| Technology | Version | Role |
|------------|---------|------|
| .NET | 10.0 | Runtime |
| NATS.Net | 2.7.3 | Messaging broker client |
| Google.Protobuf | 3.27.1 | Wire serialization |
| Npgsql | 10.0.2 | PostgreSQL driver + logical replication |
| Microsoft.Data.SqlClient | 7.0.1 | SQL Server Change Tracking |
| Neo4j.Driver | 6.0.0 | Neo4j Bolt protocol |
| MongoDB.Driver | 3.x | MongoDB change streams |
| YamlDotNet | 17.1.0 | Descriptor file parsing |
| xUnit | 2.9.3 | Testing |
| FluentAssertions | 6.x | Test assertions |
| NSubstitute | 5.x | Mocking |

---

## 2. Solution Structure

```
UniversalConnector/
├── src/
│   ├── CommonModel.Runtime.Core/               # Abstractions, models, configuration, descriptors
│   ├── CommonModel.Runtime.Infrastructure/     # NATS integration, checkpoint store, health, ontology
│   ├── CommonModel.Runtime.Drivers.Generic/    # Protocol adapters, connector engine, field mapping
│   └── CommonModel.Runtime.Host/               # Worker service host, DI, pipeline orchestration
├── tests/
│   └── CommonModel.Runtime.Tests/              # xUnit tests (222 tests)
├── connectors/                                  # YAML connector descriptor files
├── docker/                                      # Docker init scripts (postgres, sqlserver, nats, mongo)
├── Dockerfile                                   # Multi-stage build
├── docker-compose.yml                           # Full dev stack
└── SPEC.md                                      # This file
```

### Project dependencies

```
CommonModel.Runtime.Host
    ├── CommonModel.Runtime.Infrastructure
    │       └── CommonModel.Runtime.Core
    └── CommonModel.Runtime.Drivers.Generic
            └── CommonModel.Runtime.Core
```

---

## 3. Architecture & Data Flow

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SOURCES                                                             │
  │  Postgres · SQL Server · Neo4j · MongoDB · Databricks · SAP · ...  │
  └──────────────────────────┬──────────────────────────────────────────┘
                             │  WAL / Change Tracking / Polling / API
                             ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  IProtocolAdapter (per source type)                                  │
  │  Emits: RawChangeRecord { EntityPath, ChangeType, Fields,           │
  │                           PreviousFields, SourceTimestamp }          │
  └──────────────────────────┬───────────────────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  GenericConnector (extends BaseConnector)                            │
  │  · FieldMapper.Apply() — renames, casts, excludes, resolves PK,     │
  │    applies ConceptMap, builds PreviousFields diff from snapshot cache│
  │  · Builds metadata { slot, publication, adapter version... }        │
  │  · Emits: RawChangeEvent (domain model)                              │
  └──────────────────────────┬───────────────────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  ConnectorPipelineService (orchestrator)                             │
  │  Routes event to IEventPipeline                                      │
  └──────────────────────────┬───────────────────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  DefaultEventPipeline                                                │
  │  1. NatsPublisher.PublishAsync()  → protobuf Envelope → NATS        │
  │  2. NatsCheckpointStore.SaveAsync()  → NATS KV bucket               │
  └──────────────────────────┬───────────────────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  NATS Broker (JetStream or Core)                                     │
  │  Stream: CDC   subjects: cdc.>                                       │
  │  DLQ:          subjects: cdc.dlq.>                                   │
  │  Health:       subjects: cdc.health.<driverId>                       │
  │  Lifecycle:    subjects: cdc.commands.* / cdc.lifecycle.*            │
  │  Checkpoints:  KV bucket: cm-checkpoints                             │
  └──────────────────────────────────────────────────────────────────────┘
```

---

## 4. Core Abstractions & Models

### 4.1 `RawChangeRecord`
Raw output from a protocol adapter. Not enriched; no field mapping applied.

```csharp
public sealed class RawChangeRecord
{
    public required string EntityPath { get; init; }
    public required ChangeType ChangeType { get; init; }
    public DateTimeOffset? SourceTimestamp { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; }
    public IReadOnlyDictionary<string, object?> PreviousFields { get; init; }
    public IReadOnlyDictionary<string, string> AdapterMetadata { get; init; }
}
```

### 4.2 `RawChangeEvent`
Enriched domain model produced by `GenericConnector`. Published to NATS.

```csharp
public sealed record RawChangeEvent
{
    public string EventId { get; init; }           // ULID
    public DateTimeOffset DetectedAt { get; init; }
    public DateTimeOffset? SourceTimestamp { get; init; }
    public required string SourceType { get; init; }
    public required string DriverId { get; init; }
    public string Context { get; init; }            // e.g. ctx:order-management
    public required string EntityPath { get; init; }
    public ChangeType ChangeType { get; init; }
    public IReadOnlyDictionary<string, object?> PrimaryKey { get; init; }
    public IReadOnlyDictionary<string, object?> Fields { get; init; }
    public IReadOnlyDictionary<string, object?>? PreviousFields { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; }
    public long SequenceNumber { get; init; }
    public string? SubjectHint { get; init; }       // pre-computed NATS subject
}
```

### 4.3 `ChangeType` enum
```
Insert | Update | Delete | Snapshot | SchemaChange | Heartbeat
```

### 4.4 `DriverState` enum
```
Disconnected | Connecting | Connected | Streaming | Reconnecting | Failed
```

### 4.5 `HealthStatus`
```csharp
public sealed class HealthStatus
{
    public required string DriverId { get; init; }
    public required string SourceType { get; init; }
    public DriverState State { get; init; }
    public DateTimeOffset LastChecked { get; init; }
    public DateTimeOffset? LastEventAt { get; init; }
    public long TotalEventsEmitted { get; init; }
    public int ConsecutiveFailures { get; init; }
    public string? LastError { get; init; }
}
```

### 4.6 `Checkpoint`
```csharp
public sealed record Checkpoint
{
    public required string DriverId { get; init; }
    public required string EntityPath { get; init; }
    public required string Position { get; init; }   // WAL LSN or watermark timestamp
    public DateTimeOffset UpdatedAt { get; init; }
}
```

### 4.7 `OntologyEntry`
```csharp
public sealed record OntologyEntry
{
    public required string Iri { get; init; }
    public string? Label { get; init; }
    public string? ParentIri { get; init; }
    public string? Type { get; init; }   // "class" | "property" | "individual"
}
```

### 4.8 `LifecycleCommand` / `LifecycleEvent`
```csharp
public sealed record LifecycleCommand
{
    public string CommandId { get; init; }           // ULID
    public required string DriverId { get; init; }
    public required string Action { get; init; }     // "start" | "stop" | "restart"
    public string? RequestedBy { get; init; }
    public DateTimeOffset IssuedAt { get; init; }
}

public sealed record LifecycleEvent
{
    public required string DriverId { get; init; }
    public required DriverState State { get; init; }
    public DriverState? PreviousState { get; init; }
    public string? TriggeringAction { get; init; }
    public string? CommandId { get; init; }
    public DateTimeOffset OccurredAt { get; init; }
}
```

### 4.9 Key interfaces

| Interface | Implementation | Purpose |
|-----------|---------------|---------|
| `IProtocolAdapter` | Per-source adapters | Streams raw changes from a source |
| `ISourceDriver` | `GenericConnector` | Enriched driver with retry/backoff |
| `IDriverFactory` | `MultiSourceGenericFactory` | Creates drivers by sourceType |
| `IConnectorRegistry` | `ConnectorRegistry` | Resolves/creates all enabled drivers |
| `IEventPipeline` | `DefaultEventPipeline` | Processes events (publish + checkpoint) |
| `INatsPublisher` | `NatsPublisher` | Serializes and publishes to NATS |
| `ICheckpointStore` | `NatsCheckpointStore` | Persists resume positions |
| `IOntologyCache` | `FusekiOntologyCache` | In-memory ontology lookup |
| `IDriverLifecycleController` | `ConnectorPipelineService` | Start/stop/restart drivers at runtime |

---

## 5. Configuration

### 5.1 `appsettings.json` (defaults)

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.Hosting.Lifetime": "Information",
      "CommonModel.Runtime": "Debug"
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
  "GenericConnector": {
    "DescriptorDirectory": "connectors",
    "FailOnDescriptorError": true
  },
  "OntologyCache": {
    "GraphIri": null,
    "RefreshSubject": "cdc.ontology.refresh",
    "LoadOnStartup": false
  },
  "Heartbeat": {
    "IntervalSeconds": 30,
    "SubjectPrefix": "cdc.health",
    "UseJetStream": false
  }
}
```

### 5.2 `NatsOptions`

| Property | Default | Description |
|----------|---------|-------------|
| `Servers` | `["nats://localhost:4222"]` | NATS server URLs |
| `SubjectPrefix` | `"cdc"` | Root subject prefix for all events |
| `UseJetStream` | `true` | Use JetStream persistence |
| `DlqSubjectPrefix` | `"cdc.dlq"` | Dead-letter queue subject root |
| `CheckpointBucket` | `"cm-checkpoints"` | NATS KV bucket for checkpoints |
| `CredsFile` | `null` | Path to NATS credentials file (set via env) |
| `StopOnCriticalFailure` | `false` | Halt the host if startup checks fail |

### 5.3 `HeartbeatOptions`

| Property | Default | Description |
|----------|---------|-------------|
| `IntervalSeconds` | `30` | Heartbeat publish interval |
| `SubjectPrefix` | `"cdc.health"` | Subject prefix for heartbeat messages |
| `UseJetStream` | `false` | Heartbeats use core NATS (fire-and-forget) |

### 5.4 `OntologyCacheOptions`

| Property | Default | Description |
|----------|---------|-------------|
| `EndpointUrl` | `null` | Fuseki SPARQL endpoint. Null = disabled |
| `GraphIri` | `null` | Optional named graph IRI to query |
| `RefreshSubject` | `"cdc.ontology.refresh"` | NATS subject to trigger hot reload |
| `LoadOnStartup` | `false` | Pre-load cache before first lookup |

### 5.5 Environment variable overrides

All configuration keys can be overridden via environment variables using the `__` separator:

```
Nats__Servers__0=nats://prod-nats:4222
Nats__CredsFile=/etc/nats/creds/connector.creds
Nats__UseJetStream=true
OntologyCache__EndpointUrl=http://fuseki:3030/ontology
```

---

## 6. Connector Descriptors

Each connector is defined by a YAML (or JSON) file in the `connectors/` directory.
Files are loaded by `DescriptorLoader` at startup and validated by `DescriptorValidator`.

### 6.1 Full descriptor schema

```yaml
# Required identity fields
driverId: <string>              # Unique driver identifier (e.g. pg-orders)
context: <string>               # Semantic context IRI-style (e.g. ctx:order-management)
sourceType: <string>            # Source type key (postgres|sqlserver|neo4j|mongodb|...)
description: <string>           # Human-readable description (optional)
enabled: <bool>                 # Default: true. Set false to disable without removing the file.

connection:
  # Use one of: connectionString OR individual fields
  connectionString: <string>    # Full connection string (overrides all below)
  host: <string>
  port: <int>
  database: <string>
  username: <string>
  password: <string>            # Use ${ENV_VAR} for secrets
  uri: <string>                 # For Neo4j bolt URI
  baseUrl: <string>             # For HTTP-based sources
  apiToken: <string>
  # OAuth (SharePoint)
  tenantId: <string>
  clientId: <string>
  clientSecret: <string>
  # SAP specific
  sapClient: <string>
  # SSL
  verifySsl: <bool>             # Default: true
  sslCertPath: <string>

changeDetection:
  mode: <cdc|polling|delta>
  # Polling
  pollIntervalSeconds: <int>    # Default: 30
  watermarkColumn: <string>     # Default: updated_at
  lookbackDuration: <ISO8601>   # Default: PT1H
  # CDC (Postgres)
  replicationSlot: <string>     # Default: uc_slot
  publication: <string>         # Default: uc_pub
  # SQL Server
  startingVersion: <long>       # Default: -1 (current)
  autoEnableChangeTracking: <bool>  # Default: true

watch:
  autoDiscover: <bool>          # true = discover all tables; false = use entities list
  entities:
    - name: <schema.table>
      primaryKey: [<column>, ...]
      filter: <string>          # Optional SQL/Cypher filter
      changeDetectionOverride: <cdc|polling>

fieldMapping:
  - source: <column_name>
    target: <renamed_name>      # Optional rename
    type: <string|int|long|double|bool|timestamp|date>  # Optional cast
    exclude: <bool>             # Exclude from output
    isKey: <bool>               # Route to PrimaryKey dict instead of Fields
    staticValue: <any>          # Inject a static value
    conceptMap:                 # Map raw values to semantic IRIs
      <raw_value>: <iri_or_label>

nats:
  subjectOverride: <string>     # Override computed subject entirely
  subjectTemplate: <string>     # Template with {context}, {entityPath}, {changeType}
  serializationFormat: json     # Currently always json (protobuf is used internally)
  additionalHeaders:
    <key>: <value>              # Extra NATS headers on every message

resilience:
  maxConsecutiveFailures: <int>     # Default: 5
  retryDelaySeconds: <int>          # Default: 10
  backoffMultiplier: <double>       # Default: 1.5
  maxRetryDelaySeconds: <int>       # Default: 120
```

### 6.2 Environment variable interpolation

Any value in the YAML can reference an environment variable:
```yaml
password: "${DB_PASSWORD}"
```
`DescriptorLoader` applies regex substitution (`\$\{([^}]+)\}`) before deserialization.
Missing variables are replaced with empty string; a warning is logged.

### 6.3 Validation rules (`DescriptorValidator`)

**Errors (block startup when `FailOnDescriptorError: true`):**
- `driverId`, `context`, `sourceType` must be non-empty
- `sourceType` must be one of: `postgres`, `sqlserver`, `neo4j`, `databricks`, `seeq`, `avevapi`, `sharepoint`, `sap`, `mongodb`
- `mode` must be valid for the source type:
  - `postgres`: `cdc`, `polling`
  - `sqlserver`: `cdc`, `polling`
  - `neo4j`: `polling`
  - `mongodb`: `cdc`, `polling`
  - `sharepoint`: `delta`
  - `databricks`: `cdc`, `polling`
  - `seeq`, `avevapi`, `sap`: `polling`
- Required connection fields per source type must be present

**Warnings (logged, do not block):**
- `autoDiscover: false` with empty `entities`
- CDC mode without explicit `replicationSlot`
- Passwords containing literal values instead of `${ENV_VAR}`

---

## 7. Infrastructure Services

### 7.1 `NatsConnectionFactory`
Singleton. Manages a single shared `NatsConnection`. Thread-safe lazy initialization with double-checked locking. Reads `NatsOptions` for server URLs and optional creds file.

### 7.2 `NatsPublisher` (implements `INatsPublisher`)

Publishes `RawChangeEvent` to NATS as a protobuf-serialized `Envelope`.

**Flow:**
1. Build subject (from `SubjectHint`, `subjectOverride`, or computed)
2. Build `Envelope` protobuf message
3. Serialize to `byte[]` via `ToByteArray()`
4. Log JSON representation at `Debug` level (via `Google.Protobuf.JsonFormatter`)
5. If circuit is open → route to DLQ
6. If `UseJetStream: false` → `conn.PublishAsync()` (core NATS)
7. If `UseJetStream: true` → `js.PublishAsync()` with up to 4 attempts (3 retries + 1 final)
8. On all attempts exhausted → open/advance circuit, route to DLQ

**Circuit breaker:**
- Threshold: 5 consecutive failures
- Half-open window: 30 seconds
- On open: events go straight to DLQ
- On any success: failure counter resets to 0

**Retry delays:** `[100ms, 1s, 10s]` before attempts 2, 3, 4.

**DLQ subject format:** `{dlqSubjectPrefix}.{originalSubject}` (lowercased)

**NATS Headers on every message:**
```
eventId       : <ulid>
driverId      : <driverId>
context       : <context>
sourceType    : <sourceType>
changeType    : <changeType>
content-type  : application/x-protobuf
<any additionalHeaders from descriptor>
```

**Observability:**
- `ActivitySource`: `CommonModel.Runtime.Infrastructure`, tags: `nats.subject`, `event.id`, `driver.id`
- Metrics (via `System.Diagnostics.Metrics`):
  - `cm.events.published` (counter, tag: `driver.id`)
  - `cm.events.dlq` (counter)
  - `cm.events.publish_retries` (counter, tag: `driver.id`)

### 7.3 `NatsCheckpointStore` (implements `ICheckpointStore`)
Stores and retrieves checkpoints from a NATS KV bucket (`CheckpointBucket` config key).
Key format: `{driverId}:{entityPath}` with colons replaced by dashes.
Position value: WAL LSN (CDC) or watermark timestamp (polling).

### 7.4 `StartupSelfTestService` (implements `IHostedService`)
Runs pre-flight checks on `StartAsync`:
1. **creds-file**: if `CredsFile` is configured, checks `File.Exists()`
2. **nats-connect**: connects and pings NATS (5s timeout)
3. **jetstream** (if `UseJetStream: true`): calls `js.GetAccountInfoAsync()`
4. **stream provisioning**: calls `EnsureStreamsAsync()` — creates `CDC` stream covering `cdc.>` if it doesn't exist

Stream `CDC` configuration:
```
Storage:   File
Retention: Limits
MaxAge:    1 day
Replicas:  1
```

If `StopOnCriticalFailure: true` and any check fails → `IHostApplicationLifetime.StopApplication()`.

### 7.5 `HealthHeartbeatService` (extends `BackgroundService`)
Publishes JSON `HealthStatus` to `{subjectPrefix}.{driverId}` every `IntervalSeconds`.
Fires immediately at startup (does not wait for the first interval).
When `UseJetStream: true`, uses JetStream with fallback to core NATS on failure.
When `UseJetStream: false` (default), uses core NATS directly.

### 7.6 `DriverLifecycleService` (extends `BackgroundService`)
Subscribes to `cdc.commands.*` on NATS.
Deserializes `LifecycleCommand` (JSON), validates via `LifecycleFsm`, executes via `IDriverLifecycleController`.
Publishes `LifecycleEvent` (JSON) to `cdc.lifecycle.{driverId}` after each state change.

### 7.7 `FusekiOntologyCache` (implements `IOntologyCache`)
HTTP client that queries an Apache Jena Fuseki SPARQL endpoint.
SPARQL query fetches `owl:Class`, `owl:ObjectProperty`, `owl:DatatypeProperty`, `owl:NamedIndividual`
with `rdfs:label` and `rdfs:subClassOf`.
Results cached in two `ConcurrentDictionary` instances: by IRI and by label.
If `EndpointUrl` is null → no-op (empty cache, no errors).
If `LoadOnStartup: false` → lazy load on first lookup.

### 7.8 `OntologyCacheRefreshService` (extends `BackgroundService`)
Subscribes to `cdc.ontology.refresh` on NATS.
On any message received → calls `IOntologyCache.RefreshAsync()`.
Also performs initial load if `LoadOnStartup: true`.

### 7.9 `NatsHealthCheck` (implements `IHealthCheck`)
Pings the NATS connection with a 3-second timeout.
Registered as a .NET health check; queryable via the standard health endpoint.

---

## 8. Generic Connector Engine

### 8.1 `BaseConnector` (abstract)

Implements retry/backoff loop for `StreamChangesAsync()`.
All drivers extend `BaseConnector`.

**Resilience behavior:**
- `MaxConsecutiveFailures` (default 5): failures before entering `Failed` state
- `RetryDelaySeconds` (default 10): base delay
- `BackoffMultiplier` (default 1.5): exponential backoff
- `MaxRetryDelaySeconds` (default 120): cap on delay
- On exception in `PollOrStreamAsync`: increments failure counter, logs warning, waits, retries
- On success: resets failure counter to 0

### 8.2 `GenericConnector` (extends `BaseConnector`)

Bridges a `ConnectorDescriptor` + `IProtocolAdapter` into a full `ISourceDriver`.

**In `PollOrStreamAsync`:**
1. Calls `_adapter.StreamRawChangesAsync(descriptor, ct)` → `IAsyncEnumerable<RawChangeRecord>`
2. For each record:
   - Resolves `EntityConfig` from descriptor
   - Gets `previousFields` from `raw.PreviousFields` if non-empty, otherwise looks up snapshot cache
   - Calls `_fieldMapper.Apply(raw.Fields, previousFields, rules, entityConfig)` → `(primaryKey, fields, prevFields)`
   - **Resolves `ChangeType.Snapshot`** — polling adapters (Neo4j, watermark SQL) always emit `Snapshot` because they query by watermark and cannot distinguish Insert from Update. The snapshot cache resolves this:
     ```
     Snapshot + key NOT in cache  →  Insert
     Snapshot + key IN cache      →  Update
     ```
   - Builds `Metadata` from descriptor and `AdapterMetadata`
   - Builds `SubjectHint` from `subjectOverride` or `subjectTemplate`
   - Yields `RawChangeEvent`
3. After each non-delete record: saves current `fields` to snapshot cache (keyed by `{entityPath}:{pk1=v1}:{pk2=v2}...`)
4. On delete: removes the key from the snapshot cache

### 8.3 `FieldMapper`

Applies transformation rules from the descriptor's `fieldMapping` section.

**Pipeline:**
1. Inject `staticValue` rules (run first, before source fields)
2. For each field in `fields` / `previousFields`:
   - If a rule matches (`source` key, case-insensitive):
     - Apply `target` rename
     - Apply `type` cast (`string|int|long|double|bool|timestamp|date`)
     - Apply `conceptMap` value substitution
     - If `exclude: true` → skip
     - If `isKey: true` → route to `primaryKey` dict
   - If no rule matches:
     - If column is in `entityConfig.PrimaryKey` → route to `primaryKey`
     - Else → route to `payload`
3. Returns `(primaryKey, payload, previousPayload)`

### 8.4 `DescriptorLoader`

Loads `.yaml`, `.yml`, or `.json` descriptor files from a directory.
Applies environment variable interpolation before deserialization.
Returns `DescriptorLoadResult` with `Ok`/`Fail` status and warnings.

### 8.5 `DescriptorStore`

`ConcurrentDictionary<string, ConnectorDescriptor>` keyed by `DriverId`.
`GetEnabled()` → filters to `Enabled == true`.
`ConnectorRegistry.ResolveAll()` calls `GetEnabled()`.

### 8.6 `DescriptorBootstrapService` (extends `BackgroundService`)

Hosted service that loads all descriptors from `GenericConnector:DescriptorDirectory` at startup.
Validates each descriptor. If `FailOnDescriptorError: true` and a descriptor fails → stops application.
Registers valid descriptors in `DescriptorStore`.

### 8.7 `AdapterRegistry`

Resolves `IProtocolAdapter` implementations by `SourceType` (case-insensitive).
Built from all registered `IProtocolAdapter` instances at startup.
Throws `InvalidOperationException` if sourceType is not found.

### 8.8 `LifecycleFsm`

Validates state transitions before executing lifecycle commands.

| Current State | Valid Actions |
|--------------|---------------|
| Disconnected | start |
| Failed | start, restart |
| Connecting | stop |
| Connected | stop |
| Streaming | stop, restart |
| Reconnecting | stop |

---

## 9. Protocol Adapters

All adapters extend `BaseProtocolAdapter` which guards `OpenAsync`/`CloseAsync` with an `_isOpen` flag.

### 9.1 `PostgresAdapter` (sourceType: `postgres`)

| Mode | Implementation |
|------|---------------|
| `cdc` | Npgsql `LogicalReplicationConnection`, pgoutput protocol V2 |
| `polling` | SQL SELECT with watermark column |

**CDC message types handled:**

| Message | Previous Values | Action |
|---------|----------------|--------|
| `InsertMessage` | None | ChangeType.Insert |
| `FullUpdateMessage` | Full old row (`OldRow`) | ChangeType.Update + PreviousFields |
| `IndexUpdateMessage` | Identity columns (`Key`) | ChangeType.Update + PreviousFields (PK only) |
| `UpdateMessage` | None | ChangeType.Update |
| `FullDeleteMessage` | Full old row | ChangeType.Delete |
| `KeyDeleteMessage` | PK columns | ChangeType.Delete |

**Startup provisioning (`EnsureReplicationSlotAndPublication`):**
1. Checks `wal_level = 'logical'` — throws if not
2. Creates publication if missing: `CREATE PUBLICATION {pub} FOR ALL TABLES`
3. Creates replication slot if missing: `pg_create_logical_replication_slot`
4. Sets `REPLICA IDENTITY FULL` on each watched entity (idempotent)

> **REPLICA IDENTITY FULL is required** for `PreviousFields` to be populated on
> UPDATE and DELETE operations. The adapter applies this automatically.

### 9.2 `SqlServerAdapter` (sourceType: `sqlserver`)

| Mode | Implementation |
|------|---------------|
| `cdc` | `CHANGETABLE(CHANGES ...)` — SQL Server Change Tracking |
| `polling` | SQL SELECT with watermark column |

Change Tracking columns: `SYS_CHANGE_VERSION`, `SYS_CHANGE_OPERATION` (I/U/D).
Auto-enables Change Tracking on database and tables if `autoEnableChangeTracking: true`.
Tracks per-entity version numbers for incremental polling.

**DELETE event primary key handling:**
The `CHANGETABLE` query uses a `LEFT JOIN` to the source table. On `DELETE`, all `t.*` columns are `NULL` because the row is gone. The adapter always selects PK columns from the `ct` (CHANGETABLE) side so DELETE events always carry a meaningful identity:

```sql
SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, ct.SYS_CHANGE_COLUMNS,
       ct.[pk1], ct.[pk2],    -- always present even on DELETE
       t.*                    -- NULL on DELETE (row is gone)
FROM CHANGETABLE(CHANGES {table}, @from) AS ct
LEFT JOIN {table} t ON t.[pk1] = ct.[pk1] AND t.[pk2] = ct.[pk2]
```

For DELETE events `fields` is set to only the PK values (`pkValues`); all `t.*` nulls are discarded.

### 9.3 `Neo4jAdapter` (sourceType: `neo4j`)

Mode: `polling` only.
Uses Bolt protocol (`neo4j.Driver`).
Supports both node entities and relationship entities (prefixed `REL:`).
Watermark-based polling with configurable lookback.

**Delete detection — snapshot diff:**
Neo4j has no native change stream or DELETE notification. The adapter detects deletes by performing a full scan of all nodes/relationships of each entity type every poll cycle and diffing the current PK set against the previous cycle's set.

```
_knownKeys[entityName]              = set of PK strings seen last cycle
_lastKnownFields[entityName][pk]    = last-known field values for that PK
```

Each poll cycle:
1. **Full scan** — fetch all nodes of that type; collect current PK set and update `_lastKnownFields`
2. **Diff** — emit `ChangeType.Delete` for every PK in `_knownKeys` that is absent from the current scan
3. **Watermark filter** — emit `ChangeType.Snapshot` for records whose watermark property exceeds `since`
4. **Update** `_knownKeys` with the current set

> **Limitation:** deletes that happen while the connector is stopped are missed on restart, because `_knownKeys` is rebuilt from the first full scan. The snapshot cache is in-memory only — there is no persistent delete checkpoint.

### 9.4 `DatabricksAdapter` (sourceType: `databricks`)

| Mode | Implementation |
|------|---------------|
| `cdc` | Delta Change Data Feed (CDF) — queries `DESCRIBE HISTORY`, streams from versions |
| `polling` | Watermark-based SELECT via ODBC |

Connection: Simba Spark ODBC driver with token auth.

### 9.5 `MongoDbAdapter` (sourceType: `mongodb`)

| Mode | Implementation |
|------|---------------|
| `cdc` | MongoDB Change Streams with resume tokens |
| `polling` | Per-collection watermark query |

Resume tokens stored in checkpoint store for crash recovery.

### 9.6 `HttpRestAdapter` (sourceType: `sharepoint` | `sap` | `seeq` | `avevapi`)

| Source | Auth | Mode | Notes |
|--------|------|------|-------|
| `sharepoint` | OAuth (tenant/client/secret) | `delta` | Delta links for incremental changes |
| `sap` | Basic auth | `polling` | SAP OData API |
| `seeq` | Token | `polling` | Seeq REST API |
| `avevapi` | Token | `polling` | AVEVA PI historian API |

---

## 10. Host Application

### 10.1 `Program.cs`

```csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddUniversalConnector(builder.Configuration);
var host = builder.Build();
await host.RunAsync();
```

### 10.2 `ServiceCollectionExtensions.AddUniversalConnector()`

Registration order and types:

```
Configuration bindings:
  Nats           → IOptions<NatsOptions>
  OntologyCache  → IOptions<OntologyCacheOptions>
  Heartbeat      → IOptions<HeartbeatOptions>

Singletons:
  NatsConnectionFactory
  INatsPublisher         ← NatsPublisher
  ICheckpointStore       ← NatsCheckpointStore
  IEventPipeline         ← DefaultEventPipeline
  IOntologyCache         ← FusekiOntologyCache  (with HttpClient)
  LifecycleFsm
  IConnectorRegistry     ← ConnectorRegistry
  ConnectorPipelineService  (also: IDriverLifecycleController)

Hosted services:
  StartupSelfTestService
  OntologyCacheRefreshService
  DriverLifecycleService
  HealthHeartbeatService
  DescriptorBootstrapService   (via AddGenericConnector)
  ConnectorPipelineService

Health checks:
  NatsHealthCheck

Generic connector subsystem (AddGenericConnector):
  DescriptorValidator
  IDescriptorLoader      ← DescriptorLoader
  DescriptorStore
  FieldMapper
  AdapterRegistry
  Adapters:
    PostgresAdapter, SqlServerAdapter, Neo4jAdapter,
    DatabricksAdapter, MongoDbAdapter,
    HttpRestAdapter (x4: sharepoint, sap, seeq, avevapi)
  IDriverFactory         ← MultiSourceGenericFactory
```

### 10.3 `ConnectorPipelineService`

Dual role: `BackgroundService` + `IDriverLifecycleController`.

**Startup sequence:**
1. `IConnectorRegistry.ResolveAll()` → creates `ISourceDriver` for each enabled descriptor
2. For each driver: launches `RunDriverLoopAsync()` as a background `Task`
3. Driver loop: `ConnectAsync()` → `StreamChangesAsync()` → `IEventPipeline.ProcessAsync()` per event

**Lifecycle methods:**
- `StopAsync(driverId)`: cancels driver token, waits up to 15s for loop to finish
- `StartAsync(driverId)`: creates new CTS, relaunches `RunDriverLoopAsync()`
- `RestartAsync(driverId)`: stop then start
- `GetAllHealth()`: delegates to `ISourceDriver.GetHealth()` for each registered driver

### 10.4 `DefaultEventPipeline`

```csharp
public async Task ProcessAsync(RawChangeEvent evt, CancellationToken ct)
{
    await _publisher.PublishAsync(evt, evt.SubjectHint, null, ct);
    var position = evt.SourceTimestamp?.ToString("O") ?? evt.EventId;
    await _checkpoint.SaveAsync(new Checkpoint
    {
        DriverId   = evt.DriverId,
        EntityPath = evt.EntityPath,
        Position   = position
    }, ct);
}
```

---

## 11. Wire Format — Protobuf Envelope

**File:** `src/CommonModel.Runtime.Infrastructure/Protos/envelope.proto`
**Package:** `commonmodel.runtime.v1`
**C# namespace:** `CommonModel.Runtime.Infrastructure.Wire`

```protobuf
syntax = "proto3";
package commonmodel.runtime.v1;

import "google/protobuf/timestamp.proto";

message Envelope {
  string event_id        = 1;   // ULID string
  Timestamp detected_at  = 2;   // When the connector detected the change
  Timestamp source_timestamp = 3;  // When the source system made the change
  string source_type     = 4;   // e.g. "postgres"
  string driver_id       = 5;   // e.g. "pg-orders"
  string context         = 6;   // e.g. "ctx:order-management"
  string entity_path     = 7;   // e.g. "public.orders"
  string change_type     = 8;   // "Insert"|"Update"|"Delete"|"Snapshot"
  map<string,string> primary_key    = 9;   // e.g. {"id": "42"}
  map<string,string> fields         = 10;  // New (or current) field values
  map<string,string> previous_fields = 11; // Old field values (UPDATE/DELETE)
  map<string,string> metadata       = 12;  // Adapter and descriptor metadata
  int64 sequence_number  = 13;  // Monotonically increasing per driver
}
```

**Schema evolution rules:**
- **Compatible (safe):** adding new optional fields with new field numbers; adding enum values
- **Breaking (coordinate consumers):** removing/renaming fields; changing field types; reusing field numbers

---

## 12. NATS Subject Schema

### Event subjects

```
cdc.{context}.{entityPath}.{changeType}    (when context is set)
cdc.{sourceType}.{driverId}.{changeType}   (fallback)
```

Context colons are replaced with dashes:
`ctx:order-management` → `ctx-order-management`

Example: `cdc.ctx-order-management.public.orders.update`

`subjectOverride` in descriptor overrides the entire computed subject.
`subjectTemplate` allows partial override using `{context}`, `{entityPath}`, `{changeType}` tokens.

### System subjects

| Subject | Direction | Format | Purpose |
|---------|-----------|--------|---------|
| `cdc.health.<driverId>` | Connector → | JSON `HealthStatus` | Driver heartbeat (every 30s) |
| `cdc.commands.<driverId>` | → Connector | JSON `LifecycleCommand` | Start/stop/restart a driver |
| `cdc.lifecycle.<driverId>` | Connector → | JSON `LifecycleEvent` | State change notification |
| `cdc.ontology.refresh` | → Connector | any | Trigger ontology cache reload |
| `cdc.dlq.<original.subject>` | Connector → | Protobuf `Envelope` | Failed publish fallback |

### JetStream stream

| Property | Value |
|----------|-------|
| Name | `CDC` |
| Subjects | `cdc.>` |
| Storage | File |
| Retention | Limits |
| Max age | 1 day |
| Replicas | 1 |

---

## 13. Connector YAML Files

Located in `connectors/` at the solution root. Loaded at startup from path configured in `GenericConnector:DescriptorDirectory`.

| File | driverId | sourceType | Mode | Enabled |
|------|----------|------------|------|---------|
| `postgres-orders.yaml` | `pg-orders` | postgres | cdc | ✅ |
| `postgres-aveva.yaml` | `pg-aveva` | postgres | polling | ✅ |
| `example-postgres.yaml` | `pg-assets-polling` | postgres | polling | ✅ |
| `neo4j-graph.yaml` | `neo4j-graph` | neo4j | polling | ❌ |
| `mongodb-assets.yaml` | `mongodb-assets` | mongodb | cdc | ❌ |
| `sqlserver-crm.yaml` | `sqlserver-crm` | sqlserver | cdc | ❌ |
| `sqlserver-equipment.yaml` | `sqlserver-equipment` | sqlserver | cdc | ❌ |
| `avevapi-historian.yaml` | `avevapi-historian` | avevapi | polling | ✅ |
| `databricks-lakehouse.yaml` | `databricks-lakehouse` | databricks | cdc | ✅ |
| `sap-s4hana.yaml` | `sap-s4hana` | sap | polling | ✅ |
| `seeq-plant.yaml` | `seeq-plant` | seeq | polling | ✅ |
| `sharepoint-docs.yaml` | `sharepoint-docs` | sharepoint | delta | ✅ |

Context naming convention: `ctx:{domain}` (e.g. `ctx:order-management`, `ctx:asset-management`, `ctx:crm`).

---

## 14. Resilience & Circuit Breaker

### 14.1 Driver-level retry (BaseConnector)

Configured per connector via descriptor `resilience` section.

```
On exception in PollOrStreamAsync:
  consecutiveFailures++
  if consecutiveFailures >= maxConsecutiveFailures → state = Failed
  delay = min(retryDelaySeconds * backoffMultiplier^(failures-1), maxRetryDelaySeconds)
  wait delay → retry
On success:
  consecutiveFailures = 0
  state = Streaming
```

### 14.2 Publisher-level circuit breaker (NatsPublisher)

```
Threshold:        5 consecutive publish failures
Half-open window: 30 seconds

CLOSED (normal):
  Attempt 1 → [100ms] → Attempt 2 → [1s] → Attempt 3 → [10s] → Attempt 4
  All fail → circuitFailures++
  If circuitFailures >= 5 → record openedAt timestamp

OPEN:
  elapsed < 30s → route event directly to DLQ (no attempts)

HALF-OPEN (after 30s):
  Next event gets a full 4-attempt cycle
  On success → circuitFailures = 0 → CLOSED
  On failure → reset openedAt → back to OPEN
```

### 14.3 DLQ routing

Events that exceed all retry attempts are published to:
`{dlqSubjectPrefix}.{originalSubject}` (lowercased)

DLQ uses core NATS (not JetStream) so a DLQ publish failure cannot cause infinite recursion.

---

## 15. Lifecycle Management

Drivers can be controlled at runtime by publishing `LifecycleCommand` messages to `cdc.commands.<driverId>`.

### Valid state transitions

```
Disconnected  --start-->  Connecting
Failed        --start-->  Connecting
Failed        --restart-->Connecting
Connecting    --stop-->   Disconnected
Connected     --stop-->   Disconnected
Streaming     --stop-->   Disconnected
Streaming     --restart-->Connecting
Reconnecting  --stop-->   Disconnected
```

### Command message format (JSON)
```json
{
  "commandId": "01J8...",
  "driverId": "pg-orders",
  "action": "restart",
  "requestedBy": "operator",
  "issuedAt": "2026-05-13T10:00:00Z"
}
```

### Lifecycle event published after state change
```json
{
  "driverId": "pg-orders",
  "state": "Connecting",
  "previousState": "Streaming",
  "triggeringAction": "restart",
  "commandId": "01J8...",
  "occurredAt": "2026-05-13T10:00:01Z"
}
```

---

## 16. Health & Heartbeats

### Heartbeat payload (JSON, published every 30s to `cdc.health.<driverId>`)
```json
{
  "driverId": "pg-orders",
  "sourceType": "postgres",
  "state": "Streaming",
  "lastChecked": "2026-05-13T10:00:00Z",
  "lastEventAt": "2026-05-13T09:59:58Z",
  "totalEventsEmitted": 1234,
  "consecutiveFailures": 0,
  "lastError": null
}
```

Health subjects use **core NATS** (not JetStream) — fire-and-forget.
No stream needed. Use `UseJetStream: false` in `Heartbeat` config (default).

### .NET Health Check
`NatsHealthCheck` registered with `AddHealthChecks()`.
Pings NATS with 3s timeout. Available via the standard .NET health endpoint.

---

## 17. Ontology Cache (Fuseki)

Optional semantic enrichment layer. Disabled by default (`EndpointUrl: null`).

**When enabled:**
- Connects to Apache Jena Fuseki SPARQL endpoint
- Queries all named concepts (classes, properties, individuals) with labels and hierarchy
- Caches in memory: `IRI → OntologyEntry` and `Label → [OntologyEntry]`
- `FieldMapper.ApplyConceptMap()` translates raw field values to semantic IRIs

**SPARQL query issued:**
```sparql
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?iri ?label ?parent ?type
WHERE {
  VALUES ?type { owl:Class owl:ObjectProperty owl:DatatypeProperty owl:NamedIndividual }
  ?iri a ?type .
  OPTIONAL { ?iri rdfs:label ?label . FILTER(lang(?label) = "en" || lang(?label) = "") }
  OPTIONAL { ?iri rdfs:subClassOf ?parent . FILTER(!isBlank(?parent)) }
}
ORDER BY ?iri
```

**To enable:**
1. Start Fuseki: `docker compose --profile fuseki up -d fuseki`
2. Upload ontology (`.ttl`/`.owl`) at `http://localhost:3030`
3. Set `OntologyCache:EndpointUrl` and `OntologyCache:LoadOnStartup: true`
4. Add `conceptMap` entries to connector descriptors

**Hot reload:** Publish any message to `cdc.ontology.refresh` → cache re-queries Fuseki.

---

## 18. Checkpoints

Checkpoints allow the connector to resume from its last position after a restart.

**Stored in:** NATS KV bucket (default name: `cm-checkpoints`)

**Key format:** `{driverId}:{entityPath}` with colons replaced by dashes
Example: `pg-orders:public.orders` → key `pg-orders-public.orders`

**Position value:**
- CDC (Postgres): WAL LSN or `SourceTimestamp.ToString("O")`
- Polling: watermark column value (DateTimeOffset ISO-8601)
- Fallback: `EventId` (ULID)

**Checkpoint is saved after every successfully published event** in `DefaultEventPipeline`.

---

## 19. Test Coverage

**222 tests, 0 failures. Target: .NET 10.**

| Test File | What is tested |
|-----------|---------------|
| `BaseConnectorTests.cs` | Retry/backoff logic, state transitions, failure counting |
| `DescriptorLoaderTests.cs` | YAML/JSON parsing, env var interpolation, error handling |
| `DescriptorStoreTests.cs` | ConcurrentDictionary operations, GetEnabled filtering |
| `DescriptorValidatorTests.cs` | All validation rules, warnings, error messages |
| `FieldMapperTests.cs` | Rename, cast, exclude, isKey, staticValue, previousFields mapping |
| `ConceptMapTests.cs` | ConceptMap value substitution |
| `SubjectTemplateResolverTests.cs` | Subject computation from template/override/default |
| `NatsPublisherTests.cs` | Circuit breaker, retry logic, DLQ routing, core vs JetStream paths |
| `CheckpointStoreTests.cs` | Get/Save round-trip with NATS KV mock |
| `NatsConnectionFactoryTests.cs` | Singleton connection, thread safety |
| `StartupSelfTestServiceTests.cs` | Pre-flight check pass/fail, StopOnCriticalFailure behavior |
| `HealthHeartbeatTests.cs` | Heartbeat publish interval, UseJetStream=false default |
| `DriverLifecycleServiceTests.cs` | Command handling, FSM integration, event publishing |
| `LifecycleFsmTests.cs` | All valid/invalid transition combinations |
| `FusekiOntologyCacheTests.cs` | SPARQL response parsing, cache population, refresh |
| `OntologyCacheTests.cs` | GetByIri, FindByLabel, null endpoint short-circuit |
| `ConnectorPipelineServiceTests.cs` | Driver loop orchestration, lifecycle controller methods |

**Test helpers:**
- `InMemoryCheckpointStore` — test double for `ICheckpointStore`
- `InMemoryOntologyCache` — test double for `IOntologyCache`

---

## 20. Infrastructure — Docker & NATS

### 20.1 Dockerfile (multi-stage)

```dockerfile
# Stage 1: restore (layer-cached by copying .csproj files first)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS restore
# copies *.csproj files, runs dotnet restore --runtime linux-x64

# Stage 2: publish
FROM restore AS publish
# copies src/, runs dotnet publish -c Release -r linux-x64 --self-contained false

# Stage 3: runtime
FROM mcr.microsoft.com/dotnet/runtime:10.0
# - creates non-root appuser
# - copies published binaries and connectors/ directory
# - HEALTHCHECK via pgrep
# ENTRYPOINT: dotnet CommonModel.Runtime.Host.dll
```

### 20.2 Docker Compose services

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `nats` | `nats:2-alpine` | 4222, 8222 | Message broker with JetStream |
| `postgres` | `postgres:16-alpine` | 5433:5432 | PostgreSQL with `wal_level=logical` |
| `neo4j` | `neo4j:5` | 7474, 7687 | Graph database |
| `fuseki` *(profile)* | `stain/jena-fuseki` | 3030 | SPARQL endpoint for ontology |
| `nats-surveyor` *(profile)* | surveyor | 7777 | NATS monitoring UI |

Run with profiles:
```powershell
docker compose up -d                          # nats + postgres + neo4j
docker compose --profile fuseki up -d fuseki  # + ontology server
```

### 20.3 NATS configuration (`docker/nats/nats.conf`)

```
jetstream {
  store_dir: /data/jetstream
  max_mem:   512MB
  max_file:  10GB
}
http_port: 8222
```

### 20.4 PostgreSQL CDC prerequisites

```sql
-- Required in postgresql.conf:
wal_level = logical

-- Applied automatically by PostgresAdapter at startup:
ALTER TABLE <entity> REPLICA IDENTITY FULL;

-- Created automatically by PostgresAdapter if missing:
CREATE PUBLICATION uc_pub FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('uc_slot', 'pgoutput');
```

> **Important — shared Docker instance:** The single `postgres` container hosts both the `pg-orders` CDC connector (which creates a logical replication slot `uc_slot`) and the `pg-aveva` polling connector. Because a logical replication slot already exists on the instance, PostgreSQL refuses to start if `wal_level` is set to anything below `logical`. **Do not change `wal_level` to `replica` or `minimal`** — the container will fail to start. Keep `wal_level=logical` even though `pg-aveva` only uses polling.

### 20.5 `aveva_db` schema (`pg-aveva` connector)

The `pg-aveva` connector watches two tables in the `aveva_db` database (port 5433):

**`public.assets`**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | integer | NO | Serial PK |
| `asset_id` | varchar | NO | Business key (connector PK) |
| `name` | varchar | NO | |
| `type` | varchar | YES | |
| `category` | varchar | YES | |
| `manufacturer` | varchar | YES | |
| `model` | varchar | YES | |
| `serial_number` | varchar | YES | |
| `status` | varchar | NO | operational / maintenance / offline |
| `description` | text | YES | |
| `location_id` | varchar | YES | FK to `public.locations` |
| `install_date` | date | YES | |
| `last_maintenance_date` | date | YES | |
| `tags` | ARRAY | YES | e.g. `ARRAY['critical','cooling']` |
| `specs` | jsonb | YES | Arbitrary key/value specs |
| `created_at` | timestamptz | NO | |
| `updated_at` | timestamptz | NO | Watermark column |

**`public.locations`** — primaryKey: `location_id`

---

## 21. Python Consumer (`C:\Repos\NTAS_Consumer`)

Standalone Python script to consume and display CDC events from NATS.

### Files

| File | Purpose |
|------|---------|
| `consumer.py` | Main async consumer |
| `envelope.proto` | Copy of the protobuf schema (must be kept in sync) |
| `envelope_pb2.py` | Generated Python protobuf bindings (from protoc) |
| `requirements.txt` | `nats-py>=2.7`, `protobuf>=4.25` |
| `README.md` | Setup and run instructions |

### Setup

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install grpcio-tools
python -m grpc_tools.protoc --python_out=. -I. envelope.proto
```

### Usage

```powershell
python consumer.py                                    # all events on cdc.>
python consumer.py --subject "cdc.postgres.>"         # postgres only
python consumer.py --jetstream --stream CDC --durable my-consumer
```

### Output format

**UPDATE (with previous values):**
```
═══════════════════════════════════════════════════════════════════════════
Subject         : cdc.ctx-order-management.public.orders.update
ChangeType      : UPDATE
EntityPath      : public.orders
PrimaryKey      : id=42
PreviousFields  : status=processing, updated_at=2026-05-13T09:11:02
NewFields       : status=shipped,    updated_at=2026-05-13T10:23:10
Changes:
  status      processing           ►  shipped
  updated_at  2026-05-13T09:11:02  ►  2026-05-13T10:23:10
Unchanged       : customer_id, id, total_amount
```

**Heartbeat (`cdc.health.*`) — JSON decoded, printed inline:**
```
Subject         : cdc.health.pg-orders    [HEARTBEAT]
  state          : Streaming
  totalEventsEmitted : 47
```

---

## 22. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Single-purpose connector** — detect and publish only | Downstream consumers handle persistence, transformation, integration |
| **Protobuf wire format** | Schema evolution safety, compact binary, language-agnostic |
| **NATS JetStream** for events | At-least-once delivery, replay, durable consumers |
| **Core NATS** for heartbeats | Fire-and-forget; no stream required; simpler operations |
| **ULID for EventId** | Sortable, URL-safe, unique, monotonic within millisecond |
| **REPLICA IDENTITY FULL** auto-applied | Ensures `PreviousFields` is always populated for Postgres CDC |
| **Circuit breaker in publisher** | Prevents cascading failures from NATS unavailability |
| **DLQ via core NATS** | DLQ path must not depend on JetStream to avoid infinite loops |
| **YAML descriptors** | Operators configure connectors without recompiling |
| **Snapshot cache for polling** | Polling adapters have no native prev-value support; in-memory cache provides it |
| **Fuseki optional** | Connector works without ontology; feature activates only when `EndpointUrl` is set |
| **LifecycleFsm** | Prevents invalid state transitions; commands validated before execution |
| **`enabled: false` in YAML** | Turn off a connector without removing the file; no code changes needed |
| **Neo4j delete detection via snapshot diff** | Neo4j has no native delete event; full scan + PK diff per poll cycle detects disappearing nodes. Limitation: deletes during downtime are missed (in-memory only) |
| **SQL Server DELETE carries PK via `ct.*`** | `CHANGETABLE` LEFT JOIN returns NULL for all table columns on DELETE; PK columns are always selected from the `ct` side to preserve identity |
| **`ChangeType.Snapshot` resolved in GenericConnector** | Polling adapters cannot distinguish Insert from Update; the snapshot cache makes the determination: unseen key → Insert, known key → Update |
