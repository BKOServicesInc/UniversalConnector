# UniversalConnector → AssetLink Compliance Plan

> **Scope:** Connector runtime only (Layers 1–4 + driver contract).
> No projectors, no publisher SDK, no graph layer.
> This is a review document — no code changes yet.

---

## Current State vs. Required State

| Area | Current | AssetLink Required |
|---|---|---|
| Wire format | JSON (`DataChangeEvent`) | Protobuf envelope (`Envelope` + `Any` payload) |
| Event model | Custom `DataChangeEvent` record | Generated C# POCOs from Protobuf / LinkML |
| Subject pattern | `{SubjectPrefix}.{ConnectorId}.{EntityPath}` (approximate) | `cdc.<context>.<aspect-path>.<event_type>` |
| JetStream | Optional (`UseJetStream = false`) | Mandatory for all CDC events |
| Checkpointing | None | JetStream KV store per connector |
| DLQ | None | `cdc.dlq.<connector-id>` |
| Retry on publish | None | Backoff: [100ms, 1s, 10s], then DLQ |
| Lifecycle state machine | Simple `BackgroundService` start/stop | Full FSM: stopped → starting → running → paused → failed |
| Control plane (NATS commands) | None | `connectors.<id>.command` subscriber |
| Health heartbeat | None | `connectors.<id>.health` every 30s |
| Lifecycle events | None | `connectors.<id>.events.lifecycle` (JetStream durable) |
| Ontology cache (Layer 2) | **Does not exist** | Fuseki SPARQL at startup; refresh on `ontology.updated.<context>` |
| Mapping layer (Layer 3) | **Does not exist** | YAML rules file per connector: concept resolvers, attribute maps, enum maps |
| Driver contract | `IDataSourceConnector` → `DataChangeEvent` | `ISourceDriver` → `RawChangeEvent` |
| Driver loading | Unknown (Generic project) | MEF / assembly scan via `[Export(typeof(ISourceDriver))]` |
| Configuration | `appsettings.json` sections | Per-connector YAML with full schema; secrets via env vars |
| NATS credentials | `CredsFile` path (optional) | Required per connector; startup self-test before source connection |
| `event_id` | None | ULID, generated per event |
| `ontology_version` | None | Required on every envelope; refuse to publish without it |
| Provenance | None | `connector_id`, `source_system`, `source_ts`, `publish_ts` on every event |
| Project naming | `UniversalConnector.*` | `CommonModel.Runtime.*` per AssetLink repo convention |
| NATS client version | Need to verify | Must be `NATS.Net` v2+, NOT `NATS.Client` v1 |

---

## Gap Details by Layer

---

### Layer 1 — Lifecycle & Control Plane

**File to replace:** [`src/UniversalConnector.Host/ConnectorPipelineService.cs`](src/UniversalConnector.Host/ConnectorPipelineService.cs)

#### 1.1 State machine
The current `ConnectorPipelineService` has no formal state machine — it simply starts and runs until `CancellationToken` fires. AssetLink requires:

```
stopped → starting → running ←→ paused
                ↓                  ↓
              failed ← restart ←──┘
```

Introduce a `ConnectorState` enum and a state-transition guard that logs and publishes each transition.

#### 1.2 NATS command subscriber
Subscribe to `connectors.<connector-id>.command` (core NATS, not JetStream) and handle:

| Command | Action |
|---|---|
| `start` | stopped → starting |
| `stop` | any → stopped (graceful drain) |
| `pause` | running → paused |
| `resume` | paused → running |
| `reload-config` | re-read YAML, restart driver if config changed |
| `refresh-ontology` | force Layer 2 cache refresh |
| `replay-from-checkpoint` | driver re-emits from a specified checkpoint |
| `replay-from-zero` | full snapshot replay |
| `health` | force-publish health immediately |

Commands are **reply-style** — ack with success or error on the reply inbox.

#### 1.3 Health heartbeat
Publish every 30 seconds to `connectors.<id>.health` (core NATS, NOT JetStream — ephemeral). Payload includes:
- `connector_id`, `state`, `uptime_seconds`, `ontology_version`
- Driver name, version, lag_seconds, last_checkpoint
- Throughput (events/min 1m and 5m)
- Error counts, DLQ count in 24h

#### 1.4 Lifecycle events
Publish every state transition to `connectors.<id>.events.lifecycle` — this IS JetStream durable.

#### 1.5 Graceful shutdown
On `stop` command or `SIGTERM`:
1. Stop driver from emitting
2. Drain in-flight events through Mapping → Publisher
3. Flush JetStream pending acks
4. Publish `state: stopped` lifecycle event
5. Drain NATS connection
6. Exit cleanly (configurable max drain timeout)

---

### Layer 2 — Ontology Cache *(entirely new)*

**New file needed:** `CommonModel.Runtime.Core/Ontology/OntologyCache.cs`

This layer does not exist at all in the current codebase.

#### 2.1 Startup loading
Connect to Fuseki via `ontology.fuseki_url` from config:
1. Query version registry for latest ontology version satisfying `expected_version` constraint.
2. Fetch named graphs for the connector's declared `contexts`.
3. Build in-memory:
   - Class definitions (dict keyed by URI)
   - Slot/property definitions per class
   - Crosswalk mappings (source class URI → target class URI + predicate)
4. Refuse to start if Fuseki is unreachable.

#### 2.2 Refresh on notification
Subscribe to `ontology.updated.<context>` for each declared context. On notification:
1. Acquire write lock on cache
2. Re-fetch affected graphs
3. Build new representation alongside existing (atomic swap on completion)
4. Subsequent events use new version

#### 2.3 Failure handling
- Cannot reach Fuseki at startup → refuse to start, log clearly
- Cannot reach during runtime → keep cached version, log warning, retry in background
- Refresh fails validation → keep old cache, emit lifecycle event warning
- Running on deprecated ontology → warn on every health beat; hard-fail after grace period

---

### Layer 3 — Mapping & Enrichment *(entirely new)*

**New file needed:** `CommonModel.Runtime.Core/Mapping/`

This layer does not exist. Currently drivers emit `DataChangeEvent` (which already has canonical fields); after alignment the driver emits `RawChangeEvent` and this layer does the translation.

#### 3.1 RawChangeEvent (driver output)
Replace `DataChangeEvent` with:

```csharp
public record RawChangeEvent(
    string SourceId,          // table name, API endpoint, NodeId, etc.
    string Operation,         // "insert" | "update" | "delete" | "upsert" | "snapshot"
    IReadOnlyDictionary<string, object> Key,
    IReadOnlyDictionary<string, object>? Before,
    IReadOnlyDictionary<string, object>? After,
    DateTimeOffset SourceTimestamp,
    IReadOnlyDictionary<string, object> RawMetadata);
```

#### 3.2 Mapping rules file (YAML, per connector instance)
External declarative file — not code. Contains:
- `concept_resolvers`: match source records to canonical concepts + context
- `attribute_mappings`: field-to-field mappings with templates
- `enum_mappings`: raw enum codes to canonical values
- `subject_template`: override for subject construction

#### 3.3 Mapping process per event
1. Match raw event's `SourceId` against concept resolvers → determine concept + context
2. Apply attribute mappings → populate canonical C# POCO
3. Tag with `ontology_version` from Layer 2 cache
4. Validate required fields and types
5. Emit cross-context `references` fields for the projector to resolve (no Neo4j reads on hot path)

#### 3.4 Validation failure handling
Invalid events → routed to local error log + metric increment. Runtime does NOT publish invalid events.

---

### Layer 4 — Publisher

**File to modify:** [`src/UniversalConnector.Nats/NatsPublisher.cs`](src/UniversalConnector.Nats/) (significant rework)

#### 4.1 Protobuf envelope
Replace JSON publishing with Protobuf `Envelope` wrapping a `google.protobuf.Any` payload:

```protobuf
message Envelope {
  string event_id = 1;               // ULID
  string envelope_version = 2;       // "v1"
  string canonical_schema = 3;       // FQN e.g. "common_model.events.v1.EquipmentUpsert"
  string canonical_schema_version = 4;
  string ontology_version = 5;       // REQUIRED — refuse to publish without it
  string context = 6;                // ctx:PID, ctx:AF-Process, etc.
  string source_system = 7;
  string connector_id = 8;
  google.protobuf.Timestamp source_ts = 9;
  google.protobuf.Timestamp publish_ts = 10;
  string trace_id = 11;
  string parent_event_id = 12;
  google.protobuf.Any payload = 13;
}
```

**New NuGet dependencies needed:** `Google.Protobuf`, `Grpc.Tools`, `CommonModel.CanonicalEvents` (once published).

#### 4.2 Subject computation
Replace the current subject template with:
```
cdc.<context>.<aspect-path>.<event_type>
```
Driven by `subject_template` in the connector YAML config. Tokens are lowercase-hyphenated. Original casing stays in the payload, not the subject.

#### 4.3 JetStream — mandatory
`UseJetStream` becomes required (not optional). Publish with `expected_stream: CDC`. Await ack with configurable timeout.

#### 4.4 Retry + DLQ
On nak or timeout:
- Retry with backoff `[100ms, 1000ms, 10000ms]`
- After exhausting retries → publish to `cdc.dlq.<connector-id>` with original event + `failure_reason`

#### 4.5 Checkpoint management
After successful JetStream ack, update checkpoint in JetStream KV:
```
Bucket: connector-checkpoints
Key:    <connector-id>
Value:  { driver_checkpoint: <opaque>, last_event_id: <ulid>, updated_at: <ts> }
```
The `driver_checkpoint` structure is driver-defined and opaque to the runtime.

On startup, load checkpoint and pass to `ISourceDriver.StartAsync`.

#### 4.6 ULID event_id
Add `Ulid` package (or equivalent) — every event gets a new ULID at publish time.

#### 4.7 Backpressure
Publisher uses a **bounded** internal queue. If JetStream is slow, the queue fills → mapping layer blocks → driver backs off. No unbounded buffering.

---

### Layer 5 — Driver Contract (ISourceDriver)

**File to modify:** [`src/UniversalConnector.Core/Abstractions/IDataSourceConnector.cs`](src/UniversalConnector.Core/Abstractions/)

Rename and reshape `IDataSourceConnector` → `ISourceDriver`:

#### Current interface
```csharp
public interface IDataSourceConnector
{
    string ConnectorId { get; }
    string SourceType { get; }
    Task ConnectAsync(CancellationToken ct);
    IAsyncEnumerable<DataChangeEvent> StreamChangesAsync(CancellationToken ct);
    Task DisconnectAsync(CancellationToken ct);
    IAsyncDisposable (DisposeAsync)
}
```

#### Required interface
```csharp
public interface ISourceDriver
{
    string Name { get; }
    string Version { get; }
    Task StartAsync(IDictionary<string, object> config, Checkpoint? checkpoint, CancellationToken ct);
    Task StopAsync(CancellationToken ct);
    IAsyncEnumerable<RawChangeEvent> ChangesAsync(CancellationToken ct);
    Task CommitAsync(Checkpoint checkpoint, CancellationToken ct);
    HealthStatus GetHealth();
}
```

Key changes:
- `ConnectAsync` + `DisconnectAsync` → `StartAsync(config, checkpoint)` + `StopAsync`
- `StreamChangesAsync` → `ChangesAsync` (returns `RawChangeEvent`, not `DataChangeEvent`)
- Add `CommitAsync` — runtime calls this after successful JetStream ack
- Add `GetHealth` — synchronous snapshot for the runtime to publish
- Driver receives its config via `StartAsync`, not via DI/constructor — so tests can inject config

#### Driver loading
Replace current mechanism with MEF / assembly scanning:
```csharp
[Export(typeof(ISourceDriver))]
[ExportMetadata("Name", "postgres")]
[ExportMetadata("Version", "0.1.0")]
public class PostgresDriver : ISourceDriver { ... }
```

#### Impact on existing drivers
Every existing driver in `UniversalConnector.Connectors` must be updated to implement `ISourceDriver` instead of `IDataSourceConnector`, emit `RawChangeEvent` instead of `DataChangeEvent`, and handle checkpoint resume via `StartAsync`.

---

### Configuration

**File to replace:** [`src/UniversalConnector.Host/appsettings.json`](src/UniversalConnector.Host/appsettings.json) and [`src/UniversalConnector.Core/Configuration/ConnectorOptions.cs`](src/UniversalConnector.Core/Configuration/ConnectorOptions.cs)

Replace the flat `appsettings.json` approach with a per-connector YAML config file:

```yaml
connector:
  id: <unique-within-deployment>
  description: "..."
  driver: postgres                   # which ISourceDriver to load
  driver_config:                     # opaque to runtime; passed to driver
    type: postgres
    # ... driver-specific config

  ontology:
    fuseki_url: http://fuseki:3030/common-model
    expected_version: ">=2026.01.0"
    refresh_on_notification: true

  contexts:
    - ctx:AF-Process

  mapping:
    rules_file: mapping/<connector-id>.yaml

  publish:
    subject_template: "cdc.{context}.{site}.{aspect_path}.{event_type}"
    stream: CDC
    serialization: protobuf

  reliability:
    checkpoint_store: jetstream-kv
    checkpoint_bucket: connector-checkpoints
    checkpoint_key: ${CONNECTOR_ID}
    dlq_subject: cdc.dlq.${CONNECTOR_ID}
    max_retries: 3
    retry_backoff_ms: [100, 1000, 10000]

  observability:
    log_level: info
    metrics_port: 9100
    health_interval_seconds: 30
```

Config is validated against JSON Schema at startup — invalid config → refuse to start.  
Secrets use `${ENV_VAR}` references — never inline in config files.

---

### NATS Credentials & RBAC

The connector's NATS user must be constrained to the `connector-runtime` role:

```
publish:   cdc.>, connectors.<id>.health, connectors.<id>.events.>
subscribe: connectors.<id>.command, connectors.<id>.config,
           connectors.<id>.ontology.refresh, ontology.updated.>, ontology.released
```

Startup self-test: before connecting to the source system, verify the connector can publish a test message to its own health subject. Fail fast if the credentials are wrong.

---

### Project / Solution Naming

AssetLink specifies the .NET runtime repo as `common-model-connector-runtime-dotnet` with:

```
src/
  CommonModel.Runtime.Core/        ← domain logic, no IO
    Lifecycle/
    Control/
    Ontology/
    Mapping/
    Publishing/
    Checkpointing/
    ISourceDriver.cs
    Envelope.cs
  CommonModel.Runtime.Host/        ← entry point, DI wiring
tests/
  CommonModel.Runtime.Core.Tests/
  CommonModel.Runtime.Integration.Tests/
```

Current names (`UniversalConnector.Core`, `UniversalConnector.Host`, etc.) will need to be renamed or mapped. The driver projects (`UniversalConnector.Connectors`, `UniversalConnector.Generic`) should eventually move to their own separate repos (`common-model-driver-<name>`), but that can be deferred until the runtime contract is stable.

---

### NuGet Package Changes

| Package | Action | Reason |
|---|---|---|
| `Dapper` | ✅ Already removed | PostgresDataSink removed |
| `Npgsql` (Host) | ✅ Already removed | PostgresDataSink removed |
| `NATS.Net` v2 | Verify version | Must be v2+; v1 (`NATS.Client`) is forbidden by AssetLink |
| `Google.Protobuf` | Add | Protobuf envelope serialization |
| `Grpc.Tools` | Add | Protobuf C# code generation from `.proto` files |
| `CommonModel.CanonicalEvents` | Add (when published) | Generated event POCOs from the canonical-events repo |
| ULID library (e.g. `Ulid`) | Add | ULID generation for `event_id` |
| SPARQL client (e.g. `dotNetRDF`) | Add | Fuseki queries for ontology cache (Layer 2) |
| YAML config parser (e.g. `YamlDotNet`) | Add | Connector YAML config loading |
| JSON Schema validator | Add | Config schema validation at startup |

---

## What is Already Compliant

- ✅ .NET is the correct language for vendor SDKs (e.g., OPC UA, AF)
- ✅ `IAsyncEnumerable` pattern for streaming changes — maps directly to `ChangesAsync`
- ✅ Plugin/driver separation concept exists
- ✅ PostgresDataSink removed — connector is now publish-only
- ✅ `BackgroundService` hosting model — can be built on top of
- ✅ `ConnectorId` and `SourceType` properties on drivers — align to `Name`/`Version`
- ✅ `.gitignore` added, `bin/obj` untracked

---

## Suggested Implementation Order

These are logical phases — not a directive to implement yet.

| Phase | Scope | Dependencies |
|---|---|---|
| **1** | Rename solution/projects to `CommonModel.Runtime.*`; update namespaces | None |
| **2** | Define `RawChangeEvent`, `Checkpoint`, `HealthStatus`, `ISourceDriver` interface | None |
| **3** | Update all existing drivers to implement `ISourceDriver` + emit `RawChangeEvent` | Phase 2 |
| **4** | Add connector YAML config + JSON Schema validation | None |
| **5** | Add Protobuf envelope + ULID; switch NatsPublisher to JetStream-mandatory + ack | Phase 4 |
| **6** | Add checkpoint management (JetStream KV) | Phase 5 |
| **7** | Add DLQ + retry logic | Phase 5 |
| **8** | Add mapping layer (YAML rules, concept resolvers, attribute maps) | Phases 2, 5 |
| **9** | Add ontology cache (Fuseki SPARQL, refresh subscription) | Phase 4 |
| **10** | Add lifecycle state machine + NATS command subscriber | Phase 4 |
| **11** | Add health heartbeat + lifecycle event publishing to JetStream | Phases 5, 10 |
| **12** | Add NATS RBAC credential handling + startup self-test | Phase 4 |

Phases 5–7 and 8–9 can proceed in parallel once their prerequisites are met.

---

## Open Questions

1. **Protobuf schema repo** — `common-model-canonical-events` does not exist yet. Until it does, should the connector define its own interim Protobuf envelope, or use a JSON envelope as a placeholder?
2. **Fuseki** — Is a Fuseki instance available for development? Layer 2 is blocked without it.
3. **NATS credentials** — Are NATS accounts/users already configured with `nsc`, or does that need to be set up first?
4. **Driver separation** — Should existing drivers (`PostgresConnector`, `GenericConnector`) stay in this repo for now, or move to separate repos immediately?
5. **`ctx:` identifiers** — What contexts will this connector populate? The connector YAML `contexts:` field needs to match the AssetLink context registry (e.g., `ctx:AF-Process:<server-id>`).
6. **Mapping rules files** — Who owns/authors the per-connector YAML mapping files? Are templates available for common source types?
