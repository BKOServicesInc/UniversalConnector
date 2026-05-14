# Implementation Plan

Build order matters. EventBridge must exist before ConnectorHub can forward to it.
Debezium is fully independent and can be started last.

---

## Phase 1 — EventBridge (Week 1–2)

EventBridge is the foundation. Build it first in isolation, testable without any connector.

### 1.1 Solution scaffold

```
dotnet new sln -n EventBridge
dotnet new classlib -n EventBridge.Core          -f net10.0
dotnet new classlib -n EventBridge.Infrastructure -f net10.0
dotnet new webapi   -n EventBridge.Api            -f net10.0
dotnet new xunit    -n EventBridge.Tests          -f net10.0
```

Add project references:
```
EventBridge.Infrastructure → EventBridge.Core
EventBridge.Api            → EventBridge.Infrastructure + EventBridge.Core
EventBridge.Tests          → EventBridge.Api + EventBridge.Infrastructure
```

### 1.2 Core models & abstractions

Create in `EventBridge.Core`:
- `ChangeRequest.cs` — the HTTP contract DTO (from CONTRACT.md)
- `ChangeEvent.cs` — enriched internal domain model
- `ChangeType.cs` enum
- `Checkpoint.cs`, `OntologyEntry.cs`
- All interfaces: `IChangePipeline`, `INatsPublisher`, `ICheckpointStore`,
  `IOntologyCache`, `IFieldMappingService`, `IDebeziumTranslator`
- Options: `NatsOptions`, `HeartbeatOptions`, `OntologyCacheOptions`, `EventBridgeOptions`

### 1.3 Infrastructure — NATS & protobuf

Port directly from UniversalConnector:
- Copy `envelope.proto` → `EventBridge.Infrastructure/Protos/`
- Port `NatsConnectionFactory`, `NatsPublisher`, `NatsCheckpointStore`
- Port `StartupSelfTestService`
- Port `FusekiOntologyCache`, `OntologyCacheRefreshService`

**Key change:** `NatsPublisher` now accepts a `ChangeEvent` (domain model),
not a `RawChangeEvent`. `EnvelopeBuilder` produces the protobuf `Envelope`.

### 1.4 Field mapping service

- Port `FieldMapper` from ConnectorHub → rename `FieldMappingService`
- Add `MappingRuleLoader` — loads `config/mapping-rules.yaml`
- Rules keyed by `driverId`; `"*"` rules merged for all drivers
- Input: `IReadOnlyDictionary<string, string>` (ChangeRequest already has string values)

### 1.5 Debezium translator

- Implement `DebeziumTranslator.Translate(JsonElement) → ChangeRequest`
- Add `config/debezium-mappings.yaml` loader
- Map `op` → `ChangeType`, `before/after` → `previousFields/fields`, etc.

### 1.6 Pipeline

- Implement `DefaultChangePipeline`:
  idempotency → field mapping → change type resolution → ontology → envelope → publish → checkpoint
- Implement in-memory `IdempotencyStore`

### 1.7 API layer

- `ApiKeyMiddleware` — validate `X-Api-Key` header
- `ChangeController` — `POST /api/changes` → call pipeline → `202 Accepted`
- `DebeziumController` — `POST /api/debezium` → translate → call pipeline → `202`
- `NatsHealthCheck`
- Wire up in `Program.cs` / `ServiceCollectionExtensions`

### 1.8 Tests (Phase 1)

Write tests for:
- `DefaultChangePipelineTests`
- `DebeziumTranslatorTests` (all op types, null before, missing fields)
- `FieldMappingServiceTests`
- `ApiKeyMiddlewareTests`
- `ChangeControllerTests` (202, 400, 401, 409, 500)
- `DebeziumControllerTests`
- `NatsPublisherTests` (circuit breaker, DLQ)

### 1.9 Docker Compose (EventBridge)

- `Dockerfile` (multi-stage, .NET 10 runtime)
- `docker-compose.yml` with `eventbridge` + `nats` services
- `docker/nats/nats.conf`
- `.env.example`

### Verification — Phase 1

```bash
# Start EventBridge + NATS
docker compose up -d

# Post a test ChangeRequest manually
curl -X POST http://localhost:5100/api/changes \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: test-key" \
  -d '{
    "requestId": "01JTEST00000000000000001",
    "driverId": "test",
    "sourceType": "postgres",
    "context": "ctx:test",
    "entityPath": "public.orders",
    "changeType": "Insert",
    "detectedAt": "2026-05-13T10:00:00Z",
    "primaryKey": {"id": "1"},
    "fields": {"status": "pending", "total_amount": "99.95"},
    "metadata": {}
  }'
# Expected: 202 Accepted

# Verify message arrived in NATS
nats stream view CDC --count 1 -s nats://localhost:4222
```

---

## Phase 2 — ConnectorHub (Week 2–3)

Refactor UniversalConnector into the thin ConnectorHub.
Start from the existing codebase — do not rewrite from scratch.

### 2.1 Copy / rename solution

```
git clone <assetlink-repo> ConnectorHub
```

Rename projects:
```
CommonModel.Runtime.Core               → ConnectorHub.Core
CommonModel.Runtime.Drivers.Generic   → ConnectorHub.Drivers
CommonModel.Runtime.Host              → ConnectorHub.Host
CommonModel.Runtime.Tests             → ConnectorHub.Tests
```

Delete project entirely:
```
CommonModel.Runtime.Infrastructure    ← DELETE
```

### 2.2 Remove infrastructure dependencies

In `ConnectorHub.Host.csproj`:
- Remove `<ProjectReference>` to Infrastructure
- Remove NuGet: `NATS.Net`, `Google.Protobuf`, `Grpc.Tools`

In `ConnectorHub.Core`:
- Delete `IDataSink.cs` (if still present)

### 2.3 Add `ChangeRequest` model

- Add `ConnectorHub.Core/Models/ChangeRequest.cs` (copy from CONTRACT.md definition)
- Add `IHttpChangeForwarder` interface to `ConnectorHub.Core/Abstractions/`

### 2.4 Update `ConnectorDescriptor`

- Remove `NatsOptions` inner class and `Nats` property
- Remove `FieldMapping` list
- Add `EventBridgeOptions` inner class with `Url`, `ApiKey`, `TimeoutSeconds`, `AdditionalMetadata`
- Add `EventBridge` property to `ConnectorDescriptor`

### 2.5 Simplify `GenericConnector`

- Remove `FieldMapper` dependency
- Remove snapshot cache (`_snapshots`) — ChangeType.Snapshot resolution moves to EventBridge
- Remove `SubjectTemplateResolver` usage
- Build `ChangeRequest` from `RawChangeRecord`
- Call `IHttpChangeForwarder.ForwardAsync(request, ct)` instead of `IEventPipeline.ProcessAsync`
- Keep: `BaseConnector`, retry loop, `ConnectorRegistry`, `AdapterRegistry`

### 2.6 Implement `HttpChangeForwarder`

New file: `ConnectorHub.Host/Services/HttpChangeForwarder.cs`
- Typed `HttpClient` named `"eventbridge"`
- Serialize `ChangeRequest` → JSON (`System.Text.Json`)
- POST to `{descriptor.EventBridge.Url}/api/changes`
- Header `X-Api-Key`
- On 202 → OK; 409 → log + skip; 4xx → throw; 5xx → throw (triggers retry)
- Debug log the JSON payload

### 2.7 Update `ConnectorPipelineService`

- Remove `IEventPipeline` injection
- The `RunDriverLoopAsync` now calls `_driver.StreamChangesAsync(ct)` and for each
  event increments the health counter (no publish needed — that's EventBridge's job)
- Update health endpoint to show events forwarded vs published

### 2.8 Update YAML descriptors

For each YAML in `connectors/`:
- Remove `fieldMapping:` section (move rules to EventBridge `mapping-rules.yaml`)
- Remove `nats:` section
- Add `eventBridge:` section

### 2.9 Update `appsettings.json`

Remove: all `Nats`, `OntologyCache`, `Heartbeat` sections
Add: `EventBridge.DefaultUrl`, `EventBridge.DefaultApiKey`

### 2.10 Tests (Phase 2)

Update/add:
- `HttpChangeForwarderTests`
- `GenericConnectorTests` — verify `ChangeRequest` built correctly
- `DescriptorLoaderTests` — `eventBridge` section, no `nats` section
- Remove `NatsPublisherTests`, `CheckpointStoreTests`, `StartupSelfTestServiceTests`

### Verification — Phase 2

```bash
# Start infra + EventBridge
cd EventBridge && docker compose up -d
# Start ConnectorHub
cd ConnectorHub && dotnet run --project src/ConnectorHub.Host

# Run a Postgres test INSERT (port 5433)
psql -h localhost -p 5433 -U connector -d aveva_db \
  -c "INSERT INTO public.assets (asset_id, name, status, created_at, updated_at)
      VALUES ('ASSET-TEST', 'Test Pump', 'operational', NOW(), NOW())"

# Verify it arrives in NATS via Python consumer
cd C:\Repos\NTAS_Consumer && python consumer.py
```

---

## Phase 3 — DebeziumConnector (Week 3)

No code required. Pure configuration and Docker.

### 3.1 Create repo structure

```
mkdir DebeziumConnector
mkdir DebeziumConnector/config/postgres
mkdir DebeziumConnector/config/sqlserver
mkdir DebeziumConnector/config/mongodb
```

### 3.2 Write config files

Copy the `application.properties` files from `DEBEZIUM-SPEC.md`:
- `config/postgres/application.properties`
- `config/sqlserver/application.properties`
- `config/mongodb/application.properties`

Adjust `table.include.list` values to match your actual tables.

### 3.3 Write Docker Compose

Copy from `DEBEZIUM-SPEC.md`, verify network names match your setup.

### 3.4 Add context mapping to EventBridge

Edit `EventBridge/config/debezium-mappings.yaml`:
```yaml
"pg-orders-debezium":
  context: ctx:order-management
  sourceType: postgres
```

### 3.5 Verify prerequisites

```sql
-- On the Postgres instance Debezium will connect to:
SELECT slot_name, plugin, active FROM pg_replication_slots;
-- Ensure uc_slot (ConnectorHub) and debezium_slot (Debezium) are separate

ALTER TABLE public.orders      REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;
```

### Verification — Phase 3

```bash
# Start Debezium
cd DebeziumConnector && docker compose up -d debezium-postgres

# Watch Debezium logs
docker logs -f debezium-postgres

# Insert a row in Postgres — should arrive via /api/debezium path
psql -h localhost -p 5433 -U connector -d ordersdb \
  -c "INSERT INTO public.orders (customer_id, status, total_amount)
      VALUES (9999, 'pending', 1.00)"

# Verify in Python consumer — source should show driverId=pg-orders-debezium
python consumer.py
```

---

## Phase 4 — Integration Hardening (Week 4)

### 4.1 End-to-end test matrix

| Scenario | Connector | Source | Expected |
|----------|-----------|--------|----------|
| Insert | ConnectorHub | Postgres CDC | Insert event in NATS |
| Update + prev values | ConnectorHub | Postgres CDC | Update + previousFields |
| Delete | ConnectorHub | Postgres CDC | Delete with PK |
| Insert | ConnectorHub | Neo4j poll | Insert event (Snapshot resolved) |
| Delete | ConnectorHub | Neo4j poll | Delete (snapshot diff) |
| Insert | Debezium | Postgres WAL | Insert via /api/debezium |
| Update | Debezium | SQL Server CT | Update via /api/debezium |
| Duplicate requestId | ConnectorHub | any | 409, no duplicate in NATS |
| EventBridge down | ConnectorHub | any | Retry + backoff, resumes |
| NATS down | EventBridge | any | DLQ, circuit breaker opens |

### 4.2 Idempotency stress test

Send the same `ChangeRequest` 10 times concurrently to `/api/changes`.
Verify exactly 1 event appears in NATS (only first accepted, rest return 409).

### 4.3 NATS replay test

Stop Python consumer. Insert 50 rows. Start consumer with `DeliverPolicy.ALL`.
Verify all 50 events delivered.

---

## Repository Summary

| Repo | Branch | Status |
|------|--------|--------|
| `EventBridge` | `main` | Build first |
| `ConnectorHub` | `main` | Refactor from AssetLink/UniversalConnector |
| `DebeziumConnector` | `main` | Config only, no build |

Shared infra (NATS, Fuseki) can live in a fourth `infra` repo or in `EventBridge/docker-compose.infra.yml`.
