# Session Starter Prompt

---

## Context

You are implementing a new multi-repository .NET 10 solution called the
**Decoupled Change Detection Platform**. This is a refactor of an existing project
called UniversalConnector (also referred to as AssetLink).

The existing codebase lives at `C:\Repos\UniversalConnector`.
Its full specification is at `C:\Repos\UniversalConnector\SPEC.md`.

The new solution decouples change detection from event publishing:

- **ConnectorHub** — thin .NET Worker Service (refactored from UniversalConnector).
  Detects changes, POSTs them as JSON to EventBridge. No NATS, no protobuf.

- **EventBridge** — new ASP.NET Core Web API + Worker.
  Receives HTTP posts, applies field mapping and ontology, serializes to protobuf,
  publishes to NATS JetStream.

- **DebeziumConnector** — config-only project.
  Debezium Server (Java) monitors sources via CDC and POSTs to EventBridge using
  the Debezium HTTP sink. No custom code required.

---

## Reference files (read these before starting any implementation)

All files are at `C:\Repos\UniversalConnector\new-project-plan\`:

| File | What it contains |
|------|-----------------|
| `ARCHITECTURE.md` | High-level design, data flow diagrams, deployment topology |
| `CONTRACT.md` | HTTP API contract between connectors and EventBridge (ChangeRequest JSON schema) |
| `EVENTBRIDGE-SPEC.md` | Full specification of the EventBridge microservice |
| `CONNECTOR-HUB-SPEC.md` | Full specification of the simplified ConnectorHub |
| `DEBEZIUM-SPEC.md` | Debezium Server configuration and Docker Compose |
| `IMPLEMENTATION-PLAN.md` | Ordered 4-phase build plan with verification steps |

The existing code to learn from and reuse:
- `C:\Repos\UniversalConnector\src\` — all existing adapters, engine, infrastructure
- `C:\Repos\UniversalConnector\SPEC.md` — complete spec of what exists today

---

## Key design decisions already made (do not question these)

1. Connectors POST JSON (`ChangeRequest`) over HTTP to EventBridge — no NATS in connectors
2. EventBridge is the only component that knows about NATS and protobuf
3. All field mapping rules move from connector descriptors to EventBridge `mapping-rules.yaml`
4. `ChangeType.Snapshot` resolution (Insert vs Update) moves to EventBridge (uses checkpoint store)
5. Debezium uses its built-in HTTP sink — no custom Java code
6. Authentication between connector and EventBridge: `X-Api-Key` header
7. Idempotency: `requestId` (ULID) deduplicated at EventBridge with 5-min TTL
8. `REPLICA IDENTITY FULL` still auto-applied by PostgresAdapter at startup
9. Neo4j delete detection via snapshot diff — same as today, stays in the adapter
10. The protobuf `Envelope` wire format is unchanged — consumers see the same messages

---

## Experience from the previous implementation (important lessons)

### Postgres / Docker
- The shared Docker Postgres instance has both `pg-orders` (CDC) and `pg-aveva` (polling).
  It has a logical replication slot (`uc_slot`). **Never set `wal_level` below `logical`**
  or the container will refuse to start.
- `REPLICA IDENTITY FULL` must be set on each watched table for `PreviousFields` to work.
- The Postgres container runs on host port **5433** (not default 5432).

### SQL Server
- `CHANGETABLE` LEFT JOIN returns `NULL` for all `t.*` columns on DELETE.
  Always select PK columns from the `ct.*` side explicitly so DELETE events carry an identity.

### Neo4j
- All events from a polling adapter come as `ChangeType.Snapshot`.
  Resolving to Insert/Update requires the snapshot cache (or EventBridge's checkpoint store).
- Delete detection requires full scan + PK diff every cycle (`_knownKeys`, `_lastKnownFields`).

### NATS
- JetStream: persistent, ACK'd, replayable. Use for change events.
- Core NATS: fire-and-forget. Use only for heartbeats.
- Heartbeat subjects (`cdc.health.*`) are JSON, not protobuf — the Python consumer
  must route these separately.
- To inspect stored messages: `nats stream view CDC --count 10 -s nats://localhost:4222`
  or browse `http://localhost:8222/jsz?streams=1`

### .NET / Build
- The correct JetStream stream config type is `StreamConfig` from `NATS.Client.JetStream.Models`.
  Property is `NumReplicas` (not `Replicas`).
- Protobuf JSON logging: use `Google.Protobuf.JsonFormatter` with `Settings.Default`.
- Always stop the running connector process before rebuilding (DLL lock).

### Python consumer (`C:\Repos\NTAS_Consumer\consumer.py`)
- Protobuf consumer for `cdc.>` subjects.
- Heartbeats on `cdc.health.*` are JSON — route them separately to `print_heartbeat()`.
- Shows per-field diff for UPDATE events (previous vs new values).
- To replay all stored messages: `DeliverPolicy.ALL` in consumer config.

---

## Build order

Follow the phases in `IMPLEMENTATION-PLAN.md` exactly:

1. **Phase 1: EventBridge** — build and test in isolation first
2. **Phase 2: ConnectorHub** — refactor from UniversalConnector, point at EventBridge
3. **Phase 3: DebeziumConnector** — write config files, start Docker
4. **Phase 4: Integration** — end-to-end test matrix

Do not skip Phase 1. ConnectorHub cannot be tested without EventBridge running.

---

## How to start

1. Read all files in `C:\Repos\UniversalConnector\new-project-plan\` (all 6 spec files)
2. Read `C:\Repos\UniversalConnector\SPEC.md` (the existing system spec)
3. Skim the existing source at `C:\Repos\UniversalConnector\src\` to understand the code patterns
4. Create `C:\Repos\EventBridge\` and scaffold the solution per Phase 1 of the implementation plan
5. Work through the phases in order, asking for clarification only if a spec is contradictory

Do not ask permission to proceed between steps. Work through the plan autonomously.
Report what you have built at the end of each Phase with a short summary.

---

## Tech stack reminder

| Technology | Version |
|-----------|---------|
| .NET | 10.0 |
| ASP.NET Core | 10.0 |
| NATS.Net | 2.7.3 |
| Google.Protobuf | 3.27.1 |
| Npgsql | 10.0.2 |
| Microsoft.Data.SqlClient | 7.0.1 |
| Neo4j.Driver | 6.0.0 |
| MongoDB.Driver | 3.x |
| YamlDotNet | 17.1.0 |
| xUnit | 2.9.3 |
| FluentAssertions | 6.x |
| NSubstitute | 5.x |
| System.Text.Json | inbox (.NET 10) |
| Debezium Server | 2.7 |
