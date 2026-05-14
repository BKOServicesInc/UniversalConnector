# Architecture — Decoupled Change Detection Platform

## Overview

The platform is split into three independent repositories.
Each has a single, well-defined responsibility.

```
┌───────────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                                   │
│  Postgres · SQL Server · Neo4j · MongoDB · Any JDBC/Debezium source       │
└──────────┬──────────────────────────────────────────────────────┬─────────┘
           │  WAL / Change Tracking / Polling                     │  Debezium CDC
           ▼                                                       ▼
┌────────────────────────┐                        ┌───────────────────────────┐
│   ConnectorHub         │                        │   DebeziumConnector       │
│   (.NET Worker)        │                        │   (Debezium Server / Java)│
│                        │                        │                           │
│  Thin adapters:        │                        │  Monitors sources via     │
│  Postgres, SQL Server, │                        │  Kafka-compatible CDC.    │
│  Neo4j, MongoDB, HTTP  │                        │  Uses HTTP sink to POST   │
│                        │                        │  CloudEvents to           │
│  Only job:             │                        │  EventBridge.             │
│  detect change →       │                        │                           │
│  POST ChangeRequest    │                        │                           │
└──────────┬─────────────┘                        └──────────┬────────────────┘
           │  HTTP POST /api/changes                         │  HTTP POST /api/debezium
           │  (ChangeRequest JSON)                           │  (Debezium CloudEvents JSON)
           └───────────────────────┬─────────────────────────┘
                                   ▼
              ┌────────────────────────────────────────────┐
              │   EventBridge  (ASP.NET Core Web API)      │
              │                                            │
              │  1. Receive ChangeRequest (any source)     │
              │  2. Apply field mapping rules              │
              │  3. Apply ontology enrichment (optional)   │
              │  4. Cast to protobuf Envelope              │
              │  5. Publish to NATS JetStream              │
              │  6. Save checkpoint                        │
              │  7. DLQ on failure                        │
              └──────────────────┬─────────────────────────┘
                                 │
                                 ▼
              ┌────────────────────────────────────────────┐
              │   NATS JetStream                           │
              │   Stream: CDC   subjects: cdc.>            │
              │   DLQ:    cdc.dlq.>                        │
              │   Health: cdc.health.<driverId>            │
              │   KV:     cm-checkpoints                   │
              └────────────────────────────────────────────┘
```

---

## Repositories

| Repo | Type | Language | Purpose |
|------|------|----------|---------|
| `ConnectorHub` | Worker Service | .NET 10 | Thin change detectors; one per source family |
| `EventBridge` | Web API + Worker | .NET 10 | Central publisher: ontology, protobuf, NATS |
| `DebeziumConnector` | Config + Docker | Java (Debezium Server) | JDBC/Debezium CDC → EventBridge HTTP |

---

## Design Principles

1. **Connectors are dumb.** They detect a change and POST it. No NATS dependency.
   No protobuf. No ontology. No field mapping. Easily replaced or rewritten in any language.

2. **EventBridge is the brain.** All enrichment, transformation, protocol work lives here.
   Any source that can POST JSON can feed EventBridge — not just .NET connectors.

3. **Contract is HTTP + JSON.** The `ChangeRequest` payload is the only contract between
   connectors and EventBridge. It is versioned and documented in `CONTRACT.md`.

4. **Debezium is a first-class citizen.** Debezium Server's HTTP sink POSTs to a dedicated
   `/api/debezium` endpoint. EventBridge translates the Debezium CloudEvents format
   to `ChangeRequest` internally before running it through the same pipeline.

5. **NATS and protobuf are EventBridge internals.** No connector needs to know about them.

6. **Descriptor YAMLs move.** ConnectorHub still uses YAML descriptors for connection
   details and entity configuration. EventBridge uses its own YAML/JSON for field
   mapping and ontology rules — the two configs are separate.

---

## Key Data Flows

### .NET connector → EventBridge → NATS

```
[PostgresAdapter polls / streams WAL]
        │
        ▼
[ConnectorPipelineService]
  builds ChangeRequest JSON
        │
        ▼ HTTP POST /api/changes
[EventBridge ChangeController]
  → FieldMappingService.Apply()
  → OntologyEnrichmentService.Enrich()
  → EnvelopeBuilder.Build()  →  protobuf Envelope
  → NatsPublisher.PublishAsync()
  → CheckpointStore.SaveAsync()
```

### Debezium → EventBridge → NATS

```
[Debezium Server monitors Postgres/MySQL/SQL Server]
        │
        ▼ HTTP POST /api/debezium  (CloudEvents JSON)
[EventBridge DebeziumController]
  → DebeziumTranslator.ToChangeRequest()
  → (same pipeline as above)
```

---

## Technology Stack

| Concern | Technology |
|---------|-----------|
| .NET runtime | .NET 10 |
| Web API | ASP.NET Core minimal API |
| Messaging | NATS.Net 2.7+ (JetStream) |
| Serialization | Google.Protobuf 3.27+ |
| PostgreSQL driver | Npgsql 10+ (logical replication) |
| SQL Server driver | Microsoft.Data.SqlClient 7+ |
| Neo4j driver | Neo4j.Driver 6+ |
| MongoDB driver | MongoDB.Driver 3+ |
| CDC (Java path) | Debezium Server 2.7+ |
| Ontology | Apache Jena Fuseki (optional) |
| Config | YAML descriptors + appsettings.json |
| Containers | Docker Compose |
| Testing | xUnit, FluentAssertions, NSubstitute |

---

## Inter-service Authentication

ConnectorHub authenticates to EventBridge using a shared API key:

```
Header:  X-Api-Key: <connector-api-key>
```

EventBridge validates the key on every request. Keys are set via environment variables.
Debezium Server sends the same header via its HTTP sink `headers` configuration.

---

## Deployment Topology (Docker Compose)

```
┌── docker-compose.yml (shared / infra) ──────────────────────────┐
│  nats          4222, 8222                                        │
│  fuseki        3030   (profile: fuseki)                         │
│  nats-surveyor 7777   (profile: monitoring)                     │
└──────────────────────────────────────────────────────────────────┘

┌── ConnectorHub/docker-compose.yml ──────────────────────────────┐
│  postgres      5433:5432   (aveva_db + orders)                  │
│  neo4j         7474, 7687                                        │
│  connector-hub 8080   (health only)                             │
└──────────────────────────────────────────────────────────────────┘

┌── EventBridge/docker-compose.yml ───────────────────────────────┐
│  eventbridge   5100:8080   POST /api/changes, /api/debezium     │
└──────────────────────────────────────────────────────────────────┘

┌── DebeziumConnector/docker-compose.yml ─────────────────────────┐
│  debezium-server  (no exposed port; POSTs to eventbridge:8080)  │
└──────────────────────────────────────────────────────────────────┘
```
