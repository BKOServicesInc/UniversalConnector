## UniversalConnector — Architecture Diagram Explained

The diagram is organized as a **left-to-right data flow** across five vertical columns, each representing a distinct layer of the system. Data always moves from left (sources) to right (outputs), and every component in between has a single, well-defined responsibility.

---

### Column 1 — Data Sources (Dark Green)

This is where all data originates. The system supports seven source types out of the box:

- **PostgreSQL** — the most feature-rich source, supporting both CDC via Write-Ahead Log (WAL) replication and traditional timestamp-based polling.
- **SQL Server** — uses SQL Server Change Tracking or polling.
- **MongoDB** — leverages Change Streams for real-time capture, with a polling fallback.
- **Neo4j** — graph database, supports streaming and polling.
- **Databricks** — targets Delta Lake tables via polling with watermark advancement.
- **HTTP / REST** — polls external APIs or receives inbound webhooks.
- **Custom** — any source can be added by implementing the `IProtocolAdapter` interface, making the system fully extensible without touching the core.

These sources are never accessed directly by the application logic. They are always mediated by the Adapter Layer.

---

### Column 2 — Adapter Layer (Blue)

Each card here is a concrete implementation of `IProtocolAdapter` (which inherits from `BaseProtocolAdapter`). The adapter's only job is to connect to its specific source and emit a stream of raw change records — `RawChangeRecord` objects — regardless of what that source looks like internally.

This abstraction is critical: the rest of the system never knows or cares whether data came from a Postgres WAL stream or an HTTP polling loop. Every adapter speaks the same language. The `BaseProtocolAdapter` base class handles lifecycle (open, close, dispose), and each derived adapter overrides `StreamRawChangesAsync`, returning an `IAsyncEnumerable<RawChangeRecord>`.

---

### Column 3 — Generic Engine (Teal)

This is the brain of the system. It is the largest and most complex layer, and it is entirely descriptor-driven — meaning its behavior is defined by YAML or JSON configuration files, not hard-coded logic.

The pipeline flows top to bottom within this column:

1. **DescriptorLoader** reads `.yaml`, `.yml`, or `.json` files from the configured directory. It also performs environment variable interpolation (e.g., `${DB_PASSWORD}`) so secrets are never stored in files.
2. **DescriptorValidator** checks each loaded descriptor for required fields, valid mode values, and internal consistency. Invalid descriptors are rejected with clear error messages before the system starts.
3. **DescriptorBootstrapService** is an `IHostedService` that runs at startup, orchestrating the load-validate-register pipeline. If `FailOnDescriptorError` is set, a single bad descriptor will abort the whole startup.
4. **ConnectorRegistry** is the runtime lookup table — it maps a `connectorId` string to a live `IConnector` instance.
5. **GenericConnector** is the heart of the engine. It wraps the retry loop, sequence numbering, health reporting, and event normalization. It takes a raw record from an adapter and turns it into a fully enriched `DataChangeEvent`.
6. **Snapshot Cache** is a specialized in-memory `Dictionary` inside `GenericConnector`. Because polling adapters (unlike CDC) only see the current state of a row, the engine stores the last-seen field values per entity/primary key. On the next poll, if a row changed, the engine injects the cached values as `previous_payload`, giving all adapters the same before/after visibility that PostgreSQL CDC gets natively.
7. **AdapterRegistry** maps a `sourceType` string (e.g., `"postgres"`, `"mongodb"`) to the correct adapter implementation. Duplicate registrations are detected and logged as warnings at startup rather than crashing.
8. **MultiSourceGenericFactory** combines the registry and descriptor to instantiate and wire up connectors on demand.

---

### The Central Event — `DataChangeEvent`

Highlighted between the Host and Outputs columns, `DataChangeEvent` is the single canonical sealed record that flows out of the engine. It carries everything downstream needs: `EventId`, `ConnectorId`, `SourceType`, `EntityPath`, `ChangeType` (Insert/Update/Delete/Snapshot), `PrimaryKey` (as a dictionary), `Payload`, `PreviousPayload`, `Metadata`, `SequenceNumber`, `SchemaVersion`, `DetectedAt`, and `SourceTimestamp`. It is the contract between the engine and the outside world.

---

### Column 4 — Host Layer (Purple)

This is the `UniversalConnector.Host` project — the .NET Worker Service executable that ties everything together via dependency injection.

- **ConnectorPipelineService** is the top-level `IHostedService`. It starts all registered connectors, listens to their event streams, and fans out each `DataChangeEvent` to both outputs simultaneously (NATS and the Postgres sink). It also handles graceful shutdown by calling `DisconnectAsync(CancellationToken.None)` in the finally block, ensuring connectors are always closed cleanly even if cancellation has already been signalled.
- **NatsPublisher** wraps NATS.Net 2.x and publishes each event as a serialized JSON message to a structured subject.
- **PostgresDataSink** uses Dapper with `CommandDefinition` (to carry the `CancellationToken` correctly) to `INSERT` each event into the `data_changes` table. Failures are swallowed and logged — a sink failure must never prevent the NATS publish from happening.
- **ServiceCollectionExtensions** centralizes all DI registrations, keeping `Program.cs` clean.
- **appsettings.json** holds the `NatsOptions` and `PostgresSinkOptions` configuration sections, with support for environment variable overrides via the standard .NET configuration system.

---

### Column 5 — Outputs (Amber)

There are two parallel outputs, both written to on every event:

**NATS JetStream** receives every `DataChangeEvent` as a JSON message. The subject follows the pattern:
```
{prefix}.{sourceType}.{connectorId}.{changeType}
```
This makes it trivially easy for any downstream consumer to subscribe to exactly what it cares about — e.g., all inserts from a specific connector, all changes from a specific source type, or everything.

**PostgreSQL (CDCDB)** persists every event permanently to the `data_changes` table. The table stores all fields of `DataChangeEvent`, with `primary_key`, `payload`, `previous_payload`, and `metadata` stored as `JSONB` columns — enabling fast containment queries using GIN indexes (e.g., `primary_key @> '{"assetId":"ASSET-011"}'`). The `ON CONFLICT (event_id) DO NOTHING` clause makes all inserts idempotent, so retries are safe.

---

### How It All Connects

The arrows in the diagram show the data path:

> **Source** → Adapter reads raw rows/events → **RawChangeRecord** → Engine normalizes + enriches → **DataChangeEvent** → Host fans out → **NATS** (real-time stream) + **PostgreSQL** (durable audit log)

The system is designed so that adding a new data source requires only one thing: a new class implementing `IProtocolAdapter`, registered in DI. No other layer needs to change. Similarly, adding a new output sink means implementing `IDataSink` and injecting it — the pipeline doesn't care how many sinks exist.