# DebeziumConnector — Specification

## Purpose

DebeziumConnector monitors data sources using Debezium Server and forwards change events
to EventBridge via HTTP. It requires no custom code — it is purely configuration.

Debezium Server is a Java application that runs Debezium connectors and supports multiple
sink types. This project uses the **HTTP sink** to POST events to EventBridge.

---

## How Debezium Server Works

```
┌─────────────────────────────────────────────────────┐
│  Debezium Server (Java)                              │
│                                                      │
│  Source connector (e.g. PostgreSQL):                 │
│    Reads WAL via logical replication                 │
│    Produces Debezium change events                   │
│                                                      │
│  HTTP Sink:                                          │
│    POSTs events to EventBridge /api/debezium         │
└─────────────────────────────────────────────────────┘
```

Debezium Server supports these sources out of the box:
- PostgreSQL (WAL / pgoutput)
- MySQL (binlog)
- SQL Server (Change Data Capture)
- MongoDB (change streams)
- Oracle
- Db2
- Cassandra
- Vitess

For each source a separate `application.properties` file is provided.

---

## Repository Structure

```
DebeziumConnector/
├── config/
│   ├── postgres/
│   │   └── application.properties        ← Debezium Server config for Postgres
│   ├── sqlserver/
│   │   └── application.properties
│   └── mongodb/
│       └── application.properties
├── docker/
│   └── wait-for-eventbridge.sh           ← healthcheck helper
└── docker-compose.yml
```

---

## Debezium Server — PostgreSQL config

`config/postgres/application.properties`

```properties
# ── Debezium Server ────────────────────────────────────────────────────────
debezium.format.value=json
debezium.format.key=json
debezium.sink.type=http

# ── HTTP Sink ─────────────────────────────────────────────────────────────
debezium.sink.http.url=http://eventbridge:8080/api/debezium
debezium.sink.http.headers.Content-Type=application/json
debezium.sink.http.headers.X-Api-Key=${EVENTBRIDGE_API_KEY}
debezium.sink.http.timeout.ms=10000
debezium.sink.http.retries.max=5

# ── Source connector ──────────────────────────────────────────────────────
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.name=pg-orders-debezium
debezium.source.database.hostname=${POSTGRES_HOST:-postgres}
debezium.source.database.port=${POSTGRES_PORT:-5432}
debezium.source.database.user=${POSTGRES_USER:-connector}
debezium.source.database.password=${POSTGRES_PASSWORD:-12345}
debezium.source.database.dbname=${POSTGRES_DB:-ordersdb}
debezium.source.database.server.name=pg-orders-debezium
debezium.source.plugin.name=pgoutput
debezium.source.slot.name=debezium_slot
debezium.source.publication.name=dbz_pub

# Watch specific tables (comment out to watch all)
debezium.source.table.include.list=public.orders,public.order_items

# Include the before-image of each row on UPDATE/DELETE
# Requires: ALTER TABLE <t> REPLICA IDENTITY FULL  (applied by EventBridge or manually)
debezium.source.tombstones.on.delete=false

# ── Offset storage (where Debezium persists its WAL position) ─────────────
debezium.source.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
debezium.source.offset.storage.file.filename=/debezium/data/offsets.dat
debezium.source.offset.flush.interval.ms=5000

# ── Schema history ────────────────────────────────────────────────────────
debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=/debezium/data/schema-history.dat
```

---

## Debezium Server — SQL Server config

`config/sqlserver/application.properties`

```properties
debezium.format.value=json
debezium.format.key=json
debezium.sink.type=http

debezium.sink.http.url=http://eventbridge:8080/api/debezium
debezium.sink.http.headers.Content-Type=application/json
debezium.sink.http.headers.X-Api-Key=${EVENTBRIDGE_API_KEY}
debezium.sink.http.timeout.ms=10000
debezium.sink.http.retries.max=5

debezium.source.connector.class=io.debezium.connector.sqlserver.SqlServerConnector
debezium.source.name=sqlserver-crm-debezium
debezium.source.database.hostname=${SQLSERVER_HOST:-sqlserver}
debezium.source.database.port=${SQLSERVER_PORT:-1433}
debezium.source.database.user=${SQLSERVER_USER:-connector}
debezium.source.database.password=${SQLSERVER_PASSWORD}
debezium.source.database.names=${SQLSERVER_DB:-crmdb}
debezium.source.database.encrypt=false

debezium.source.table.include.list=dbo.Customers,dbo.Contacts

debezium.source.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
debezium.source.offset.storage.file.filename=/debezium/data/offsets.dat
debezium.source.offset.flush.interval.ms=5000

debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=/debezium/data/schema-history.dat
```

---

## Debezium Server — MongoDB config

`config/mongodb/application.properties`

```properties
debezium.format.value=json
debezium.format.key=json
debezium.sink.type=http

debezium.sink.http.url=http://eventbridge:8080/api/debezium
debezium.sink.http.headers.Content-Type=application/json
debezium.sink.http.headers.X-Api-Key=${EVENTBRIDGE_API_KEY}

debezium.source.connector.class=io.debezium.connector.mongodb.MongoDbConnector
debezium.source.name=mongodb-assets-debezium
debezium.source.mongodb.connection.string=mongodb://${MONGO_HOST:-mongo}:27017
debezium.source.collection.include.list=assetsdb.assets,assetsdb.locations

debezium.source.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
debezium.source.offset.storage.file.filename=/debezium/data/offsets.dat
```

---

## Docker Compose

```yaml
# DebeziumConnector/docker-compose.yml

services:

  debezium-postgres:
    image: quay.io/debezium/server:2.7
    container_name: debezium-postgres
    restart: unless-stopped
    environment:
      EVENTBRIDGE_API_KEY: ${EVENTBRIDGE_API_KEY}
      POSTGRES_HOST:       ${POSTGRES_HOST:-postgres}
      POSTGRES_PORT:       ${POSTGRES_PORT:-5432}
      POSTGRES_USER:       ${POSTGRES_USER:-connector}
      POSTGRES_PASSWORD:   ${POSTGRES_PASSWORD:-12345}
      POSTGRES_DB:         ${POSTGRES_DB:-ordersdb}
    volumes:
      - ./config/postgres:/debezium/config:ro
      - debezium-postgres-data:/debezium/data
    networks:
      - connector-net
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8080/q/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 10
      start_period: 30s

  # Uncomment to run SQL Server CDC via Debezium:
  # debezium-sqlserver:
  #   image: quay.io/debezium/server:2.7
  #   container_name: debezium-sqlserver
  #   volumes:
  #     - ./config/sqlserver:/debezium/config:ro
  #     - debezium-sqlserver-data:/debezium/data
  #   environment:
  #     EVENTBRIDGE_API_KEY:  ${EVENTBRIDGE_API_KEY}
  #     SQLSERVER_HOST:       ${SQLSERVER_HOST:-sqlserver}
  #     SQLSERVER_PASSWORD:   ${SQLSERVER_PASSWORD}
  #     SQLSERVER_DB:         ${SQLSERVER_DB:-crmdb}
  #   networks:
  #     - connector-net

networks:
  connector-net:
    external: true    # shared with EventBridge and ConnectorHub compose networks

volumes:
  debezium-postgres-data:
  # debezium-sqlserver-data:
```

---

## PostgreSQL prerequisites for Debezium

Debezium requires the same PostgreSQL configuration as ConnectorHub CDC:

```sql
-- postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Run once per watched table (or set globally):
ALTER TABLE public.orders      REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;

-- Debezium creates its own slot and publication automatically.
-- Slot name: debezium_slot   Publication name: dbz_pub
```

> **Note:** Do not share Debezium's slot (`debezium_slot`) with ConnectorHub's
> slot (`uc_slot`). Each consumer must have its own replication slot.

---

## Offset Persistence

Debezium stores its WAL offset in `/debezium/data/offsets.dat` inside the container.
This is volume-mounted so the position survives container restarts.

For production, switch to Kafka offset storage or a database-backed store:

```properties
debezium.source.offset.storage=io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore
debezium.source.offset.storage.jdbc.url=jdbc:postgresql://postgres:5432/offsetsdb
debezium.source.offset.storage.jdbc.user=connector
debezium.source.offset.storage.jdbc.password=12345
```

---

## Comparison: ConnectorHub vs DebeziumConnector

| Aspect | ConnectorHub (.NET) | DebeziumConnector (Java) |
|--------|---------------------|--------------------------|
| Language | .NET 10 | Java (Debezium Server) |
| Custom code | Yes — adapters | None — pure config |
| Supported sources | Postgres, SQL Server, Neo4j, MongoDB, HTTP, ... | Postgres, MySQL, SQL Server, MongoDB, Oracle, ... |
| CDC mechanism | Npgsql logical replication / CT | Debezium (WAL/binlog/CT) |
| Before-image | REPLICA IDENTITY FULL | REPLICA IDENTITY FULL |
| Offset storage | NATS KV checkpoint | File / JDBC |
| Deployment | Docker / K8s | Docker / K8s |
| EventBridge endpoint | `/api/changes` | `/api/debezium` |

Both send events to the same EventBridge pipeline — from NATS's perspective
there is no difference between a ConnectorHub event and a Debezium event.
