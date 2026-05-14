# HTTP Contract — ConnectorHub ↔ EventBridge

## Base URL

```
POST  http://eventbridge:8080/api/changes     # .NET connectors
POST  http://eventbridge:8080/api/debezium    # Debezium Server HTTP sink
GET   http://eventbridge:8080/health          # .NET health endpoint
```

---

## Authentication

Every request must include:

```
X-Api-Key: <shared-secret>
Content-Type: application/json
```

EventBridge returns `401 Unauthorized` if the key is missing or invalid.
Keys are configured via environment variable `EventBridge__ApiKeys__0`, `__1`, etc.
ConnectorHub sets `ConnectorOptions.EventBridgeApiKey` → sent as `X-Api-Key`.

---

## 1. `POST /api/changes` — ChangeRequest

Sent by ConnectorHub (and any other .NET / custom connector).

### Request body

```json
{
  "requestId":       "01J8XXXXXXXXXXXXXXXXXXXX",
  "driverId":        "pg-orders",
  "sourceType":      "postgres",
  "context":         "ctx:order-management",
  "entityPath":      "public.orders",
  "changeType":      "Update",
  "detectedAt":      "2026-05-13T10:00:00Z",
  "sourceTimestamp": "2026-05-13T09:59:58Z",
  "primaryKey":      { "id": "42" },
  "fields":          { "status": "shipped",    "updated_at": "2026-05-13T10:00:00Z" },
  "previousFields":  { "status": "processing", "updated_at": "2026-05-13T09:00:00Z" },
  "metadata":        { "slot": "uc_slot", "publication": "uc_pub" }
}
```

### Field reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `requestId` | string (ULID) | yes | Idempotency key; duplicates are discarded |
| `driverId` | string | yes | Unique connector identifier |
| `sourceType` | string | yes | `postgres` \| `sqlserver` \| `neo4j` \| `mongodb` \| ... |
| `context` | string | no | Semantic context IRI e.g. `ctx:order-management` |
| `entityPath` | string | yes | Table / node label e.g. `public.orders` |
| `changeType` | string | yes | `Insert` \| `Update` \| `Delete` \| `Snapshot` |
| `detectedAt` | ISO-8601 | yes | When the connector detected the change |
| `sourceTimestamp` | ISO-8601 | no | When the source DB made the change |
| `primaryKey` | object | yes | Key-value pairs identifying the row |
| `fields` | object | yes | New (or current) field values as strings |
| `previousFields` | object | no | Previous field values (UPDATE / DELETE) |
| `metadata` | object | no | Adapter-specific metadata (slot, version, etc.) |

### Response codes

| Code | Meaning |
|------|---------|
| `202 Accepted` | Request accepted and queued for processing |
| `400 Bad Request` | Missing required fields; body contains `errors[]` |
| `401 Unauthorized` | Missing or invalid `X-Api-Key` |
| `409 Conflict` | Duplicate `requestId` — already processed (idempotent, safe) |
| `500 Internal Server Error` | EventBridge pipeline failure |

### 202 response body

```json
{
  "requestId": "01J8XXXXXXXXXXXXXXXXXXXX",
  "eventId":   "01J8YYYYYYYYYYYYYYYYYYYY",
  "subject":   "cdc.ctx-order-management.public.orders.update",
  "accepted":  true
}
```

---

## 2. `POST /api/debezium` — Debezium CloudEvents

Sent by Debezium Server HTTP sink. EventBridge translates internally to `ChangeRequest`.

### Request body (Debezium format)

```json
{
  "schema": { ... },
  "payload": {
    "before": { "id": 1, "status": "processing", "updated_at": 1715594400000 },
    "after":  { "id": 1, "status": "shipped",    "updated_at": 1715598000000 },
    "source": {
      "version":   "2.7.0",
      "connector": "postgresql",
      "name":      "pg-orders",
      "ts_ms":     1715598000000,
      "snapshot":  "false",
      "db":        "ordersdb",
      "schema":    "public",
      "table":     "orders"
    },
    "op":    "u",
    "ts_ms": 1715598000000
  }
}
```

### `op` → `changeType` mapping

| Debezium `op` | ChangeType |
|---------------|-----------|
| `c` | Insert |
| `u` | Update |
| `d` | Delete |
| `r` | Snapshot |

### Field extraction

| Debezium field | ChangeRequest field |
|----------------|---------------------|
| `payload.source.name` | `driverId` |
| `payload.source.connector` | `sourceType` |
| `payload.source.table` | `entityPath` (with schema prefix) |
| `payload.after` | `fields` |
| `payload.before` | `previousFields` |
| `payload.source.ts_ms` | `sourceTimestamp` |

> The `context` for Debezium-sourced events is configured in EventBridge's
> `debezium-mappings.yaml` — keyed by `driverId` (Debezium connector name).

---

## 3. Idempotency

EventBridge stores processed `requestId` values in a short-lived in-memory cache
(or NATS KV) with a 5-minute TTL. Resubmitting the same `requestId` within
the window returns `409` without re-publishing to NATS.

---

## 4. Error body format

```json
{
  "requestId": "01J8XXXXXXXXXXXXXXXXXXXX",
  "errors": [
    "driverId is required",
    "changeType must be one of: Insert, Update, Delete, Snapshot"
  ]
}
```

---

## 5. Versioning

The API is versioned via URL prefix when breaking changes are needed:
```
/api/v1/changes    (current — v1 prefix optional, defaults to v1)
/api/v2/changes    (future)
```

The `requestId` ULID scheme guarantees global uniqueness across versions.
