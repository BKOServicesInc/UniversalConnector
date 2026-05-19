# Connector Descriptors

Every `.yaml` in this directory is a live connector instance. The runtime auto-discovers them at startup via `DescriptorBootstrapService` — adding a new connector is just dropping a new descriptor here and restarting.

A descriptor's `sourceType` field picks which adapter handles it (`postgres`, `sqlserver`, `mongodb`, `neo4j`, `databricks`, `avevapi-af`, `sharepoint`, `sap`, `seeq`). All other fields (poll intervals, watch filters, NATS subjects, reverse-channel commandSubjects, resilience tuning) are descriptor-driven — no code changes needed to onboard a new source instance.

`${VAR}` placeholders are expanded from environment variables. Real secrets stay out of source.

## What's in this directory

| File | Source type | Notes |
|---|---|---|
| `aveva-piaf-server1.yaml` | `avevapi-af` | Primary PI AF server — **bidirectional**. Forward CDC + reverse write-channel. See `docs/AVEVA-CONNECTOR.md`. |
| `avevapi-historian.yaml` | `avevapi` | PI Data Archive (time-series), forward only |
| `databricks-lakehouse.yaml` | `databricks` | Delta Lake watermark polling |
| `mongodb-assets.yaml` | `mongodb` | Mongo Change Streams |
| `neo4j-graph.yaml` | `neo4j` | Polling fallback for graph DB |
| `postgres-orders.yaml` | `postgres` | Postgres logical-decoding CDC |
| `postgres-aveva.yaml` | `postgres` | Postgres staging for AVEVA data |
| `example-postgres.yaml` | `postgres` | Reference template for Postgres setup |
| `sap-s4hana.yaml` | `sap` | SAP S/4 HANA via HTTP REST adapter |
| `seeq-plant.yaml` | `seeq` | Seeq plant data via HTTP REST |
| `sharepoint-docs.yaml` | `sharepoint` | SharePoint documents via HTTP REST |
| `sqlserver-crm.yaml` | `sqlserver` | SQL Server Change Tracking |
| `sqlserver-equipment.yaml` | `sqlserver` | SQL Server Change Tracking |
| `examples/aveva-piaf-server2.yaml` | `avevapi-af` | Demonstrates multi-server onboarding (disabled) |

## Onboarding another instance of the same source type

1. Copy the existing descriptor.
2. Change `driverId` (unique across the deployment), the connection details, and the NATS subject scope.
3. Restart the host. The new descriptor is discovered and started automatically.

No DI changes, no recompilation. The cookie/checkpoint state for the new driver lives in JetStream KV under the new `driverId`.

## Disabling a connector without removing the file

Set `enabled: false` at the top of the YAML. The descriptor is parsed and validated but its driver is not started.
