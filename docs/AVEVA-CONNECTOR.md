# AVEVA PI Connector — File-by-File Guide

This document maps the bidirectional AVEVA PI System Explorer (PI Asset Framework) integration to the exact files that implement it. The connector is bidirectional: **forward** propagates AF check-ins to NATS as CDC events, **reverse** consumes NATS commands and applies them to AF via the OSIsoft AF SDK.

Everything described here lives in this repo — no separate runtime, no separate service.

---

## At a Glance

```
                     ┌──────────────────────────────────────┐
                     │   PI System Explorer (PI AF Server)  │
                     └─────┬─────────────────────────▲──────┘
                  forward  │                         │  reverse
        FindChangedItems   │                         │  ApplyAsync
                           ▼                         │
                  ┌────────────────────┐    ┌────────┴────────┐
                  │ AvevaPiAfAdapter   │    │ AvevaPiAfAdapter│
                  │ (poll / CDC out)   │    │ (CRUD in)       │
                  └────────┬───────────┘    └────────▲────────┘
                           │                         │
                  ┌────────▼───────────┐    ┌────────┴────────┐
                  │ GenericConnector   │    │ ReverseChannel  │
                  │ + EventPipeline    │    │ Service         │
                  └────────┬───────────┘    └────────▲────────┘
                           │                         │
                       cdc.>                       cmd.>
                           ▼                         │
                  ┌──────────────────────────────────┴───────┐
                  │              NATS (JetStream)            │
                  └──────────────────────────────────────────┘
```

---

## The Configuration — What Activates the Connector

### `connectors/aveva-piaf-server1.yaml`

The descriptor. This single YAML file is what makes one PI AF server live in the runtime. Onboarding a second AF server is a pure copy-and-rename of this file — no code or DI changes.

Key fields:

| Section | Purpose |
|---|---|
| `driverId` | Unique per descriptor — used as the cookie checkpoint key, log prefix, command-target id |
| `sourceType: avevapi-af` | Selects the adapter (`AvevaPiAfAdapter` registers under this name) |
| `connection.afSystemName` / `afDatabase` | Identifies which PI AF server and database to attach to |
| `changeDetection.pollIntervalSeconds` | How often the forward path calls `FindChangedItems` |
| `watch.entities` | Filter — `elementTemplate`, `element`, or both |
| `nats.subjectTemplate` | How outbound CDC events are addressed |
| `nats.commandSubjects` | What inbound write subjects the reverse channel subscribes to |
| `nats.commandConsumer` | JetStream consumer name for reverse-channel durability |

### `connectors/examples/aveva-piaf-server2.yaml`

The "second server" example. Demonstrates the config-only onboarding story — just `driverId`, `afSystemName`, `afDatabase`, and command-subject scope change. Disabled by default (`enabled: false`).

---

## The Adapter — Where Both Directions Live

### `src/CommonModel.Runtime.Drivers.Generic/Adapters/AvevaPiAfAdapter.cs`

Single class, implements both `IProtocolAdapter` (forward) and `IWritableProtocolAdapter` (reverse). Registered as a singleton in `GenericConnectorExtensions.AddGenericConnector(...)`. One instance services all `avevapi-af` descriptors; the PISystem connection is cached per `afSystemName` so multiple descriptors targeting the same server share one underlying connection.

#### Forward path: `StreamRawChangesAsync` → `DrainChanges` → `ToRecord`

1. **`OpenCoreAsync`** — connects to the PISystem (Windows-integrated or explicit creds from the descriptor), then calls `EnsureCookieAsync`.
2. **`EnsureCookieAsync`** — per-driver one-time cookie initialization. Order of preference:
   - Already in memory (subsequent calls) → no-op.
   - **Restore from JetStream KV checkpoint** (`cm-checkpoints` bucket, key `<driverId>.afcookie`) — survives restart.
   - **Anchor by draining** with `db.FindChangedItems(false, 1000, null, out var next)` in a loop until empty. The returned items are discarded; the resulting cookie marks "buffer tail". This avoids replaying days of pre-existing AF history on cold start.
3. **`StreamRawChangesAsync`** — outer async iterator. Each tick:
   - Calls `DrainChanges` (sync iterator) which pages through `db.FindChangedItems(false, 500, cookie, out next)`.
   - For each `AFChangeInfo`, calls `ToRecord`.
   - Filters out self-write echoes (see "Loop Prevention" below).
   - After the drain completes, calls `TrySaveCookieAsync` to persist the advanced cookie.
   - Sleeps `pollIntervalSeconds`.
4. **`ToRecord`** — converts an `AFChangeInfo` to a `RawChangeRecord`. Uses **`info.FindObject(db.PISystem, autoLoad: true)`** rather than `db.ElementTemplates[info.ID]` or `db.Elements[info.ID]` — the indexed accessors read the local collection cache and **miss newly-added items**. `FindObject(autoLoad: true)` forces a server-side fetch, which is required for `Added` actions to resolve.

#### Reverse path: `ApplyAsync`

Implements `IWritableProtocolAdapter.ApplyAsync(descriptor, command, ct)`. Wraps the canonical AF SDK transaction pattern:

```
mutate in-memory  →  db.CheckIn()  →  on failure: db.UndoCheckOut(true)
```

Supports CRUD on two entity types:
- `elementTemplate` — `ApplyTemplate` → `db.ElementTemplates.Add` / property mutation / `Remove`
- `element` — `ApplyElement` → resolves parent by path or root, attaches template, sets attributes, commits

After every successful `CheckIn`, generates a ULID **replica-session id**, adds it to `_selfWriteSessions`, and returns it on the `WriteResult`. The forward path consumes this set to suppress the echo CDC event (loop prevention).

#### Cookie persistence

- **`SerializeCookie`** — `XmlSerializer` against the cookie's runtime type, base64-encoded, prefixed with `AssemblyQualifiedName`. The AVEVA SDK cookie is opaque (`object`) but XML-serializable.
- **`TryDeserializeCookie`** — reverses the above. All failures degrade gracefully: warning logged, fall through to re-anchor.

#### Diagnostic logging

`DrainChanges` emits Debug-level summaries every poll: `PI AF poll for '<driver>': AF returned N change(s), emitted M [paths]; skipped K [reasons]`. Silent polls become visible as `no changes`. Each yielded record also emits a `PI AF yielding <ChangeType> for <path>` debug line.

---

## The Reverse-Channel Host Service

### `src/CommonModel.Runtime.Host/ReverseChannelService.cs`

A `BackgroundService` that runs alongside the main pipeline. At startup it scans every enabled descriptor whose `sourceType` resolves to a writable adapter (i.e. registered in `WritableAdapterRegistry`) and subscribes to each subject in `nats.commandSubjects`.

Flow per message:

1. Decode bytes as a protobuf `Envelope` (`Envelope.Parser.ParseFrom(msg.Data)`).
2. Apply target filters — drop the message if `metadata.targetDriverId` or `metadata.targetSourceType` is set and doesn't match.
3. Build a `WriteCommand` — `EntityType` from the first segment of `EntityPath`, `Operation` from `ChangeType` (`Insert`/`Create` → `Create`, `Update`/`Snapshot` → `Update`, `Delete` → `Delete`).
4. Call `adapter.ApplyAsync(descriptor, command, ct)`.
5. Log the result, including the `replicaSession` id returned on success so it's correlatable with the suppressed forward echo.

Subscriptions run concurrently per subject; one failing subject doesn't take down the others.

### `src/CommonModel.Runtime.Core/Abstractions/IWritableProtocolAdapter.cs`

The contract the reverse channel uses. `WriteCommand`, `WriteResult`, and `WriteOperation` are defined alongside it. Any future writable adapter (e.g. a SQL Server sink) only needs to implement this interface and register in `WritableAdapterRegistry` to participate in the reverse channel.

### `src/CommonModel.Runtime.Drivers.Generic/Engine/WritableAdapterRegistry.cs`

Lookup map of `sourceType → IWritableProtocolAdapter`. `ReverseChannelService` consults it to know which descriptors have a writable adapter at startup.

---

## The Shared Infrastructure

### Wire format — `src/CommonModel.Runtime.Infrastructure/Protos/envelope.proto`

The single message schema used in BOTH directions. Forward CDC events and reverse write commands carry the same protobuf shape — `event_id`, `entity_path`, `change_type`, `primary_key`, `fields`, `metadata`, etc. Generated C# lives in `CommonModel.Runtime.Infrastructure.Wire` namespace.

### Publisher — `src/CommonModel.Runtime.Infrastructure/NatsPublisher.cs`

How `RawChangeEvent`s become NATS messages on the forward path. Two relevant pieces:

- **`BuildSubject` + `SanitizeSubjectSegment`** — NATS subjects accept only `[A-Za-z0-9_.-]`. PI AF entity paths like `elementTemplate/BKO Templat by Veda` contain `/` and spaces. The sanitizer converts `/` and `\` to `.` (preserving hierarchy for wildcard subscriptions) and replaces other illegal characters with `_`.
- **JetStream + DLQ + circuit breaker** — same logic used by every other source type; the AVEVA path doesn't customize it.

### Checkpoint store — `src/CommonModel.Runtime.Infrastructure/NatsCheckpointStore.cs`

JetStream KV bucket (`cm-checkpoints`) used for:
1. The AF change cookie (key `<driverId>.afcookie`).
2. Per-event checkpoints written by `DefaultEventPipeline` after each successful publish.

`BuildKey` performs analogous sanitization for KV keys (which accept `/` but not space or backslash).

### Event pipeline — `src/CommonModel.Runtime.Host/DefaultEventPipeline.cs`

The seam between adapter and outputs. For each event: publish to NATS, then save a per-entity checkpoint. Errors are caught and logged by `ConnectorPipelineService` so one bad event doesn't kill the stream.

### Driver runner — `src/CommonModel.Runtime.Host/ConnectorPipelineService.cs`

Hosts the `BaseConnector → GenericConnector → AvevaPiAfAdapter` chain. Restarts a driver on unexpected exit unless it explicitly went `Failed`. Emits diagnostic logs around each pipeline call.

---

## Loop Prevention

The reverse path writes to AF via `CheckIn`. The next forward poll will see that write as a change and would re-publish it as a CDC event — feedback loop. Two-level guard:

| Layer | File | Mechanism |
|---|---|---|
| **L1 — Adapter session** | `AvevaPiAfAdapter._selfWriteSessions` | On successful `ApplyAsync`, push a ULID. On the next forward poll, any record whose `AdapterMetadata["replicaSession"]` matches is dropped. |
| **L2 — Future** | (Reserved for cross-instance dedup via the protobuf `event_id`) | Not implemented today; required if multiple connector instances target the same AF database. |

---

## Testing — End-to-End Smoke

### Forward path: AF → NATS

1. Start `dotnet run --project src/CommonModel.Runtime.Host`.
2. In PI System Explorer, create a new Element Template (or modify one), click **Check In**.
3. Within `pollIntervalSeconds`, the connector log should show:
   - `PI AF yielding Insert for elementTemplate/<name>`
   - `NATS ► cdc.ctx-asset-framework.elementTemplate.<name>.insert`
   - `Processed Insert event for aveva-piaf-server1/elementTemplate/<name>`

### Reverse path: NATS → AF

Two CLI scripts ship in `tools/`:

- **`tools/SendWriteCommand.csx`** — publishes a protobuf `Envelope` as a write command on `cmd.avevapi-af.aveva-piaf-server1.elementTemplate.<op>`.

  ```powershell
  dotnet script tools/SendWriteCommand.csx -- create UC_TestTemplate "Test description"
  dotnet script tools/SendWriteCommand.csx -- update UC_TestTemplate "New description"
  dotnet script tools/SendWriteCommand.csx -- delete UC_TestTemplate
  ```

  Each command produces a connector log line: `Reverse-applied <Op> elementTemplate on 'aveva-piaf-server1' (corr=..., replicaSession=...)`. Refresh PI System Explorer to verify.

- **`tools/WatchCdc.csx`** — subscribes to `cdc.>` and prints every CDC event. Useful as a third terminal during reverse testing to **confirm the echo is suppressed** — a reverse-applied template should NOT appear as a CDC event on `cdc.ctx-asset-framework.>`.

Both scripts require `dotnet-script`:
```powershell
dotnet tool install -g dotnet-script
```

---

## Known Failure Modes and Where to Look

| Symptom | Most likely file | What to check |
|---|---|---|
| `PI AF system 'X' is not registered on this host` | `AvevaPiAfAdapter.OpenCoreAsync` | `afSystemName` must match an entry in PI System Explorer → File → Connections. Verify with `afdiag /pisystem:<name>`. |
| Changes detected once, then nothing | `AvevaPiAfAdapter.ToRecord` | Confirms `info.FindObject(db.PISystem, autoLoad: true)` is being used — the indexed accessor `db.ElementTemplates[id]` misses newly-added items. |
| `Subject is invalid` / DLQ failures | `NatsPublisher.BuildSubject` | The sanitizer should convert `/` `\` ` ` in entity paths; if a new char type slips through, extend `SanitizeSubjectSegment`. |
| `Key contains invalid characters` for checkpoints | `NatsCheckpointStore.BuildKey` | Same sanitizer story, KV-key rules are slightly more permissive (allow `/`). |
| Reverse channel idle on startup | `ReverseChannelService.ExecuteAsync` | Logs the registered writable sourceTypes; check descriptor `enabled` + `commandSubjects` populated. |
| `CheckIn failed: ...` on write | `AvevaPiAfAdapter.ApplyAsync` | Usually permissions — the user the connector authenticates as needs write rights on the AF database. |
| First-run replays days of history | `AvevaPiAfAdapter.EnsureCookieAsync` | Should log `Anchored ... discarded N pre-existing change record(s)`. If you see no such log and historical events flood the pipeline, the drain-anchor logic regressed. |

---

## Cookie Reset (For Testing)

A fresh anchor on next startup requires clearing the persisted cookie. With NATS CLI:

```powershell
nats kv del cm-checkpoints aveva-piaf-server1.afcookie
```

Without NATS CLI installed, use a throwaway nats-box container:

```powershell
docker run --rm --network connector-net synadia/nats-box:latest `
  nats kv del cm-checkpoints aveva-piaf-server1.afcookie --force --server nats://nats:4222
```

Nuclear option (wipes ALL NATS state):

```powershell
docker compose down nats
docker volume rm universalconnector_nats-data
docker compose up -d nats
```

---

## Why It Looks the Way It Does

A few design choices worth knowing:

1. **One adapter class, both directions.** Forward (`IProtocolAdapter`) and reverse (`IWritableProtocolAdapter`) share connection state and the loop-prevention session set. Splitting them would duplicate the `PISystems` cache and force cross-component coordination on echoes.

2. **Cookie persistence via JetStream KV, not local disk.** The legacy 4.8 connector wrote `AFDbCookie.xml` to `%LOCALAPPDATA%`. Putting it in JetStream KV lets a redeployed or container-restarted connector pick up where it left off without disk dependencies.

3. **`FindObject(autoLoad: true)` instead of indexed lookup.** This is the AVEVA-documented pattern and the only way to resolve newly-added objects on the very first poll that contains them. The indexed accessors are convenient but cache-bound.

4. **Drain-anchor on cold start instead of `GetFindChangedItemsCookie()`.** The system-level cookie returned by that API is incompatible with `AFDatabase.FindChangedItems` — passing it produces undefined behavior (most commonly: detects the first change, then nothing). The official anchor pattern is to call `FindChangedItems` once with a null cookie and `int.MaxValue` page size (we use a drain loop), discarding the returned items.

5. **Subject/key sanitization at the publisher, not the adapter.** Entity paths stay human-readable in the protobuf `Envelope.EntityPath` field — consumers see `elementTemplate/BKO Templat by Veda`. Only the NATS-addressable string (subject + KV key) gets transformed.
