# UniversalConnector

A descriptor-driven .NET 10 worker service that propagates change events between heterogeneous data sources (PostgreSQL, SQL Server, MongoDB, Neo4j, **AVEVA PI System Explorer**, Databricks, HTTP/REST) and NATS JetStream. Bidirectional for AVEVA PI AF — forward CDC and reverse write-channel.

This README covers **first-time local setup and end-to-end testing**. For the architecture overview see `UC_EXPLAINED.md`. For the AVEVA file-by-file guide see `docs/AVEVA-CONNECTOR.md`.

---

## What you can do after running through this

By the end of this guide you'll have:

1. A NATS server running locally in Docker
2. The connector building and running against your PI AF environment
3. A check-in in PI System Explorer producing a CDC event visible on NATS
4. A NATS command creating an Element Template inside PI System Explorer

If you don't have a PI AF environment, you can still run the connector and exercise the non-AVEVA sources (Postgres, Mongo, Neo4j) — skip the AVEVA sections.

---

## Prerequisites

| Requirement | Why | How to verify |
|---|---|---|
| **Windows 10 / 11 / Server** | The PI AF SDK is Windows-only. Other sources work on macOS / Linux. | `winver` |
| **.NET 10 SDK** | Builds and runs the connector | `dotnet --version` → 10.x |
| **Docker Desktop** | NATS + optional Postgres/Neo4j containers | `docker --version` |
| **Git** | Clone the repo | `git --version` |
| **PI System Explorer + AF Client** | AVEVA only — connects the AF SDK to a real PI AF server | Open PI System Explorer; File → Connections lists your server |
| **`dotnet-script`** *(optional)* | Test scripts in `tools/` | `dotnet script --version` |

Install `dotnet-script` if you want to use the test tools:
```powershell
dotnet tool install -g dotnet-script
```

---

## 1. Clone and configure

```powershell
git clone <repo-url>
cd UniversalConnector
git checkout Aveva-Connector

Copy-Item .env.example .env
```

Open `.env` and fill in the values for whichever sources you want to use. For an AVEVA-only smoke test, only the `AF_*` block matters:

```env
AF_SYSTEM_NAME=Aveva-Pi          # exact name from PI System Explorer → File → Connections
AF_DATABASE=BKO_LULU_DB          # the database in that AF server you want to watch
AF_USERNAME=                     # blank for Windows-integrated auth
AF_PASSWORD=
```

`AF_SYSTEM_NAME` is the **logical AF server name** as registered in PI System Explorer — not a Windows hostname. Wrong value here = `PI AF system 'X' is not registered on this host`.

---

## 2. Start NATS

```powershell
docker compose up -d nats
```

Verify it's healthy:
```powershell
docker ps --filter name=nats
# STATUS column should say "Up ... (healthy)"
```

Confirm the monitoring port responds: open http://localhost:8222/healthz in a browser — should return `{"status":"ok"}`.

If you want a JetStream/KV UI, also start the optional monitoring container:
```powershell
docker compose --profile monitoring up -d nats-surveyor
```
…then visit http://localhost:7777.

---

## 3. Verify your AF server is reachable

This step is AVEVA-specific. Skip if not testing AVEVA.

```powershell
afdiag /pisystem:Aveva-Pi
```

You should see server version, database list, and a successful connection line. If this fails, the connector will fail with the same error — fix it here first.

Common failures:
- **`PI AF system 'X' is not registered`** — open PI System Explorer → File → Connections → Add Server. The name you enter must match exactly what your `.env` `AF_SYSTEM_NAME` says.
- **`Access denied`** — your Windows user lacks rights on the AF database. Have a PI AF admin grant read+write on the target database (or set explicit `AF_USERNAME` / `AF_PASSWORD` in `.env`).

---

## 4. Build and run

From the repo root:

```powershell
dotnet run --project src/CommonModel.Runtime.Host
```

First run will download NuGet packages (including `Aveva.AFSDK`) and may take a minute. Expected startup logs:

```
Checkpoint KV bucket 'cm-checkpoints' ready
Starting N driver(s)
Starting driver 'aveva-piaf-server1' (avevapi-af)
Connected to PI AF system 'Aveva-Pi' (server: ..., version: ...)
Anchored PI AF change-cookie for driver 'aveva-piaf-server1' to current database tail
    (discarded N pre-existing change record(s) from the AF buffer).
Reverse channel for 'aveva-piaf-server1' subscribing to: cmd.avevapi-af.aveva-piaf-server1.>, ...
Heartbeat published for 'aveva-piaf-server1' (state: Streaming)
```

If you see `Anchored ...` instead of `Restored ... from checkpoint`, this is a clean first run — perfect. The connector won't replay your AF history; it starts from "now".

Leave this terminal running for the test steps below.

---

## 5. Test the forward path — AF → NATS

In a **second terminal**, run the CDC watcher:

```powershell
dotnet script tools/WatchCdc.csx
```

You should see `Listening on "cdc.>"  —  Ctrl+C to exit` and then heartbeat events scrolling by.

Now in **PI System Explorer**:

1. Right-click `BKO_LULU_DB → Templates → Element Templates → New Template`
2. Name it `UC_ForwardTest`, add description "forward-path test"
3. Click **Check In** (toolbar)
4. Confirm the check-in dialog

Within ~15 seconds (your `pollIntervalSeconds`), three things should happen:

**Connector terminal:**
```
PI AF yielding Insert for elementTemplate/UC_ForwardTest (driver=aveva-piaf-server1)
Pipeline receiving Insert for aveva-piaf-server1/elementTemplate/UC_ForwardTest ...
NATS ► cdc.ctx-asset-framework.elementTemplate.UC_ForwardTest.insert [Insert] driver=aveva-piaf-server1 ...
Processed Insert event for aveva-piaf-server1/elementTemplate/UC_ForwardTest
Pipeline returned for eventId=...
PI AF poll for 'aveva-piaf-server1': AF returned 1 change(s), emitted 1 [elementTemplate/UC_ForwardTest]; skipped 0 [].
```

**Watcher terminal:**
```
[Insert  ] cdc.ctx-asset-framework.elementTemplate.UC_ForwardTest.insert
           driver=aveva-piaf-server1  entity=elementTemplate/UC_ForwardTest
           fields=name=UC_ForwardTest, description=forward-path test, ...
```

If you see this, forward path works.

---

## 6. Test the reverse path — NATS → AF

Keep the connector and the watcher running. Open a **third terminal**.

### 6a. Create a template via NATS

```powershell
dotnet script tools/SendWriteCommand.csx -- create UC_ReverseTest "created via reverse channel"
```

Expected output:
```
Sent: Insert 'UC_ReverseTest' → cmd.avevapi-af.aveva-piaf-server1.elementTemplate.insert
```

**Connector terminal:**
```
Reverse-applied Create elementTemplate on 'aveva-piaf-server1' (corr=..., replicaSession=01K...)
PI AF CheckIn ► Create elementTemplate (corr=..., replicaSession=01K...)
```

**In PI System Explorer:** click Refresh in the toolbar. `UC_ReverseTest` should now appear under Element Templates with the description "created via reverse channel".

**In the watcher terminal:** you should **NOT** see a CDC event for `UC_ReverseTest`. That's the loop-prevention guard at work — the `replicaSession` ID matches the upcoming forward poll's change record, which is suppressed.

### 6b. Update it

```powershell
dotnet script tools/SendWriteCommand.csx -- update UC_ReverseTest "updated via reverse channel"
```

Refresh PI System Explorer → description field is updated.

### 6c. Delete it

```powershell
dotnet script tools/SendWriteCommand.csx -- delete UC_ReverseTest
```

Refresh PI System Explorer → template is gone.

---

## 7. Onboarding another AVEVA server (multi-machine pattern)

Each AVEVA installation lives on its own Windows machine (often an RDP'd remote desktop). The cleanest pattern is **one connector instance per machine**, all publishing to a single shared NATS.

### On the new machine

1. Install .NET 10 SDK, Docker Desktop (or just NATS-CLI if you don't need to run NATS here), Git, PI System Explorer.
2. `git clone` this repo and `git checkout Aveva-Connector`.
3. Either edit `connectors/aveva-piaf-server1.yaml` in place with this machine's AF system name, or copy `connectors/examples/aveva-piaf-server2.yaml` to `connectors/aveva-piaf-server2.yaml` and set:
   ```yaml
   driverId: aveva-piaf-server2                       # MUST be unique
   enabled: true
   connection:
     afSystemName: "${AF2_SYSTEM_NAME}"
     afDatabase:   "${AF2_DATABASE}"
   nats:
     commandSubjects:
       - "cmd.avevapi-af.aveva-piaf-server2.>"        # MUST not overlap with server1
     commandConsumer: aveva-piaf-server2-writer        # MUST be unique
   ```
4. Set `.env`:
   ```env
   AF2_SYSTEM_NAME=<the AF system name on THIS machine>
   AF2_DATABASE=<the database>
   Nats__Servers__0=nats://<central-nats-host>:4222   # the SHARED NATS, not localhost
   ```
5. `dotnet run --project src/CommonModel.Runtime.Host`.

The central NATS now receives CDC events from both machines on the same subject namespace, distinguished by the `afServer` NATS header (set via `nats.additionalHeaders` in each descriptor).

For more on this pattern see "Why one connector per remote desktop" in `docs/AVEVA-CONNECTOR.md`.

---

## 8. Inspecting state

### See all forward CDC events
```powershell
dotnet script tools/WatchCdc.csx
```

### See events for one AF server only
```powershell
dotnet script tools/WatchCdc.csx -- "cdc.ctx-asset-framework.>"
```

### See raw NATS messages with the `nats` CLI

If you have the NATS CLI installed (`winget install nats-io.nats` or `scoop install nats`):

```powershell
nats sub "cdc.>"
nats stream ls
nats kv ls
nats kv get cm-checkpoints aveva-piaf-server1.afcookie    # the AF change cookie
```

Without the CLI installed, use a throwaway container:

```powershell
docker run --rm -it --network connector-net synadia/nats-box:latest `
  nats sub "cdc.>" --server nats://nats:4222
```

---

## 9. Resetting state (between tests)

### Wipe the AF change cookie — next start re-anchors to "now"

```powershell
# With NATS CLI:
nats kv del cm-checkpoints aveva-piaf-server1.afcookie

# Without NATS CLI:
docker run --rm --network connector-net synadia/nats-box:latest `
  nats kv del cm-checkpoints aveva-piaf-server1.afcookie --force --server nats://nats:4222
```

### Wipe ALL NATS state (nuclear)

```powershell
docker compose down nats
docker volume rm universalconnector_nats-data
docker compose up -d nats
```

---

## 10. Troubleshooting

| Symptom | What it means | Where to look |
|---|---|---|
| `PI AF system 'X' is not registered on this host` | The name in your descriptor doesn't match what's in PI System Explorer → File → Connections | Fix the name; verify with `afdiag /pisystem:X` |
| `Anchored ... discarded 5000 pre-existing change record(s)` on first run | Normal — your AF buffer had history. Future polls will only emit new changes. | No action needed |
| First template detected, then nothing on subsequent ones | The fix in `AvevaPiAfAdapter.ToRecord` to use `info.FindObject(autoLoad: true)` regressed | `src/CommonModel.Runtime.Drivers.Generic/Adapters/AvevaPiAfAdapter.cs` |
| `Subject is invalid` from NATS | An entity path contains chars `BuildSubject` doesn't sanitize | `src/CommonModel.Runtime.Infrastructure/NatsPublisher.cs` `SanitizeSubjectSegment` |
| `CheckIn failed: ... permission denied` | The AF user the connector authenticates as lacks write rights on the database | Have a PI AF admin grant write on the target DB, or set explicit `AF_USERNAME` / `AF_PASSWORD` |
| `Reverse channel idle — no enabled descriptors declare commandSubjects` | Descriptor has `enabled: false` or empty `commandSubjects` | Check the YAML |
| `NATS.Client.Core.NatsException: connection refused` | NATS isn't running or `Nats__Servers__0` points at the wrong host | `docker ps` to check NATS; test reachability |
| `error CS1061: 'Envelope' does not contain a definition for 'ToByteArray'` from a `.csx` script | Missing `using Google.Protobuf;` at the top of the script | The shipped scripts already have this; if you wrote a new script, add the using |

### Enabling verbose logs

`src/CommonModel.Runtime.Host/appsettings.json` already sets `"CommonModel.Runtime": "Debug"`, so the per-poll diagnostic lines should appear out of the box:

- `PI AF poll for 'aveva-piaf-server1': no changes.`  — loop is alive, no changes
- `PI AF poll for 'aveva-piaf-server1': AF returned N change(s), emitted M [paths]; skipped K [reasons].` — saw changes
- `Pipeline receiving / Pipeline returned` — confirms each event made it through

If you don't see these lines in your terminal, double-check the appsettings.json log levels.

---

## 11. Project layout reference

| Path | What's in it |
|---|---|
| `src/CommonModel.Runtime.Core/` | Abstractions, models, descriptor schema |
| `src/CommonModel.Runtime.Drivers.Generic/` | All adapter implementations (`AvevaPiAfAdapter`, `PostgresAdapter`, ...) and the `GenericConnector` engine |
| `src/CommonModel.Runtime.Infrastructure/` | NATS publisher, checkpoint store, protobuf wire format |
| `src/CommonModel.Runtime.Host/` | The worker-service entrypoint, pipeline, reverse-channel host |
| `connectors/` | YAML descriptor files — one per data source instance |
| `tools/` | `dotnet-script` smoke-test utilities |
| `tests/` | xUnit tests |
| `docs/` | Architecture and connector-specific deep-dives |
| `docker/` | NATS config, init scripts for the optional database containers |

---

## 12. Further reading

- **`UC_EXPLAINED.md`** — high-level architecture, left-to-right data flow, every layer explained
- **`docs/AVEVA-CONNECTOR.md`** — file-by-file walkthrough of the AVEVA bidirectional flow, loop prevention, design choices
- **`connectors/README.md`** — index of all descriptors and how to onboard another source
- **`tools/README.md`** — what each test script does and how to write your own
- **`SPEC.md`** / **`new-project-plan/`** — original design specs and contracts

---

## Quick reference card

```powershell
# First-time setup
docker compose up -d nats
Copy-Item .env.example .env
# (edit .env with your AF_SYSTEM_NAME / AF_DATABASE)

# Run the connector
dotnet run --project src/CommonModel.Runtime.Host

# Test forward path (in a second terminal)
dotnet script tools/WatchCdc.csx
# then check in something in PI System Explorer

# Test reverse path (in a third terminal)
dotnet script tools/SendWriteCommand.csx -- create UC_Test "test description"
dotnet script tools/SendWriteCommand.csx -- update UC_Test "updated"
dotnet script tools/SendWriteCommand.csx -- delete UC_Test

# Reset cookie for a clean test
nats kv del cm-checkpoints aveva-piaf-server1.afcookie

# Stop everything
docker compose down
# (Ctrl+C in the connector terminal)
```
