# Tools

Small `dotnet-script` (`.csx`) utilities for smoke-testing the connector end to end. None of these are wired into the host — they're standalone scripts that talk to NATS directly.

## Prerequisites

```powershell
dotnet tool install -g dotnet-script
```

The scripts reference the compiled `CommonModel.Runtime.Infrastructure.dll` so the runtime project must have been built at least once:

```powershell
dotnet build src/CommonModel.Runtime.Infrastructure
```

A NATS server must be reachable at `nats://localhost:4222`. Default `docker-compose.yml` exposes it on that port.

---

## `SendWriteCommand.csx` — exercise the reverse channel (NATS → PI)

Publishes a protobuf `Envelope` on `cmd.avevapi-af.aveva-piaf-server1.elementTemplate.<op>`. `ReverseChannelService` picks it up and `AvevaPiAfAdapter.ApplyAsync` performs the CRUD via the AF SDK.

```powershell
# create a new template
dotnet script tools/SendWriteCommand.csx -- create UC_TestTemplate "Created via reverse channel"

# update its description
dotnet script tools/SendWriteCommand.csx -- update UC_TestTemplate "Updated description"

# remove it
dotnet script tools/SendWriteCommand.csx -- delete UC_TestTemplate
```

Verify in PI System Explorer (click Refresh) under `BKO_LULU_DB → Templates → Element Templates`.

Expected connector log line on success:
```
Reverse-applied <Op> elementTemplate on 'aveva-piaf-server1' (corr=<guid>, replicaSession=<ULID>)
```

The `replicaSession` ID is what suppresses the forward-CDC echo. See `docs/AVEVA-CONNECTOR.md` § Loop Prevention.

---

## `WatchCdc.csx` — observe the forward channel (PI → NATS)

Subscribes to `cdc.>` (or a custom subject) and prints every event the connector publishes.

```powershell
# all CDC events
dotnet script tools/WatchCdc.csx

# scoped to one context
dotnet script tools/WatchCdc.csx -- "cdc.ctx-asset-framework.>"

# scoped to one entity type
dotnet script tools/WatchCdc.csx -- "cdc.ctx-asset-framework.elementTemplate.>"
```

Use it as a third terminal during reverse-channel testing to confirm the echo is suppressed — a template created via `SendWriteCommand.csx` should NOT appear here.

---

## Adding a new tool

`.csx` files are standalone — drop one in this directory with the same `#r` header pattern and it works:

```csharp
#r "nuget: NATS.Client.Core, 2.5.10"
#r "nuget: Google.Protobuf, 3.28.0"
#r "../src/CommonModel.Runtime.Infrastructure/bin/Debug/net10.0/CommonModel.Runtime.Infrastructure.dll"

using Google.Protobuf;
using NATS.Client.Core;
using CommonModel.Runtime.Infrastructure.Wire;
```

The `using Google.Protobuf;` is required for the `ToByteArray()` / `Parser.ParseFrom` extension methods.
