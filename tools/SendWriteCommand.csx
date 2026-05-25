// Smoke-test tool: publishes a write command to the reverse channel.
//
// Usage from repo root:
//   dotnet script tools/SendWriteCommand.csx -- create UC_Compressor "Reverse-path test template"
//   dotnet script tools/SendWriteCommand.csx -- update UC_Compressor "Updated description"
//   dotnet script tools/SendWriteCommand.csx -- delete UC_Compressor

#r "nuget: NATS.Client.Core, 2.5.10"
#r "nuget: Google.Protobuf, 3.28.0"
#r "../src/CommonModel.Runtime.Infrastructure/bin/Debug/net10.0/CommonModel.Runtime.Infrastructure.dll"

using Google.Protobuf;
using NATS.Client.Core;
using CommonModel.Runtime.Infrastructure.Wire;

var op   = Args.Count > 0 ? Args[0].ToLowerInvariant() : "create";
var name = Args.Count > 1 ? Args[1] : "UC_Compressor";
var desc = Args.Count > 2 ? Args[2] : $"Reverse-path test template ({op})";

var changeType = op switch
{
    "create" or "insert" => "Insert",
    "update"             => "Update",
    "delete" or "remove" => "Delete",
    _ => throw new ArgumentException($"Unknown op '{op}'. Use create | update | delete.")
};

var env = new Envelope
{
    EventId    = Guid.NewGuid().ToString(),
    SourceType = "operator",
    DriverId   = "demo-operator",
    Context    = "ctx:asset-framework",
    EntityPath = $"elementTemplate/{name}",
    ChangeType = changeType
};
env.PrimaryKey["name"]         = name;
env.Fields["description"]      = desc;
env.Metadata["targetDriverId"] = "aveva-piaf-server1";

var subject = $"cmd.avevapi-af.aveva-piaf-server1.elementTemplate.{changeType.ToLowerInvariant()}";

var conn = new NatsConnection(NatsOpts.Default with { Url = "nats://nats:4222" });
try
{
    await conn.ConnectAsync();
    await conn.PublishAsync(subject, env.ToByteArray());
    await Task.Delay(200);
    Console.WriteLine($"Sent: {changeType} '{name}' → {subject}");
}
finally
{
    await conn.DisposeAsync();
}
