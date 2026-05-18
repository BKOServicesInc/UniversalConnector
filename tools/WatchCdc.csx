// Lightweight CDC subscriber — prints every event the connector publishes.
//
//   dotnet script tools/WatchCdc.csx
//   dotnet script tools/WatchCdc.csx -- "cdc.ctx-asset-framework.>"
//
// Ctrl+C to stop.

#r "nuget: NATS.Client.Core, 2.5.10"
#r "nuget: Google.Protobuf, 3.28.0"
#r "../src/CommonModel.Runtime.Infrastructure/bin/Debug/net10.0/CommonModel.Runtime.Infrastructure.dll"

using NATS.Client.Core;
using CommonModel.Runtime.Infrastructure.Wire;

var subject = Args.Count > 0 ? Args[0] : "cdc.>";

var conn = new NatsConnection(NatsOpts.Default with { Url = "nats://localhost:4222" });
try
{
    await conn.ConnectAsync();
    Console.WriteLine($"Listening on \"{subject}\"  —  Ctrl+C to exit");

    await foreach (var msg in conn.SubscribeAsync<byte[]>(subject))
    {
        try
        {
            var env = Envelope.Parser.ParseFrom(msg.Data);
            Console.WriteLine($"[{env.ChangeType,-8}] {msg.Subject}");
            Console.WriteLine($"           driver={env.DriverId}  entity={env.EntityPath}");
            if (env.Fields.Count > 0)
            {
                var sample = string.Join(", ", env.Fields.Take(4).Select(kv => $"{kv.Key}={kv.Value}"));
                Console.WriteLine($"           fields={sample}{(env.Fields.Count > 4 ? ", ..." : "")}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[parse-err] {msg.Subject}: {ex.Message}");
        }
    }
}
finally
{
    await conn.DisposeAsync();
}
