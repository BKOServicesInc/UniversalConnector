using Microsoft.Extensions.Hosting;
using CommonModel.Runtime.Host.Extensions;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddUniversalConnector(builder.Configuration);

var host = builder.Build();
await host.RunAsync();
