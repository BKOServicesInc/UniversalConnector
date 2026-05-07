using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Configuration;
using UniversalConnector.Generic.Extensions;
using UniversalConnector.Host.Services;
using UniversalConnector.Nats;

namespace UniversalConnector.Host.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddUniversalConnector(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<NatsOptions>(configuration.GetSection("Nats"));
        services.AddSingleton<INatsPublisher, NatsPublisher>();

        services.Configure<PostgresSinkOptions>(configuration.GetSection("PostgresSink"));
        services.AddSingleton<IDataSink, PostgresDataSink>();

        services.AddGenericConnector();

        services.AddSingleton<IConnectorRegistry, ConnectorRegistry>();
        services.AddHostedService<ConnectorPipelineService>();

        return services;
    }
}
