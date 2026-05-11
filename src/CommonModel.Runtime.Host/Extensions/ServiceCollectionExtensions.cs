using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Drivers.Generic.Extensions;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Host.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddUniversalConnector(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<NatsOptions>(configuration.GetSection("Nats"));
        services.AddSingleton<INatsPublisher, NatsPublisher>();

        services.AddGenericConnector();

        services.AddSingleton<IConnectorRegistry, ConnectorRegistry>();
        services.AddHostedService<ConnectorPipelineService>();

        return services;
    }
}
