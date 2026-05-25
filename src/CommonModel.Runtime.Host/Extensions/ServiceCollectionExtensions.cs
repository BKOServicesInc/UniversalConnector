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
        services.Configure<OntologyCacheOptions>(configuration.GetSection("OntologyCache"));
        services.Configure<HeartbeatOptions>(configuration.GetSection("Heartbeat"));

        // Factory owns the shared NatsConnection lifetime; IAsyncDisposable ensures cleanup.
        services.AddSingleton<NatsConnectionFactory>();
        services.AddSingleton<IAsyncDisposable>(sp => sp.GetRequiredService<NatsConnectionFactory>());
        services.AddHostedService<StartupSelfTestService>();

        services.AddHealthChecks().AddCheck<NatsHealthCheck>("nats");

        services.AddSingleton<INatsPublisher, NatsPublisher>();
        services.AddSingleton<ICheckpointStore, NatsCheckpointStore>();
        services.AddSingleton<IEventPipeline, DefaultEventPipeline>();

        services.AddHttpClient<FusekiOntologyCache>();
        services.AddSingleton<IOntologyCache, FusekiOntologyCache>();
        services.AddHostedService<OntologyCacheRefreshService>();

        services.AddGenericConnector();

        services.AddSingleton<LifecycleFsm>();

        services.AddSingleton<IConnectorRegistry, ConnectorRegistry>();
        // ConnectorPipelineService doubles as IDriverLifecycleController
        services.AddSingleton<ConnectorPipelineService>();
        services.AddSingleton<IDriverLifecycleController>(sp =>
            sp.GetRequiredService<ConnectorPipelineService>());
        services.AddHostedService(sp =>
            sp.GetRequiredService<ConnectorPipelineService>());

        services.AddHostedService<DriverLifecycleService>();
        services.AddHostedService<HealthHeartbeatService>();

        // Reverse channel — drives writable adapters (e.g. AVEVA PI AF) from NATS.
        services.AddHostedService<ReverseChannelService>();

        return services;
    }
}
