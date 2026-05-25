using Microsoft.Extensions.DependencyInjection;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Drivers.Generic.Adapters;
using CommonModel.Runtime.Drivers.Generic.Configuration;
using CommonModel.Runtime.Drivers.Generic.Engine;
using CommonModel.Runtime.Drivers.Generic.Mapping;

namespace CommonModel.Runtime.Drivers.Generic.Extensions;

public static class GenericConnectorExtensions
{
    public static IServiceCollection AddGenericConnector(
        this IServiceCollection services,
        Action<GenericConnectorOptions>? configure = null)
    {
        if (configure is not null)
            services.Configure(configure);
        else
            services.AddOptions<GenericConnectorOptions>();

        services.AddSingleton<DescriptorValidator>();
        services.AddSingleton<IDescriptorLoader, DescriptorLoader>();
        services.AddSingleton<DescriptorStore>();
        services.AddSingleton<FieldMapper>();
        services.AddSingleton<AdapterRegistry>();
        services.AddSingleton<WritableAdapterRegistry>();

        // Protocol adapters
        services.AddSingleton<IProtocolAdapter, PostgresAdapter>();
        services.AddSingleton<IProtocolAdapter, SqlServerAdapter>();
        services.AddSingleton<IProtocolAdapter, Neo4jAdapter>();
        services.AddSingleton<IProtocolAdapter, DatabricksAdapter>();
        services.AddSingleton<IProtocolAdapter, MongoDbAdapter>();

        // AVEVA PI System Explorer (AF) — bidirectional. SourceType = "avevapi-af".
        // Each AF server is configured via its own descriptor yaml — no code change.
        services.AddSingleton<AvevaPiAfAdapter>();
        services.AddSingleton<IProtocolAdapter>(sp => sp.GetRequiredService<AvevaPiAfAdapter>());

        // HTTP-based adapters registered with distinct source type names
        services.AddSingleton<IProtocolAdapter>(sp =>
            new HttpRestAdapter(sp.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>(), "sharepoint"));
        services.AddSingleton<IProtocolAdapter>(sp =>
            new HttpRestAdapter(sp.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>(), "sap"));
        services.AddSingleton<IProtocolAdapter>(sp =>
            new HttpRestAdapter(sp.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>(), "seeq"));
        services.AddSingleton<IProtocolAdapter>(sp =>
            new HttpRestAdapter(sp.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>(), "avevapi"));

        // Bootstrap service (runs before ConnectorPipelineService)
        services.AddHostedService<DescriptorBootstrapService>();

        // Generic factory fallback (sourceType = "*")
        services.AddSingleton<IDriverFactory, MultiSourceGenericFactory>();

        return services;
    }
}
