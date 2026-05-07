using Microsoft.Extensions.Logging;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Generic.Engine;

namespace UniversalConnector.Host;

public sealed class ConnectorRegistry : IConnectorRegistry
{
    private readonly List<IConnectorFactory> _factories;
    private readonly DescriptorStore _descriptorStore;
    private readonly ILogger<ConnectorRegistry> _logger;

    public ConnectorRegistry(
        IEnumerable<IConnectorFactory> factories,
        DescriptorStore descriptorStore,
        ILogger<ConnectorRegistry> logger)
    {
        _factories = factories.ToList();
        _descriptorStore = descriptorStore;
        _logger = logger;
    }

    public void Register(IConnectorFactory factory) => _factories.Add(factory);

    public IDataSourceConnector? Resolve(string connectorId, string sourceType)
    {
        var factory = _factories.FirstOrDefault(f =>
            string.Equals(f.SourceType, sourceType, StringComparison.OrdinalIgnoreCase))
            ?? _factories.FirstOrDefault(f => f.SourceType == "*");

        if (factory is null)
        {
            _logger.LogWarning("No factory found for connectorId '{Id}' sourceType '{Type}'",
                connectorId, sourceType);
            return null;
        }

        return factory.Create(connectorId);
    }

    public IReadOnlyList<IDataSourceConnector> ResolveAll()
    {
        var connectors = new List<IDataSourceConnector>();
        var genericFactory = _factories.FirstOrDefault(f => f.SourceType == "*");

        if (genericFactory is null) return connectors;

        foreach (var descriptor in _descriptorStore.GetEnabled())
        {
            try
            {
                var connector = genericFactory.Create(descriptor.ConnectorId);
                connectors.Add(connector);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create connector for '{ConnectorId}'", descriptor.ConnectorId);
            }
        }

        return connectors;
    }
}
