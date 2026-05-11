using Microsoft.Extensions.Logging;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Drivers.Generic.Engine;

namespace CommonModel.Runtime.Host;

public sealed class ConnectorRegistry : IConnectorRegistry
{
    private readonly List<IDriverFactory> _factories;
    private readonly DescriptorStore _descriptorStore;
    private readonly ILogger<ConnectorRegistry> _logger;

    public ConnectorRegistry(
        IEnumerable<IDriverFactory> factories,
        DescriptorStore descriptorStore,
        ILogger<ConnectorRegistry> logger)
    {
        _factories = factories.ToList();
        _descriptorStore = descriptorStore;
        _logger = logger;
    }

    public void Register(IDriverFactory factory) => _factories.Add(factory);

    public ISourceDriver? Resolve(string driverId, string sourceType)
    {
        var factory = _factories.FirstOrDefault(f =>
            string.Equals(f.SourceType, sourceType, StringComparison.OrdinalIgnoreCase))
            ?? _factories.FirstOrDefault(f => f.SourceType == "*");

        if (factory is null)
        {
            _logger.LogWarning("No factory found for driverId '{Id}' sourceType '{Type}'",
                driverId, sourceType);
            return null;
        }

        return factory.Create(driverId);
    }

    public IReadOnlyList<ISourceDriver> ResolveAll()
    {
        var drivers = new List<ISourceDriver>();
        var genericFactory = _factories.FirstOrDefault(f => f.SourceType == "*");

        if (genericFactory is null) return drivers;

        foreach (var descriptor in _descriptorStore.GetEnabled())
        {
            try
            {
                var driver = genericFactory.Create(descriptor.ConnectorId);
                drivers.Add(driver);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create driver for '{ConnectorId}'", descriptor.ConnectorId);
            }
        }

        return drivers;
    }
}
