using Microsoft.Extensions.Logging;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Drivers.Generic.Mapping;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

public sealed class MultiSourceGenericFactory : IConnectorFactory
{
    private readonly AdapterRegistry _adapterRegistry;
    private readonly DescriptorStore _descriptorStore;
    private readonly FieldMapper _fieldMapper;
    private readonly ILoggerFactory _loggerFactory;

    public string SourceType => "*";

    public MultiSourceGenericFactory(
        AdapterRegistry adapterRegistry,
        DescriptorStore descriptorStore,
        FieldMapper fieldMapper,
        ILoggerFactory loggerFactory)
    {
        _adapterRegistry = adapterRegistry;
        _descriptorStore = descriptorStore;
        _fieldMapper = fieldMapper;
        _loggerFactory = loggerFactory;
    }

    public IDataSourceConnector Create(string connectorId)
    {
        var descriptor = _descriptorStore.Get(connectorId)
            ?? throw new InvalidOperationException($"No descriptor found for connectorId '{connectorId}'");

        var adapter = _adapterRegistry.Resolve(descriptor.SourceType);
        var logger = _loggerFactory.CreateLogger<GenericConnector>();

        return new GenericConnector(descriptor, adapter, _fieldMapper, logger);
    }
}
