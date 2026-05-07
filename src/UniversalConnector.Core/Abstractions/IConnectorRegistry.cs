namespace UniversalConnector.Core.Abstractions;

public interface IConnectorFactory
{
    string SourceType { get; }
    IDataSourceConnector Create(string connectorId);
}

public interface IConnectorRegistry
{
    void Register(IConnectorFactory factory);
    IDataSourceConnector? Resolve(string connectorId, string sourceType);
    IReadOnlyList<IDataSourceConnector> ResolveAll();
}
