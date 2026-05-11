namespace CommonModel.Runtime.Core.Abstractions;

public interface IDriverFactory
{
    string SourceType { get; }
    ISourceDriver Create(string driverId);
}

public interface IConnectorRegistry
{
    void Register(IDriverFactory factory);
    ISourceDriver? Resolve(string driverId, string sourceType);
    IReadOnlyList<ISourceDriver> ResolveAll();
}
