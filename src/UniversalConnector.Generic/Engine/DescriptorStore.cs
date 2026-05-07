using System.Collections.Concurrent;
using UniversalConnector.Core.Descriptors;

namespace UniversalConnector.Generic.Engine;

public sealed class DescriptorStore
{
    private readonly ConcurrentDictionary<string, ConnectorDescriptor> _store = new(StringComparer.OrdinalIgnoreCase);

    public void Register(ConnectorDescriptor descriptor) =>
        _store[descriptor.ConnectorId] = descriptor;

    public ConnectorDescriptor? Get(string connectorId) =>
        _store.TryGetValue(connectorId, out var d) ? d : null;

    public IReadOnlyList<ConnectorDescriptor> GetAll() =>
        _store.Values.ToList();

    public IReadOnlyList<ConnectorDescriptor> GetEnabled() =>
        _store.Values.Where(d => d.Enabled).ToList();

    public bool Remove(string connectorId) =>
        _store.TryRemove(connectorId, out _);

    public int Count => _store.Count;
}
