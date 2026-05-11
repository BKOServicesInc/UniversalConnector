using System.Collections.Concurrent;
using CommonModel.Runtime.Core.Descriptors;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

public sealed class DescriptorStore
{
    private readonly ConcurrentDictionary<string, ConnectorDescriptor> _store = new(StringComparer.OrdinalIgnoreCase);

    public void Register(ConnectorDescriptor descriptor) =>
        _store[descriptor.DriverId] = descriptor;

    public ConnectorDescriptor? Get(string driverId) =>
        _store.TryGetValue(driverId, out var d) ? d : null;

    public IReadOnlyList<ConnectorDescriptor> GetAll() =>
        _store.Values.ToList();

    public IReadOnlyList<ConnectorDescriptor> GetEnabled() =>
        _store.Values.Where(d => d.Enabled).ToList();

    public bool Remove(string driverId) =>
        _store.TryRemove(driverId, out _);

    public int Count => _store.Count;
}
