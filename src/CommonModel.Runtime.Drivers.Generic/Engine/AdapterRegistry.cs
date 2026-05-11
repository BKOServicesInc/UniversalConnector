using Microsoft.Extensions.Logging;
using CommonModel.Runtime.Core.Abstractions;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

public sealed class AdapterRegistry
{
    private readonly IReadOnlyDictionary<string, IProtocolAdapter> _adapters;

    public AdapterRegistry(IEnumerable<IProtocolAdapter> adapters, ILogger<AdapterRegistry> logger)
    {
        var dict = new Dictionary<string, IProtocolAdapter>(StringComparer.OrdinalIgnoreCase);
        foreach (var adapter in adapters)
        {
            if (dict.TryGetValue(adapter.SourceType, out var existing))
            {
                logger.LogWarning(
                    "Duplicate adapter registration for source type '{SourceType}': " +
                    "'{Existing}' is already registered. '{Duplicate}' will be ignored.",
                    adapter.SourceType, existing.GetType().Name, adapter.GetType().Name);
                continue;
            }
            dict[adapter.SourceType] = adapter;
        }
        _adapters = dict;
    }

    public IProtocolAdapter Resolve(string sourceType)
    {
        if (_adapters.TryGetValue(sourceType, out var adapter))
            return adapter;
        throw new InvalidOperationException($"No adapter registered for source type '{sourceType}'");
    }

    public bool TryResolve(string sourceType, out IProtocolAdapter? adapter) =>
        _adapters.TryGetValue(sourceType, out adapter);

    public IReadOnlyList<string> RegisteredSourceTypes => _adapters.Keys.ToList();
}
