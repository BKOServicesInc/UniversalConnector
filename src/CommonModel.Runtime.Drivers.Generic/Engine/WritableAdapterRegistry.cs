using Microsoft.Extensions.Logging;
using CommonModel.Runtime.Core.Abstractions;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

public sealed class WritableAdapterRegistry
{
    private readonly IReadOnlyDictionary<string, IWritableProtocolAdapter> _adapters;

    public WritableAdapterRegistry(
        IEnumerable<IProtocolAdapter> allAdapters,
        ILogger<WritableAdapterRegistry> logger)
    {
        var dict = new Dictionary<string, IWritableProtocolAdapter>(StringComparer.OrdinalIgnoreCase);
        foreach (var a in allAdapters)
        {
            if (a is IWritableProtocolAdapter writable)
            {
                if (dict.ContainsKey(a.SourceType))
                {
                    logger.LogWarning(
                        "Duplicate writable adapter for sourceType '{Source}' — keeping first registration",
                        a.SourceType);
                    continue;
                }
                dict[a.SourceType] = writable;
                logger.LogInformation("Registered writable adapter for sourceType '{Source}'", a.SourceType);
            }
        }
        _adapters = dict;
    }

    public bool TryResolve(string sourceType, out IWritableProtocolAdapter? adapter) =>
        _adapters.TryGetValue(sourceType, out adapter);

    public IReadOnlyList<string> RegisteredSourceTypes => _adapters.Keys.ToList();
}
