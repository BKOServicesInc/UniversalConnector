using System.Collections.Concurrent;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<string, Checkpoint> _store = new();

    public Task<Checkpoint?> GetAsync(string driverId, string entityPath, CancellationToken ct = default)
    {
        _store.TryGetValue(Key(driverId, entityPath), out var cp);
        return Task.FromResult(cp);
    }

    public Task SaveAsync(Checkpoint checkpoint, CancellationToken ct = default)
    {
        _store[Key(checkpoint.DriverId, checkpoint.EntityPath)] = checkpoint;
        return Task.CompletedTask;
    }

    public int Count => _store.Count;

    private static string Key(string driverId, string entityPath) =>
        $"{driverId}:{entityPath}";
}
