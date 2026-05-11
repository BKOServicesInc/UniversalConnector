using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsCheckpointStore : ICheckpointStore, IAsyncDisposable
{
    private readonly NatsOptions _options;
    private readonly ILogger<NatsCheckpointStore> _logger;
    private NatsConnection? _connection;
    private INatsKVStore? _kv;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public NatsCheckpointStore(IOptions<NatsOptions> options, ILogger<NatsCheckpointStore> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<Checkpoint?> GetAsync(string driverId, string entityPath, CancellationToken ct = default)
    {
        var kv = await GetOrCreateKvAsync(ct);
        var key = BuildKey(driverId, entityPath);

        try
        {
            var entry = await kv.GetEntryAsync<byte[]>(key, cancellationToken: ct);
            if (entry.Value is null) return null;

            return JsonSerializer.Deserialize<Checkpoint>(entry.Value);
        }
        catch (NatsKVKeyNotFoundException)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read checkpoint for {DriverId}/{EntityPath}", driverId, entityPath);
            return null;
        }
    }

    public async Task SaveAsync(Checkpoint checkpoint, CancellationToken ct = default)
    {
        var kv = await GetOrCreateKvAsync(ct);
        var key = BuildKey(checkpoint.DriverId, checkpoint.EntityPath);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(checkpoint);

        try
        {
            await kv.PutAsync(key, bytes, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save checkpoint for {DriverId}/{EntityPath}",
                checkpoint.DriverId, checkpoint.EntityPath);
        }
    }

    private async ValueTask<INatsKVStore> GetOrCreateKvAsync(CancellationToken ct)
    {
        if (_kv is not null) return _kv;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_kv is not null) return _kv;

            var opts = NatsOpts.Default with { Url = string.Join(",", _options.Servers) };
            _connection = new NatsConnection(opts);
            await _connection.ConnectAsync();

            var js = new NatsJSContext(_connection);
            var kvCtx = new NatsKVContext(js);
            _kv = await GetOrCreateBucketAsync(kvCtx, _options.CheckpointBucket, ct);

            _logger.LogInformation("Checkpoint KV bucket '{Bucket}' ready", _options.CheckpointBucket);
        }
        finally
        {
            _initLock.Release();
        }

        return _kv!;
    }

    private static async Task<INatsKVStore> GetOrCreateBucketAsync(
        NatsKVContext kvCtx, string bucket, CancellationToken ct)
    {
        try
        {
            return await kvCtx.CreateStoreAsync(new NatsKVConfig(bucket), ct);
        }
        catch
        {
            return await kvCtx.GetStoreAsync(bucket, ct);
        }
    }

    // KV keys must not contain spaces; dots are valid NATS KV key separators.
    // Colons (e.g. from context names like ctx:PID) are not valid — replace with dash.
    private static string BuildKey(string driverId, string entityPath) =>
        $"{driverId}.{entityPath}".Replace(':', '-').ToLowerInvariant();

    public async ValueTask DisposeAsync()
    {
        _initLock.Dispose();
        if (_connection is not null)
            await _connection.DisposeAsync();
    }
}
