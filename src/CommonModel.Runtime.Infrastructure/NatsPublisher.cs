using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsPublisher : INatsPublisher
{
    private readonly NatsOptions _options;
    private readonly ILogger<NatsPublisher> _logger;
    private NatsConnection? _connection;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public NatsPublisher(IOptions<NatsOptions> options, ILogger<NatsPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync(
        RawChangeEvent evt,
        string? subjectOverride = null,
        IReadOnlyDictionary<string, string>? additionalHeaders = null,
        CancellationToken ct = default)
    {
        var connection = await GetOrCreateConnectionAsync(ct);

        var subject = subjectOverride
            ?? $"{_options.SubjectPrefix}.{evt.SourceType}.{evt.DriverId}.{evt.ChangeType}"
                .ToLowerInvariant();

        var json = JsonSerializer.Serialize(evt, JsonOptions.Default);
        var headers = BuildHeaders(evt, additionalHeaders);

        try
        {
            await connection.PublishAsync(subject, json, headers: headers, cancellationToken: ct);

            _logger.LogDebug("Published {ChangeType} event for {DriverId}/{EntityPath} to {Subject}",
                evt.ChangeType, evt.DriverId, evt.EntityPath, subject);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish event {EventId} to subject {Subject}",
                evt.EventId, subject);
            throw;
        }
    }

    private async ValueTask<NatsConnection> GetOrCreateConnectionAsync(CancellationToken ct)
    {
        if (_connection is not null) return _connection;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_connection is not null) return _connection;

            var opts = NatsOpts.Default with
            {
                Url = string.Join(",", _options.Servers)
            };

            var conn = new NatsConnection(opts);
            await conn.ConnectAsync();

            _logger.LogInformation("NATS connection established to {Servers}",
                string.Join(", ", _options.Servers));

            _connection = conn;
            return _connection;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private static NatsHeaders BuildHeaders(
        RawChangeEvent evt,
        IReadOnlyDictionary<string, string>? extra)
    {
        var headers = new NatsHeaders
        {
            ["driverId"]    = evt.DriverId,
            ["sourceType"]  = evt.SourceType,
            ["changeType"]  = evt.ChangeType.ToString()
        };

        if (extra is not null)
            foreach (var (k, v) in extra)
                headers[k] = v;

        return headers;
    }

    public async ValueTask DisposeAsync()
    {
        _initLock.Dispose();
        if (_connection is not null)
            await _connection.DisposeAsync();
    }
}

file static class JsonOptions
{
    internal static readonly JsonSerializerOptions Default = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}
