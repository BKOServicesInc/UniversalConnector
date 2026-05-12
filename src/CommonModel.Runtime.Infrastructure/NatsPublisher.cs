using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure.Wire;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsPublisher : INatsPublisher
{
    private readonly NatsOptions _options;
    private readonly NatsConnectionFactory _factory;
    private readonly ILogger<NatsPublisher> _logger;
    private NatsConnection? _connection;
    private NatsJSContext? _js;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    // Retry delays per attempt index: [100 ms, 1 s, 10 s]
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(10)
    ];

    public NatsPublisher(
        IOptions<NatsOptions> options,
        NatsConnectionFactory factory,
        ILogger<NatsPublisher> logger)
    {
        _options = options.Value;
        _factory = factory;
        _logger = logger;
    }

    public async Task PublishAsync(
        RawChangeEvent evt,
        string? subjectOverride = null,
        IReadOnlyDictionary<string, string>? additionalHeaders = null,
        CancellationToken ct = default)
    {
        var (conn, js) = await GetOrCreateConnectionAsync(ct);

        var subject = subjectOverride ?? BuildSubject(evt);
        var envelope = BuildEnvelope(evt);
        var bytes = envelope.ToByteArray();
        var headers = BuildHeaders(evt, additionalHeaders);

        Exception? lastEx = null;
        for (int attempt = 0; attempt < RetryDelays.Length; attempt++)
        {
            try
            {
                var ack = await js.PublishAsync(subject, bytes, headers: headers, cancellationToken: ct);
                ack.EnsureSuccess();

                _logger.LogDebug("Published {ChangeType} event {EventId} for {DriverId}/{EntityPath} to {Subject}",
                    evt.ChangeType, evt.EventId, evt.DriverId, evt.EntityPath, subject);
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                lastEx = ex;
                _logger.LogWarning(ex,
                    "Publish attempt {Attempt}/{Total} failed for event {EventId}; retrying in {Delay}",
                    attempt + 1, RetryDelays.Length, evt.EventId, RetryDelays[attempt]);
                await Task.Delay(RetryDelays[attempt], ct);
            }
        }

        // Final attempt — if this also fails, route to DLQ via core NATS
        try
        {
            var ack = await js.PublishAsync(subject, bytes, headers: headers, cancellationToken: ct);
            ack.EnsureSuccess();
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "All publish attempts exhausted for event {EventId}; routing to DLQ", evt.EventId);

            var dlqSubject = $"{_options.DlqSubjectPrefix}.{evt.DriverId}".ToLowerInvariant();
            try
            {
                await conn.PublishAsync(dlqSubject, bytes, headers: headers, cancellationToken: ct);
                _logger.LogWarning("Event {EventId} routed to DLQ subject {DlqSubject}", evt.EventId, dlqSubject);
            }
            catch (Exception dlqEx)
            {
                _logger.LogError(dlqEx,
                    "DLQ publish also failed for event {EventId} — event lost", evt.EventId);
            }
        }
    }

    private async ValueTask<(NatsConnection conn, NatsJSContext js)> GetOrCreateConnectionAsync(CancellationToken ct)
    {
        if (_connection is not null && _js is not null)
            return (_connection, _js);

        await _initLock.WaitAsync(ct);
        try
        {
            if (_connection is not null && _js is not null)
                return (_connection, _js);

            var conn = new NatsConnection(_factory.BuildOpts());
            await conn.ConnectAsync();

            _logger.LogInformation("NATS connection established to {Servers}",
                string.Join(", ", _options.Servers));

            _connection = conn;
            _js = new NatsJSContext(conn);
            return (_connection, _js);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private string BuildSubject(RawChangeEvent evt)
    {
        var changeType = evt.ChangeType.ToString().ToLowerInvariant();

        if (!string.IsNullOrEmpty(evt.Context))
        {
            // cdc.{context}.{entityPath}.{changeType}
            // Normalize context: colons are invalid in NATS subjects
            var context = evt.Context.Replace(':', '-').ToLowerInvariant();
            return $"{_options.SubjectPrefix}.{context}.{evt.EntityPath}.{changeType}";
        }

        // Legacy: cdc.{sourceType}.{driverId}.{changeType}
        return $"{_options.SubjectPrefix}.{evt.SourceType}.{evt.DriverId}.{changeType}"
            .ToLowerInvariant();
    }

    private static Envelope BuildEnvelope(RawChangeEvent evt)
    {
        var envelope = new Envelope
        {
            EventId     = evt.EventId,
            DetectedAt  = Timestamp.FromDateTimeOffset(evt.DetectedAt),
            SourceType  = evt.SourceType,
            DriverId    = evt.DriverId,
            Context     = evt.Context,
            EntityPath  = evt.EntityPath,
            ChangeType  = evt.ChangeType.ToString(),
            SequenceNumber = evt.SequenceNumber
        };

        if (evt.SourceTimestamp.HasValue)
            envelope.SourceTimestamp = Timestamp.FromDateTimeOffset(evt.SourceTimestamp.Value);

        foreach (var (k, v) in evt.PrimaryKey)
            envelope.PrimaryKey[k] = v?.ToString() ?? "";

        foreach (var (k, v) in evt.Fields)
            envelope.Fields[k] = v?.ToString() ?? "";

        if (evt.PreviousFields is not null)
            foreach (var (k, v) in evt.PreviousFields)
                envelope.PreviousFields[k] = v?.ToString() ?? "";

        foreach (var (k, v) in evt.Metadata)
            envelope.Metadata[k] = v;

        return envelope;
    }

    private static NatsHeaders BuildHeaders(
        RawChangeEvent evt,
        IReadOnlyDictionary<string, string>? extra)
    {
        var headers = new NatsHeaders
        {
            ["eventId"]    = evt.EventId,
            ["driverId"]   = evt.DriverId,
            ["context"]    = evt.Context,
            ["sourceType"] = evt.SourceType,
            ["changeType"] = evt.ChangeType.ToString(),
            ["content-type"] = "application/x-protobuf"
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
