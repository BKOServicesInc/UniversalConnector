using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure.Wire;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsPublisher : INatsPublisher, IAsyncDisposable
{
    private readonly NatsOptions _options;
    private readonly NatsConnectionFactory _factory;
    private readonly ILogger<NatsPublisher> _logger;
    private NatsJSContext? _js;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    // Observability
    private static readonly ActivitySource ActivitySource =
        new("CommonModel.Runtime.Infrastructure", "1.0");
    private static readonly Meter Meter =
        new("CommonModel.Runtime.Infrastructure", "1.0");
    private static readonly Counter<long> PublishedCounter =
        Meter.CreateCounter<long>("cm.events.published", description: "Events successfully published to NATS");
    private static readonly Counter<long> DlqCounter =
        Meter.CreateCounter<long>("cm.events.dlq", description: "Events routed to the dead-letter queue");
    private static readonly Counter<long> RetryCounter =
        Meter.CreateCounter<long>("cm.events.publish_retries", description: "Individual publish retry attempts");

    // Delays between the 3 retry attempts; the 4th attempt (final) has no delay before it.
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(10)
    ];

    private static readonly JsonFormatter EnvelopeJsonFormatter =
        new(JsonFormatter.Settings.Default.WithIndentation("  "));

    // Simple circuit breaker: after CircuitBreakerThreshold consecutive failures the
    // circuit opens for CircuitHalfOpenWindow, during which events go straight to DLQ.
    private const int CircuitBreakerThreshold = 5;
    private static readonly TimeSpan CircuitHalfOpenWindow = TimeSpan.FromSeconds(30);
    private int _circuitFailures;
    private long _circuitOpenedAtTicks = DateTimeOffset.MinValue.UtcTicks;

    public NatsPublisher(
        IOptions<NatsOptions> options,
        NatsConnectionFactory factory,
        ILogger<NatsPublisher> logger)
    {
        _options = options.Value;
        _factory = factory;
        _logger  = logger;
    }

    public async Task PublishAsync(
        RawChangeEvent evt,
        string? subjectOverride = null,
        IReadOnlyDictionary<string, string>? additionalHeaders = null,
        CancellationToken ct = default)
    {
        using var activity = ActivitySource.StartActivity("nats.publish");
        var subject = subjectOverride ?? BuildSubject(evt);
        activity?.SetTag("nats.subject", subject);
        activity?.SetTag("event.id",     evt.EventId);
        activity?.SetTag("driver.id",    evt.DriverId);

        var (conn, js) = await GetOrCreateConnectionAsync(ct);
        var envelope   = BuildEnvelope(evt);
        var bytes      = envelope.ToByteArray();
        var headers    = BuildHeaders(evt, additionalHeaders);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("NATS payload (JSON):\n{Json}", EnvelopeJsonFormatter.Format(envelope));

        if (CircuitIsOpen)
        {
            _logger.LogWarning(
                "Circuit open — routing event {EventId} directly to DLQ", evt.EventId);
            activity?.SetTag("circuit", "open");
            await SendToDlqAsync(conn, subject, bytes, headers, evt.EventId, ct);
            return;
        }

        // Core NATS path (no JetStream stream required)
        if (!_options.UseJetStream)
        {
            await conn.PublishAsync(subject, bytes, headers: headers, cancellationToken: ct);
            RecordSuccess(evt, subject);
            return;
        }

        // 3 retried attempts, then one final attempt; 4 total.
        for (int attempt = 1; attempt <= RetryDelays.Length; attempt++)
        {
            if (await TryPublishAsync(js, subject, bytes, headers, ct))
            {
                RecordSuccess(evt, subject);
                return;
            }
            RetryCounter.Add(1, new TagList { { "driver.id", evt.DriverId } });
            _logger.LogWarning(
                "Publish attempt {Attempt}/{Total} failed for event {EventId}; retrying in {Delay}",
                attempt, RetryDelays.Length + 1, evt.EventId, RetryDelays[attempt - 1]);
            await Task.Delay(RetryDelays[attempt - 1], ct);
        }

        // Final (4th) attempt
        if (await TryPublishAsync(js, subject, bytes, headers, ct))
        {
            RecordSuccess(evt, subject);
            return;
        }

        // All attempts exhausted — open/advance the circuit and route to DLQ
        var failures = Interlocked.Increment(ref _circuitFailures);
        if (failures >= CircuitBreakerThreshold)
            Interlocked.Exchange(ref _circuitOpenedAtTicks, DateTimeOffset.UtcNow.UtcTicks);

        _logger.LogError(
            "All 4 publish attempts exhausted for event {EventId}; routing to DLQ", evt.EventId);
        activity?.SetTag("result", "dlq");
        await SendToDlqAsync(conn, subject, bytes, headers, evt.EventId, ct);
    }

    private static async Task<bool> TryPublishAsync(
        NatsJSContext js,
        string subject,
        byte[] bytes,
        NatsHeaders headers,
        CancellationToken ct)
    {
        try
        {
            var ack = await js.PublishAsync(subject, bytes, headers: headers, cancellationToken: ct);
            ack.EnsureSuccess();
            return true;
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }

    private async Task SendToDlqAsync(
        NatsConnection conn,
        string originalSubject,
        byte[] bytes,
        NatsHeaders headers,
        string eventId,
        CancellationToken ct)
    {
        var dlq = $"{_options.DlqSubjectPrefix}.{originalSubject}".ToLowerInvariant();
        DlqCounter.Add(1);
        try
        {
            await conn.PublishAsync(dlq, bytes, headers: headers, cancellationToken: ct);
            _logger.LogWarning("Event {EventId} routed to DLQ subject {DlqSubject}", eventId, dlq);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DLQ publish also failed for event {EventId} — event lost", eventId);
        }
    }

    private void RecordSuccess(RawChangeEvent evt, string subject)
    {
        Interlocked.Exchange(ref _circuitFailures, 0);
        PublishedCounter.Add(1, new TagList { { "driver.id", evt.DriverId } });

        _logger.LogInformation(
            "NATS ► {Subject}  [{ChangeType}]  driver={DriverId}  entity={EntityPath}  id={EventId}",
            subject, evt.ChangeType, evt.DriverId, evt.EntityPath, evt.EventId);

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            var pk     = string.Join(", ", evt.PrimaryKey.Select(k => $"{k.Key}={k.Value}"));
            var fields = string.Join(", ", evt.Fields.Select(k => $"{k.Key}={k.Value}"));
            _logger.LogDebug(
                "         pk=({PrimaryKey})  fields=({Fields})",
                pk, fields);
        }
    }

    private bool CircuitIsOpen
    {
        get
        {
            if (_circuitFailures < CircuitBreakerThreshold) return false;
            var elapsed = DateTimeOffset.UtcNow.UtcTicks -
                          Interlocked.Read(ref _circuitOpenedAtTicks);
            return elapsed < CircuitHalfOpenWindow.Ticks;
        }
    }

    private async ValueTask<(NatsConnection conn, NatsJSContext js)> GetOrCreateConnectionAsync(
        CancellationToken ct)
    {
        var conn = await _factory.GetSharedConnectionAsync(ct);
        if (_js is not null) return (conn, _js);

        await _initLock.WaitAsync(ct);
        try
        {
            if (_js is not null) return (conn, _js);
            _js = new NatsJSContext(conn);
        }
        finally
        {
            _initLock.Release();
        }

        return (conn, _js);
    }

    private string BuildSubject(RawChangeEvent evt)
    {
        if (!string.IsNullOrEmpty(evt.Context))
        {
            var context    = evt.Context.Replace(':', '-').ToLowerInvariant();
            var entityPath = SanitizeSubjectSegment(evt.EntityPath);
            return $"{_options.SubjectPrefix}.{context}.{entityPath}.{evt.ChangeType.ToString().ToLowerInvariant()}";
        }

        return $"{_options.SubjectPrefix}.{evt.SourceType}.{evt.DriverId}.{evt.ChangeType.ToString().ToLowerInvariant()}"
            .ToLowerInvariant();
    }

    // NATS subjects only accept [A-Za-z0-9_.-] with '.' as token separator.
    // PI AF entity paths like "elementTemplate/BKO Templat by Veda" or
    // "element/\\Aveva-Pi\BKO_LULU_DB\BKO Test1" contain '/', '\\', and spaces
    // — all illegal. We normalize path separators to '.' (preserving hierarchy
    // so consumers can wildcard-subscribe per level) and replace everything
    // else illegal with '_'.
    private static string SanitizeSubjectSegment(string raw)
    {
        if (string.IsNullOrEmpty(raw)) return raw;
        var sb = new StringBuilder(raw.Length);
        foreach (var c in raw)
        {
            if (c == '/' || c == '\\') sb.Append('.');
            else if (c == '.' || c == '-' || c == '_' || char.IsLetterOrDigit(c)) sb.Append(c);
            else sb.Append('_');
        }
        var s = sb.ToString();
        while (s.Contains("..")) s = s.Replace("..", ".");
        return s.Trim('.');
    }

    private static Envelope BuildEnvelope(RawChangeEvent evt)
    {
        var envelope = new Envelope
        {
            EventId        = evt.EventId,
            DetectedAt     = Timestamp.FromDateTimeOffset(evt.DetectedAt),
            SourceType     = evt.SourceType,
            DriverId       = evt.DriverId,
            Context        = evt.Context,
            EntityPath     = evt.EntityPath,
            ChangeType     = evt.ChangeType.ToString(),
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
            ["eventId"]      = evt.EventId,
            ["driverId"]     = evt.DriverId,
            ["context"]      = evt.Context,
            ["sourceType"]   = evt.SourceType,
            ["changeType"]   = evt.ChangeType.ToString(),
            ["content-type"] = "application/x-protobuf"
        };

        if (extra is not null)
            foreach (var (k, v) in extra)
                headers[k] = v;

        return headers;
    }

    public ValueTask DisposeAsync()
    {
        _initLock.Dispose();
        return ValueTask.CompletedTask;
    }
}
