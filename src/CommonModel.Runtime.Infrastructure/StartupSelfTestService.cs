using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using CommonModel.Runtime.Core.Configuration;

namespace CommonModel.Runtime.Infrastructure;

public sealed class StartupSelfTestService : IHostedService
{
    private readonly NatsConnectionFactory _factory;
    private readonly NatsOptions _options;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<StartupSelfTestService> _logger;

    public StartupSelfTestService(
        NatsConnectionFactory factory,
        IOptions<NatsOptions> options,
        IHostApplicationLifetime lifetime,
        ILogger<StartupSelfTestService> logger)
    {
        _factory  = factory;
        _options  = options.Value;
        _lifetime = lifetime;
        _logger   = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("StartupSelfTestService: running pre-flight checks");

        var results = new List<(string check, bool passed, string? detail)>();

        CheckCredsFile(results);
        await CheckNatsAsync(results, cancellationToken);

        var passed = results.Count(r => r.passed);
        foreach (var (check, ok, detail) in results)
        {
            if (ok)
                _logger.LogInformation("  [PASS] {Check}", check);
            else
                _logger.LogWarning("  [FAIL] {Check} — {Detail}", check, detail);
        }

        _logger.LogInformation("StartupSelfTestService: {Passed}/{Total} checks passed",
            passed, results.Count);

        if (_options.StopOnCriticalFailure && results.Any(r => !r.passed))
        {
            _logger.LogCritical(
                "StopOnCriticalFailure=true and {Failed} check(s) failed — stopping application",
                results.Count(r => !r.passed));
            _lifetime.StopApplication();
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private void CheckCredsFile(List<(string, bool, string?)> results)
    {
        if (string.IsNullOrWhiteSpace(_options.CredsFile)) return;

        var exists = File.Exists(_options.CredsFile);
        results.Add(("creds-file", exists,
            exists ? null : $"file not found: {_options.CredsFile}"));
    }

    private async Task CheckNatsAsync(
        List<(string, bool, string?)> results,
        CancellationToken cancellationToken)
    {
        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        connectCts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            var conn = await _factory.GetSharedConnectionAsync(connectCts.Token);
            await conn.PingAsync(connectCts.Token);
            results.Add(("nats-connect", true, null));
        }
        catch (Exception ex)
        {
            results.Add(("nats-connect", false, ex.Message));
            return;
        }

        if (!_options.UseJetStream) return;

        using var jsCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        jsCts.CancelAfter(TimeSpan.FromSeconds(5));

        NatsJSContext? js = null;
        try
        {
            var conn = await _factory.GetSharedConnectionAsync(jsCts.Token);
            js = new NatsJSContext(conn);
            await js.GetAccountInfoAsync(jsCts.Token);
            results.Add(("jetstream", true, null));
        }
        catch (Exception ex)
        {
            results.Add(("jetstream", false, ex.Message));
            return;
        }

        await EnsureStreamsAsync(js, cancellationToken);
    }

    private async Task EnsureStreamsAsync(NatsJSContext js, CancellationToken ct)
    {
        // One stream covers all cdc.> subjects: events, lifecycle, commands.
        // Idempotent — skipped if the stream already exists.
        const string streamName = "CDC";
        const string subjectFilter = "cdc.>";

        try
        {
            await js.GetStreamAsync(streamName, cancellationToken: ct);
            _logger.LogInformation("JetStream stream '{Stream}' already exists", streamName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            try
            {
                await js.CreateStreamAsync(new StreamConfig
                {
                    Name        = streamName,
                    Subjects    = [subjectFilter],
                    Storage     = StreamConfigStorage.File,
                    Retention   = StreamConfigRetention.Limits,
                    MaxAge      = TimeSpan.FromDays(1),
                    NumReplicas = 1,
                }, ct);
                _logger.LogInformation(
                    "JetStream stream '{Stream}' created (subjects: {Subjects})",
                    streamName, subjectFilter);
            }
            catch (Exception createEx)
            {
                _logger.LogWarning(createEx,
                    "Could not create JetStream stream '{Stream}' — events will use core NATS",
                    streamName);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not check JetStream stream '{Stream}'", streamName);
        }
    }
}
