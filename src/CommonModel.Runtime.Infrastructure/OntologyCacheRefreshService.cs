using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;


namespace CommonModel.Runtime.Infrastructure;

public sealed class OntologyCacheRefreshService : BackgroundService
{
    private readonly IOntologyCache _cache;
    private readonly NatsConnectionFactory _factory;
    private readonly OntologyCacheOptions _cacheOptions;
    private readonly ILogger<OntologyCacheRefreshService> _logger;

    public OntologyCacheRefreshService(
        IOntologyCache cache,
        NatsConnectionFactory factory,
        IOptions<OntologyCacheOptions> cacheOptions,
        ILogger<OntologyCacheRefreshService> logger)
    {
        _cache        = cache;
        _factory      = factory;
        _cacheOptions = cacheOptions.Value;
        _logger       = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_cacheOptions.LoadOnStartup)
        {
            try
            {
                await _cache.RefreshAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Initial ontology cache load failed; continuing with empty cache");
            }
        }

        var subject = _cacheOptions.RefreshSubject;
        if (string.IsNullOrWhiteSpace(subject))
            return;

        await using var conn = new NatsConnection(_factory.BuildOpts());

        try
        {
            await conn.ConnectAsync();
            _logger.LogInformation("OntologyCacheRefreshService: listening on '{Subject}'", subject);

            await foreach (var _ in conn.SubscribeAsync<byte[]>(subject, cancellationToken: stoppingToken))
            {
                _logger.LogInformation("Ontology refresh signal received on '{Subject}'", subject);
                try
                {
                    await _cache.RefreshAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Ontology refresh failed after signal");
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // normal shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OntologyCacheRefreshService terminated unexpectedly");
        }
    }
}
