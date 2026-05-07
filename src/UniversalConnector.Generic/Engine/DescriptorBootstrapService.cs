using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using UniversalConnector.Core.Descriptors;
using UniversalConnector.Generic.Configuration;

namespace UniversalConnector.Generic.Engine;

public sealed class DescriptorBootstrapService : IHostedService
{
    private readonly IDescriptorLoader _loader;
    private readonly DescriptorStore _store;
    private readonly GenericConnectorOptions _options;
    private readonly ILogger<DescriptorBootstrapService> _logger;

    public DescriptorBootstrapService(
        IDescriptorLoader loader,
        DescriptorStore store,
        IOptions<GenericConnectorOptions> options,
        ILogger<DescriptorBootstrapService> logger)
    {
        _loader = loader;
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var dir = _options.DescriptorDirectory;
        _logger.LogInformation("Loading descriptors from '{Directory}'", dir);

        var results = await _loader.LoadFromDirectoryAsync(dir, cancellationToken);

        int loaded = 0, failed = 0;
        foreach (var result in results)
        {
            if (result.Success && result.Descriptor is not null)
            {
                _store.Register(result.Descriptor);
                loaded++;

                foreach (var w in result.Warnings)
                    _logger.LogWarning("[{ConnectorId}] {Warning}", result.Descriptor.ConnectorId, w);

                _logger.LogInformation("Loaded descriptor '{ConnectorId}' ({SourceType})",
                    result.Descriptor.ConnectorId, result.Descriptor.SourceType);
            }
            else
            {
                failed++;
                var fileLabel = string.IsNullOrWhiteSpace(result.FilePath) ? "(unknown file)" : result.FilePath;
                _logger.LogError("Failed to load descriptor '{File}': {Error}", fileLabel, result.Error);

                if (_options.FailOnDescriptorError)
                    throw new InvalidOperationException(
                        $"Descriptor load failed for '{fileLabel}': {result.Error}");
            }
        }

        _logger.LogInformation("Descriptor bootstrap complete: {Loaded} loaded, {Failed} failed", loaded, failed);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
