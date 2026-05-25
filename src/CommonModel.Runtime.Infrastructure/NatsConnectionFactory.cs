using Microsoft.Extensions.Options;
using NATS.Client.Core;
using CommonModel.Runtime.Core.Configuration;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsConnectionFactory : IAsyncDisposable
{
    private readonly NatsOptions _options;
    private NatsConnection? _shared;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public NatsConnectionFactory(IOptions<NatsOptions> options) =>
        _options = options.Value;

    public NatsOpts BuildOpts()
    {
        // Single-URL config (Nats:Url) takes precedence; falls back to the
        // multi-URL Servers[] form for clustered setups.
        var url = !string.IsNullOrWhiteSpace(_options.Url)
            ? _options.Url
            : string.Join(",", _options.Servers);

        var opts = NatsOpts.Default with { Url = url };

        if (!string.IsNullOrWhiteSpace(_options.CredsFile))
            opts = opts with { AuthOpts = new NatsAuthOpts { CredsFile = _options.CredsFile } };

        return opts;
    }

    public async Task<NatsConnection> GetSharedConnectionAsync(CancellationToken ct = default)
    {
        if (_shared is not null) return _shared;

        await _lock.WaitAsync(ct);
        try
        {
            if (_shared is not null) return _shared;

            var conn = new NatsConnection(BuildOpts());
            await conn.ConnectAsync();
            _shared = conn;
        }
        finally
        {
            _lock.Release();
        }

        return _shared;
    }

    public async ValueTask DisposeAsync()
    {
        _lock.Dispose();
        if (_shared is not null)
            await _shared.DisposeAsync();
    }
}
