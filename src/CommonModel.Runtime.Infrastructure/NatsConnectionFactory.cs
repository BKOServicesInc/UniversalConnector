using Microsoft.Extensions.Options;
using NATS.Client.Core;
using CommonModel.Runtime.Core.Configuration;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsConnectionFactory
{
    private readonly NatsOptions _options;

    public NatsConnectionFactory(IOptions<NatsOptions> options) =>
        _options = options.Value;

    public NatsOpts BuildOpts()
    {
        var opts = NatsOpts.Default with
        {
            Url = string.Join(",", _options.Servers)
        };

        if (!string.IsNullOrWhiteSpace(_options.CredsFile))
            opts = opts with { AuthOpts = new NatsAuthOpts { CredsFile = _options.CredsFile } };

        return opts;
    }
}
