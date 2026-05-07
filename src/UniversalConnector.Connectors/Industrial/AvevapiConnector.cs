using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Connectors.Industrial;

public sealed class AvevapiConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string BaseUrl { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string PiServerName { get; set; } = "";
    public int PollIntervalSeconds { get; set; } = 30;
    public List<string> TagWebIds { get; set; } = new();
}

public sealed class AvevapiConnector : BaseConnector
{
    private readonly AvevapiConnectorOptions _options;
    private readonly HttpClient _http = new();

    public AvevapiConnector(IOptions<AvevapiConnectorOptions> options, ILogger<AvevapiConnector> logger)
        : base(logger) => _options = options.Value;

    public override string ConnectorId => _options.ConnectorId;
    public override string SourceType => "avevapi";

    protected override Task ConnectCoreAsync(CancellationToken ct)
    {
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_options.Username}:{_options.Password}"));
        _http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", credentials);
        return Task.CompletedTask;
    }

    protected override Task DisconnectCoreAsync(CancellationToken ct) => Task.CompletedTask;

    protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);
        var watermarks = new Dictionary<string, DateTimeOffset>();

        while (!ct.IsCancellationRequested)
        {
            foreach (var webId in _options.TagWebIds)
            {
                if (!watermarks.TryGetValue(webId, out var since))
                    since = DateTimeOffset.UtcNow.AddHours(-1);

                var url = $"{_options.BaseUrl.TrimEnd('/')}/piwebapi/streams/{webId}/recorded?startTime={since:O}";
                var response = await _http.GetFromJsonAsync<JsonElement>(url, ct);

                DateTimeOffset maxWm = since;
                if (response.TryGetProperty("Items", out var items))
                {
                    foreach (var item in items.EnumerateArray())
                    {
                        var ts = item.TryGetProperty("Timestamp", out var tsProp)
                            ? DateTimeOffset.Parse(tsProp.GetString()!)
                            : since;
                        if (ts > maxWm) maxWm = ts;

                        yield return new DataChangeEvent
                        {
                            SourceType = SourceType,
                            ConnectorId = ConnectorId,
                            EntityPath = webId,
                            ChangeType = ChangeType.Snapshot,
                            SourceTimestamp = ts,
                            PrimaryKey = new Dictionary<string, object?> { ["webId"] = webId },
                            Payload = new Dictionary<string, object?>
                            {
                                ["value"] = item.TryGetProperty("Value", out var v) ? v.GetRawText() : null,
                                ["timestamp"] = ts
                            }
                        };
                    }
                }

                watermarks[webId] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    public override ValueTask DisposeAsync()
    {
        _http.Dispose();
        return base.DisposeAsync();
    }
}
