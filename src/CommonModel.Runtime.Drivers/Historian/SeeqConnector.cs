using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Historian;

public sealed class SeeqConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string BaseUrl { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public int PollIntervalSeconds { get; set; } = 30;
    public List<string> SignalIds { get; set; } = new();
}

public sealed class SeeqConnector : BaseConnector
{
    private readonly SeeqConnectorOptions _options;
    private readonly HttpClient _http = new();
    private string? _authToken;

    public SeeqConnector(IOptions<SeeqConnectorOptions> options, ILogger<SeeqConnector> logger)
        : base(logger) => _options = options.Value;

    public override string ConnectorId => _options.ConnectorId;
    public override string SourceType => "seeq";

    protected override async Task ConnectCoreAsync(CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new { username = _options.Username, password = _options.Password });
        var resp = await _http.PostAsync(
            $"{_options.BaseUrl.TrimEnd('/')}/api/v1/auth/login",
            new StringContent(body, Encoding.UTF8, "application/json"), ct);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        _authToken = json.TryGetProperty("token", out var t) ? t.GetString() : null;
        if (_authToken is not null)
            _http.DefaultRequestHeaders.Add("sq-auth", _authToken);
    }

    protected override Task DisconnectCoreAsync(CancellationToken ct)
    {
        _http.DefaultRequestHeaders.Remove("sq-auth");
        _authToken = null;
        return Task.CompletedTask;
    }

    protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);
        var watermarks = new Dictionary<string, DateTimeOffset>();

        while (!ct.IsCancellationRequested)
        {
            var end = DateTimeOffset.UtcNow;
            foreach (var signalId in _options.SignalIds)
            {
                if (!watermarks.TryGetValue(signalId, out var since))
                    since = end.AddHours(-1);

                var url = $"{_options.BaseUrl.TrimEnd('/')}/api/v1/signals/{signalId}/samples?start={since:O}&end={end:O}";
                var response = await _http.GetFromJsonAsync<JsonElement>(url, ct);

                if (response.TryGetProperty("samples", out var samples))
                {
                    foreach (var sample in samples.EnumerateArray())
                    {
                        var timestamp = sample.TryGetProperty("key", out var key)
                            ? DateTimeOffset.Parse(key.GetString()!)
                            : since;

                        yield return new DataChangeEvent
                        {
                            SourceType = SourceType,
                            ConnectorId = ConnectorId,
                            EntityPath = signalId,
                            ChangeType = ChangeType.Snapshot,
                            SourceTimestamp = timestamp,
                            PrimaryKey = new Dictionary<string, object?> { ["signalId"] = signalId },
                            Payload = new Dictionary<string, object?>
                            {
                                ["value"] = sample.TryGetProperty("value", out var v) ? v.GetRawText() : null,
                                ["timestamp"] = timestamp
                            }
                        };
                    }
                }

                watermarks[signalId] = end;
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
