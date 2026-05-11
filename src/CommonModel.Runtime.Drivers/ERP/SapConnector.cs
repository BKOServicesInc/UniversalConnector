using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Composition;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.ERP;

public sealed class SapConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string BaseUrl { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string? SapClient { get; set; }
    public int PollIntervalSeconds { get; set; } = 60;
    public List<string> EntitySets { get; set; } = new();
}

[Export(typeof(ISourceDriver))]
public sealed class SapConnector : BaseConnector
{
    private readonly SapConnectorOptions _options;
    private readonly HttpClient _http = new();
    private readonly Dictionary<string, string> _deltaLinks = new();

    public SapConnector(IOptions<SapConnectorOptions> options, ILogger<SapConnector> logger)
        : base(logger) => _options = options.Value;

    public override string DriverId => _options.DriverId;
    public override string SourceType => "sap";

    protected override Task ConnectCoreAsync(CancellationToken ct)
    {
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_options.Username}:{_options.Password}"));
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        if (!string.IsNullOrWhiteSpace(_options.SapClient))
            _http.DefaultRequestHeaders.Add("sap-client", _options.SapClient);
        return Task.CompletedTask;
    }

    protected override Task DisconnectCoreAsync(CancellationToken ct) => Task.CompletedTask;

    protected override async IAsyncEnumerable<RawChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entitySet in _options.EntitySets)
            {
                string url = _deltaLinks.TryGetValue(entitySet, out var dl)
                    ? dl
                    : $"{_options.BaseUrl.TrimEnd('/')}/{entitySet}?$trackChanges";

                var response = await _http.GetFromJsonAsync<JsonElement>(url, ct);
                if (response.TryGetProperty("value", out var items))
                {
                    foreach (var item in items.EnumerateArray())
                    {
                        var fields = new Dictionary<string, object?>();
                        foreach (var prop in item.EnumerateObject())
                        {
                            fields[prop.Name] = prop.Value.ValueKind switch
                            {
                                JsonValueKind.String => prop.Value.GetString(),
                                JsonValueKind.Number => (object?)prop.Value.GetDecimal(),
                                JsonValueKind.True   => true,
                                JsonValueKind.False  => false,
                                JsonValueKind.Null   => null,
                                _                   => prop.Value.GetRawText()
                            };
                        }

                        yield return new RawChangeEvent
                        {
                            SourceType = SourceType,
                            DriverId = DriverId,
                            EntityPath = entitySet,
                            ChangeType = ChangeType.Update,
                            PrimaryKey = new Dictionary<string, object?>(),
                            Fields = fields
                        };
                    }
                }

                if (response.TryGetProperty("@odata.deltaLink", out var link))
                    _deltaLinks[entitySet] = link.GetString()!;
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
