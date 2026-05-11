using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Collaboration;

public sealed class SharePointConnectorOptions : Core.Configuration.ConnectorOptions
{
    public string TenantId { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";
    public string SiteBaseUrl { get; set; } = "";
    public int PollIntervalSeconds { get; set; } = 60;
    public List<string> ListNames { get; set; } = new();
}

public sealed class SharePointConnector : BaseConnector
{
    private readonly SharePointConnectorOptions _options;
    private readonly HttpClient _http = new();
    private readonly Dictionary<string, string> _deltaLinks = new();

    public SharePointConnector(IOptions<SharePointConnectorOptions> options, ILogger<SharePointConnector> logger)
        : base(logger) => _options = options.Value;

    public override string ConnectorId => _options.ConnectorId;
    public override string SourceType => "sharepoint";

    protected override async Task ConnectCoreAsync(CancellationToken ct)
    {
        var tokenUrl = $"https://login.microsoftonline.com/{_options.TenantId}/oauth2/v2.0/token";
        var body = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _options.ClientId,
            ["client_secret"] = _options.ClientSecret,
            ["scope"] = "https://graph.microsoft.com/.default"
        });
        var resp = await _http.PostAsync(tokenUrl, body, ct);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        var token = json.GetProperty("access_token").GetString();
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }

    protected override Task DisconnectCoreAsync(CancellationToken ct) => Task.CompletedTask;

    protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_options.PollIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            foreach (var listName in _options.ListNames)
            {
                string url = _deltaLinks.TryGetValue(listName, out var dl)
                    ? dl
                    : $"{_options.SiteBaseUrl.TrimEnd('/')}/lists/{listName}/items/delta";

                var response = await _http.GetFromJsonAsync<JsonElement>(url, ct);
                if (response.TryGetProperty("value", out var items))
                {
                    foreach (var item in items.EnumerateArray())
                    {
                        var deleted = item.TryGetProperty("deleted", out _);
                        var fields = new Dictionary<string, object?>();
                        foreach (var prop in item.EnumerateObject())
                            fields[prop.Name] = prop.Value.GetRawText();

                        yield return new DataChangeEvent
                        {
                            SourceType = SourceType,
                            ConnectorId = ConnectorId,
                            EntityPath = listName,
                            ChangeType = deleted ? ChangeType.Delete : ChangeType.Update,
                            PrimaryKey = new Dictionary<string, object?> { ["id"] = fields.GetValueOrDefault("id") },
                            Payload = fields
                        };
                    }
                }

                if (response.TryGetProperty("@odata.deltaLink", out var link))
                    _deltaLinks[listName] = link.GetString()!;

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
