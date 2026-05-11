using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Xml;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class HttpRestAdapter : BaseProtocolAdapter
{
    private readonly ILogger<HttpRestAdapter> _logger;
    private readonly string _sourceType;
    private HttpClient? _httpClient;
    private readonly Dictionary<string, string> _deltaLinks = new();
    private readonly Dictionary<string, DateTimeOffset> _watermarks = new();
    private string? _authToken;

    public HttpRestAdapter(ILoggerFactory loggerFactory, string sourceType)
    {
        _logger = loggerFactory.CreateLogger<HttpRestAdapter>();
        _sourceType = sourceType;
    }

    public override string SourceType => _sourceType;

    protected override async Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        _httpClient = new HttpClient();

        if (!descriptor.Connection.VerifySsl)
        {
            var handler = new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true };
            _httpClient = new HttpClient(handler);
        }

        switch (_sourceType.ToLowerInvariant())
        {
            case "sharepoint":
                await AuthenticateSharePoint(descriptor, ct);
                break;
            case "seeq":
                await AuthenticateSeeq(descriptor, ct);
                break;
        }
    }

    protected override Task CloseCoreAsync(CancellationToken ct)
    {
        _httpClient?.Dispose();
        _httpClient = null;
        _authToken = null;
        return Task.CompletedTask;
    }

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        switch (_sourceType.ToLowerInvariant())
        {
            case "sharepoint":
                await foreach (var r in StreamSharePointAsync(descriptor, ct)) yield return r;
                break;
            case "sap":
                await foreach (var r in StreamSapAsync(descriptor, ct)) yield return r;
                break;
            case "seeq":
                await foreach (var r in StreamSeeqAsync(descriptor, ct)) yield return r;
                break;
            case "avevapi":
                await foreach (var r in StreamAvevaAsync(descriptor, ct)) yield return r;
                break;
        }
    }

    // ── SharePoint (Graph delta) ────────────────────────────────────────────

    private async IAsyncEnumerable<RawChangeRecord> StreamSharePointAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var baseUrl = descriptor.Connection.BaseUrl!.TrimEnd('/');
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in descriptor.Watch.Entities)
            {
                string url;
                if (_deltaLinks.TryGetValue(entity.Name, out var deltaLink))
                    url = deltaLink;
                else
                    url = $"{baseUrl}/lists/{entity.Name}/items/delta";

                var response = await _httpClient!.GetFromJsonAsync<JsonElement>(url, ct);
                var items = response.TryGetProperty("value", out var val) ? val.EnumerateArray() : default;

                foreach (var item in items)
                {
                    var fields = ParseJsonFields(item);
                    var deleted = item.TryGetProperty("deleted", out _);

                    yield return new RawChangeRecord
                    {
                        EntityPath = entity.Name,
                        ChangeType = deleted ? ChangeType.Delete : ChangeType.Update,
                        Fields = fields,
                        AdapterMetadata = new Dictionary<string, string> { ["source"] = "sharepoint" }
                    };
                }

                if (response.TryGetProperty("@odata.deltaLink", out var link))
                    _deltaLinks[entity.Name] = link.GetString()!;
                else if (response.TryGetProperty("@odata.nextLink", out var next))
                    _deltaLinks[entity.Name] = next.GetString()!;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task AuthenticateSharePoint(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var c = descriptor.Connection;
        var tokenUrl = $"https://login.microsoftonline.com/{c.TenantId}/oauth2/v2.0/token";
        var body = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"]    = "client_credentials",
            ["client_id"]     = c.ClientId ?? "",
            ["client_secret"] = c.ClientSecret ?? "",
            ["scope"]         = "https://graph.microsoft.com/.default"
        });

        var resp = await _httpClient!.PostAsync(tokenUrl, body, ct);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        _authToken = json.GetProperty("access_token").GetString();
        _httpClient.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", _authToken);
    }

    // ── SAP OData ──────────────────────────────────────────────────────────

    private async IAsyncEnumerable<RawChangeRecord> StreamSapAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var baseUrl = descriptor.Connection.BaseUrl!.TrimEnd('/');
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var c = descriptor.Connection;

        var credentials = Convert.ToBase64String(
            Encoding.UTF8.GetBytes($"{c.Username}:{c.Password}"));
        _httpClient!.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic", credentials);

        if (!string.IsNullOrWhiteSpace(c.SapClient))
            _httpClient.DefaultRequestHeaders.Add("sap-client", c.SapClient);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in descriptor.Watch.Entities)
            {
                string url;
                if (_deltaLinks.TryGetValue(entity.Name, out var deltaLink))
                    url = deltaLink;
                else
                    url = $"{baseUrl}/{entity.Name}?$trackChanges";

                var response = await _httpClient!.GetFromJsonAsync<JsonElement>(url, ct);
                var items = response.TryGetProperty("value", out var val) ? val.EnumerateArray() : default;

                foreach (var item in items)
                {
                    var fields = ParseJsonFields(item);
                    yield return new RawChangeRecord
                    {
                        EntityPath = entity.Name,
                        ChangeType = ChangeType.Update,
                        Fields = fields,
                        AdapterMetadata = new Dictionary<string, string> { ["source"] = "sap" }
                    };
                }

                if (response.TryGetProperty("@odata.deltaLink", out var link))
                    _deltaLinks[entity.Name] = link.GetString()!;
            }

            await Task.Delay(interval, ct);
        }
    }

    // ── Seeq ──────────────────────────────────────────────────────────────

    private async IAsyncEnumerable<RawChangeRecord> StreamSeeqAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var baseUrl = descriptor.Connection.BaseUrl!.TrimEnd('/');
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in descriptor.Watch.Entities)
            {
                if (!_watermarks.TryGetValue(entity.Name, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                var end = DateTimeOffset.UtcNow;
                var url = $"{baseUrl}/api/v1/signals/{entity.Name}/samples?start={since:O}&end={end:O}";
                var response = await _httpClient!.GetFromJsonAsync<JsonElement>(url, ct);

                if (response.TryGetProperty("samples", out var samples))
                {
                    foreach (var sample in samples.EnumerateArray())
                    {
                        var timestamp = sample.TryGetProperty("key", out var key)
                            ? DateTimeOffset.Parse(key.GetString()!)
                            : since;

                        var fields = new Dictionary<string, object?>
                        {
                            ["signalId"] = entity.Name,
                            ["value"]    = sample.TryGetProperty("value", out var v) ? (object?)v.GetRawText() : null,
                            ["timestamp"] = timestamp
                        };

                        yield return new RawChangeRecord
                        {
                            EntityPath = entity.Name,
                            ChangeType = ChangeType.Snapshot,
                            SourceTimestamp = timestamp,
                            Fields = fields,
                            AdapterMetadata = new Dictionary<string, string> { ["source"] = "seeq" }
                        };
                    }
                }

                _watermarks[entity.Name] = end;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task AuthenticateSeeq(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var baseUrl = descriptor.Connection.BaseUrl!.TrimEnd('/');
        var loginBody = JsonSerializer.Serialize(new
        {
            username = descriptor.Connection.Username,
            password = descriptor.Connection.Password
        });

        var resp = await _httpClient!.PostAsync(
            $"{baseUrl}/api/v1/auth/login",
            new StringContent(loginBody, Encoding.UTF8, "application/json"),
            ct);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        _authToken = json.TryGetProperty("token", out var t) ? t.GetString() : null;
        if (_authToken is not null)
            _httpClient.DefaultRequestHeaders.Add("sq-auth", _authToken);
    }

    // ── AVEVA PI ──────────────────────────────────────────────────────────

    private async IAsyncEnumerable<RawChangeRecord> StreamAvevaAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var baseUrl = descriptor.Connection.BaseUrl!.TrimEnd('/');
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var c = descriptor.Connection;

        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{c.Username}:{c.Password}"));
        _httpClient!.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in descriptor.Watch.Entities)
            {
                if (!_watermarks.TryGetValue(entity.Name, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                var url = $"{baseUrl}/piwebapi/streams/{entity.Name}/recorded?startTime={since:O}";
                var response = await _httpClient!.GetFromJsonAsync<JsonElement>(url, ct);

                if (response.TryGetProperty("Items", out var items))
                {
                    DateTimeOffset maxWm = since;
                    foreach (var item in items.EnumerateArray())
                    {
                        var timestamp = item.TryGetProperty("Timestamp", out var ts)
                            ? DateTimeOffset.Parse(ts.GetString()!)
                            : since;
                        if (timestamp > maxWm) maxWm = timestamp;

                        var fields = new Dictionary<string, object?>
                        {
                            ["webId"]     = entity.Name,
                            ["value"]     = item.TryGetProperty("Value", out var v) ? (object?)v.GetRawText() : null,
                            ["timestamp"] = timestamp
                        };

                        yield return new RawChangeRecord
                        {
                            EntityPath = entity.Name,
                            ChangeType = ChangeType.Snapshot,
                            SourceTimestamp = timestamp,
                            Fields = fields,
                            AdapterMetadata = new Dictionary<string, string> { ["source"] = "avevapi" }
                        };
                    }

                    _watermarks[entity.Name] = maxWm;
                }
            }

            await Task.Delay(interval, ct);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static Dictionary<string, object?> ParseJsonFields(JsonElement element)
    {
        var fields = new Dictionary<string, object?>();
        foreach (var prop in element.EnumerateObject())
        {
            fields[prop.Name] = prop.Value.ValueKind switch
            {
                JsonValueKind.String  => prop.Value.GetString(),
                JsonValueKind.Number  => prop.Value.TryGetInt64(out var l) ? (object)l : prop.Value.GetDouble(),
                JsonValueKind.True    => true,
                JsonValueKind.False   => false,
                JsonValueKind.Null    => null,
                _                    => prop.Value.GetRawText()
            };
        }
        return fields;
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor) => Array.Empty<string>();

    public override ValueTask DisposeAsync()
    {
        _httpClient?.Dispose();
        return base.DisposeAsync();
    }
}
