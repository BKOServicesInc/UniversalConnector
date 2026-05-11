using CommonModel.Runtime.Core.Descriptors;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

public sealed class DescriptorValidator
{
    private static readonly HashSet<string> KnownSourceTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "postgres", "sqlserver", "neo4j", "databricks", "seeq", "avevapi", "sharepoint", "sap", "mongodb"
    };

    private static readonly Dictionary<string, HashSet<string>> SupportedModes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["postgres"]   = new(StringComparer.OrdinalIgnoreCase) { "cdc", "polling" },
        ["sqlserver"]  = new(StringComparer.OrdinalIgnoreCase) { "cdc", "polling" },
        ["neo4j"]      = new(StringComparer.OrdinalIgnoreCase) { "polling" },
        ["databricks"] = new(StringComparer.OrdinalIgnoreCase) { "cdc", "polling" },
        ["seeq"]       = new(StringComparer.OrdinalIgnoreCase) { "polling" },
        ["avevapi"]    = new(StringComparer.OrdinalIgnoreCase) { "polling" },
        ["sharepoint"] = new(StringComparer.OrdinalIgnoreCase) { "delta" },
        ["sap"]        = new(StringComparer.OrdinalIgnoreCase) { "delta" },
        ["mongodb"]    = new(StringComparer.OrdinalIgnoreCase) { "cdc", "polling" }
    };

    public Core.Descriptors.DescriptorValidationResult Validate(ConnectorDescriptor d)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        if (string.IsNullOrWhiteSpace(d.DriverId))
            errors.Add("driverId is required");

        if (string.IsNullOrWhiteSpace(d.Context))
            errors.Add("context is required (e.g. ctx:MyProcess)");

        if (string.IsNullOrWhiteSpace(d.SourceType))
        {
            errors.Add("sourceType is required");
        }
        else if (!KnownSourceTypes.Contains(d.SourceType))
        {
            errors.Add($"sourceType '{d.SourceType}' not recognised");
        }
        else
        {
            var modes = SupportedModes[d.SourceType];
            if (!string.IsNullOrWhiteSpace(d.ChangeDetection.Mode) && !modes.Contains(d.ChangeDetection.Mode))
                errors.Add($"mode '{d.ChangeDetection.Mode}' not supported for '{d.SourceType}'");

            ValidateConnectionFields(d, errors);
        }

        if (!d.Watch.AutoDiscover && d.Watch.Entities.Count == 0)
            warnings.Add("no entities will be captured (watch.entities is empty and autoDiscover is false)");

        foreach (var rule in d.FieldMapping)
        {
            if (string.IsNullOrWhiteSpace(rule.Source))
                errors.Add("fieldMapping rule has empty source field name");
            if (rule.Exclude && rule.IsKey)
                errors.Add($"fieldMapping rule '{rule.Source}': exclude and isKey are mutually exclusive");
        }

        var cdcMode = string.Equals(d.ChangeDetection.Mode, "cdc", StringComparison.OrdinalIgnoreCase);
        if (cdcMode && string.Equals(d.SourceType, "postgres", StringComparison.OrdinalIgnoreCase)
                    && string.IsNullOrWhiteSpace(d.ChangeDetection.ReplicationSlot))
            warnings.Add("changeDetection.replicationSlot not set, will default to 'uc_slot'");

        if (d.Resilience.RetryDelaySeconds < 1)
            warnings.Add("resilience.retryDelaySeconds < 1; using 1s minimum");

        return errors.Count > 0
            ? Core.Descriptors.DescriptorValidationResult.Invalid(errors, warnings)
            : Core.Descriptors.DescriptorValidationResult.Valid(warnings);
    }

    private static void ValidateConnectionFields(ConnectorDescriptor d, List<string> errors)
    {
        var c = d.Connection;
        switch (d.SourceType.ToLowerInvariant())
        {
            case "postgres":
                if (string.IsNullOrWhiteSpace(c.ConnectionString) &&
                    (string.IsNullOrWhiteSpace(c.Host) || string.IsNullOrWhiteSpace(c.Database) || string.IsNullOrWhiteSpace(c.Username)))
                    errors.Add("postgres requires connection.host + connection.database + connection.username (or connection.connectionString)");
                break;
            case "sqlserver":
                if (string.IsNullOrWhiteSpace(c.ConnectionString) &&
                    (string.IsNullOrWhiteSpace(c.Host) || string.IsNullOrWhiteSpace(c.Database)))
                    errors.Add("sqlserver requires connection.host + connection.database (or connection.connectionString)");
                break;
            case "neo4j":
                if (string.IsNullOrWhiteSpace(c.Uri) || string.IsNullOrWhiteSpace(c.Username))
                    errors.Add("neo4j requires connection.uri + connection.username");
                break;
            case "databricks":
                if (string.IsNullOrWhiteSpace(c.Host) || string.IsNullOrWhiteSpace(c.HttpPath) || string.IsNullOrWhiteSpace(c.ApiToken))
                    errors.Add("databricks requires connection.host + connection.httpPath + connection.apiToken");
                break;
            case "seeq":
                if (string.IsNullOrWhiteSpace(c.BaseUrl) || string.IsNullOrWhiteSpace(c.Username))
                    errors.Add("seeq requires connection.baseUrl + connection.username");
                break;
            case "avevapi":
                if (string.IsNullOrWhiteSpace(c.BaseUrl) || string.IsNullOrWhiteSpace(c.PiServerName))
                    errors.Add("avevapi requires connection.baseUrl + connection.piServerName");
                break;
            case "sharepoint":
                if (string.IsNullOrWhiteSpace(c.TenantId) || string.IsNullOrWhiteSpace(c.ClientId) ||
                    string.IsNullOrWhiteSpace(c.ClientSecret) || string.IsNullOrWhiteSpace(c.BaseUrl))
                    errors.Add("sharepoint requires connection.tenantId + clientId + clientSecret + baseUrl");
                break;
            case "sap":
                if (string.IsNullOrWhiteSpace(c.BaseUrl) || string.IsNullOrWhiteSpace(c.Username))
                    errors.Add("sap requires connection.baseUrl + connection.username");
                break;
            case "mongodb":
                if (string.IsNullOrWhiteSpace(c.Uri) || string.IsNullOrWhiteSpace(c.Database))
                    errors.Add("mongodb requires connection.uri + connection.database");
                break;
        }
    }
}
