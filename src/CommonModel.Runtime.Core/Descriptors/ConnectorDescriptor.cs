using YamlDotNet.Serialization;

namespace CommonModel.Runtime.Core.Descriptors;

public sealed class ConnectorDescriptor
{
    [YamlMember(Alias = "driverId")]
    public string DriverId { get; set; } = "";

    [YamlMember(Alias = "context")]
    public string Context { get; set; } = "";

    [YamlMember(Alias = "sourceType")]
    public string SourceType { get; set; } = "";

    [YamlMember(Alias = "description")]
    public string? Description { get; set; }

    [YamlMember(Alias = "enabled")]
    public bool Enabled { get; set; } = true;

    [YamlMember(Alias = "connection")]
    public ConnectionConfig Connection { get; set; } = new();

    [YamlMember(Alias = "changeDetection")]
    public ChangeDetectionConfig ChangeDetection { get; set; } = new();

    [YamlMember(Alias = "watch")]
    public WatchConfig Watch { get; set; } = new();

    [YamlMember(Alias = "fieldMapping")]
    public List<FieldMappingRule> FieldMapping { get; set; } = new();

    [YamlMember(Alias = "nats")]
    public NatsDescriptorConfig Nats { get; set; } = new();

    [YamlMember(Alias = "resilience")]
    public ResilienceConfig Resilience { get; set; } = new();
}

public sealed class ConnectionConfig
{
    [YamlMember(Alias = "connectionString")]
    public string? ConnectionString { get; set; }

    [YamlMember(Alias = "host")]
    public string? Host { get; set; }

    [YamlMember(Alias = "port")]
    public int? Port { get; set; }

    [YamlMember(Alias = "database")]
    public string? Database { get; set; }

    [YamlMember(Alias = "username")]
    public string? Username { get; set; }

    [YamlMember(Alias = "password")]
    public string? Password { get; set; }

    [YamlMember(Alias = "baseUrl")]
    public string? BaseUrl { get; set; }

    [YamlMember(Alias = "apiToken")]
    public string? ApiToken { get; set; }

    [YamlMember(Alias = "tenantId")]
    public string? TenantId { get; set; }

    [YamlMember(Alias = "clientId")]
    public string? ClientId { get; set; }

    [YamlMember(Alias = "clientSecret")]
    public string? ClientSecret { get; set; }

    [YamlMember(Alias = "httpPath")]
    public string? HttpPath { get; set; }

    [YamlMember(Alias = "uri")]
    public string? Uri { get; set; }

    [YamlMember(Alias = "verifySsl")]
    public bool VerifySsl { get; set; } = true;

    [YamlMember(Alias = "sslCertPath")]
    public string? SslCertPath { get; set; }

    [YamlMember(Alias = "sapClient")]
    public string? SapClient { get; set; }

    [YamlMember(Alias = "piServerName")]
    public string? PiServerName { get; set; }
}

public sealed class ChangeDetectionConfig
{
    [YamlMember(Alias = "mode")]
    public string Mode { get; set; } = "polling";

    [YamlMember(Alias = "pollIntervalSeconds")]
    public int PollIntervalSeconds { get; set; } = 30;

    [YamlMember(Alias = "watermarkColumn")]
    public string WatermarkColumn { get; set; } = "updated_at";

    [YamlMember(Alias = "lookbackDuration")]
    public string LookbackDuration { get; set; } = "PT1H";

    [YamlMember(Alias = "replicationSlot")]
    public string ReplicationSlot { get; set; } = "uc_slot";

    [YamlMember(Alias = "publication")]
    public string Publication { get; set; } = "uc_pub";

    [YamlMember(Alias = "startingVersion")]
    public long StartingVersion { get; set; } = -1;

    [YamlMember(Alias = "autoEnableChangeTracking")]
    public bool AutoEnableChangeTracking { get; set; } = true;
}

public sealed class WatchConfig
{
    [YamlMember(Alias = "autoDiscover")]
    public bool AutoDiscover { get; set; }

    [YamlMember(Alias = "entities")]
    public List<EntityConfig> Entities { get; set; } = new();
}

public sealed class EntityConfig
{
    [YamlMember(Alias = "name")]
    public string Name { get; set; } = "";

    [YamlMember(Alias = "primaryKey")]
    public List<string> PrimaryKey { get; set; } = new();

    [YamlMember(Alias = "filter")]
    public string? Filter { get; set; }

    [YamlMember(Alias = "changeDetectionOverride")]
    public string? ChangeDetectionOverride { get; set; }
}

public sealed class FieldMappingRule
{
    [YamlMember(Alias = "source")]
    public string Source { get; set; } = "";

    [YamlMember(Alias = "target")]
    public string? Target { get; set; }

    [YamlMember(Alias = "type")]
    public string? Type { get; set; }

    [YamlMember(Alias = "exclude")]
    public bool Exclude { get; set; }

    [YamlMember(Alias = "isKey")]
    public bool IsKey { get; set; }

    [YamlMember(Alias = "staticValue")]
    public object? StaticValue { get; set; }

    // Maps source field values to ontology concept IRIs.
    // Key = source value (as string); value = target IRI or concept identifier.
    [YamlMember(Alias = "conceptMap")]
    public Dictionary<string, string>? ConceptMap { get; set; }
}

public sealed class NatsDescriptorConfig
{
    /// <summary>
    /// Subject template following the AssetLink hierarchy: cdc.{context}.{aspectPath}.{eventType}
    /// </summary>
    [YamlMember(Alias = "subjectTemplate")]
    public string? SubjectTemplate { get; set; }

    [YamlMember(Alias = "subjectOverride")]
    public string? SubjectOverride { get; set; }

    [YamlMember(Alias = "serializationFormat")]
    public string SerializationFormat { get; set; } = "json";

    [YamlMember(Alias = "additionalHeaders")]
    public Dictionary<string, string> AdditionalHeaders { get; set; } = new();
}

public sealed class ResilienceConfig
{
    [YamlMember(Alias = "maxConsecutiveFailures")]
    public int MaxConsecutiveFailures { get; set; } = 5;

    [YamlMember(Alias = "retryDelaySeconds")]
    public int RetryDelaySeconds { get; set; } = 10;

    [YamlMember(Alias = "backoffMultiplier")]
    public double BackoffMultiplier { get; set; } = 1.5;

    [YamlMember(Alias = "maxRetryDelaySeconds")]
    public int MaxRetryDelaySeconds { get; set; } = 120;
}
