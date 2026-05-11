using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Drivers.Generic.Engine;

namespace CommonModel.Runtime.Tests.Engine;

public class DescriptorValidatorTests
{
    private readonly DescriptorValidator _sut = new();

    // ── Valid descriptors ─────────────────────────────────────────────────────

    [Fact]
    public void Validate_ValidPostgresDescriptor_ReturnsValid()
    {
        var d = Postgres();
        _sut.Validate(d).IsValid.Should().BeTrue();
    }

    [Theory]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    [InlineData("neo4j")]
    [InlineData("mongodb")]
    [InlineData("databricks")]
    [InlineData("seeq")]
    [InlineData("avevapi")]
    [InlineData("sharepoint")]
    [InlineData("sap")]
    public void Validate_AllKnownSourceTypes_ReturnValid(string sourceType)
    {
        var d = FullDescriptor(sourceType);
        _sut.Validate(d).IsValid.Should().BeTrue();
    }

    // ── Required field errors ─────────────────────────────────────────────────

    [Fact]
    public void Validate_MissingConnectorId_ReturnsError()
    {
        var d = Postgres(); d.ConnectorId = "";
        var r = _sut.Validate(d);
        r.IsValid.Should().BeFalse();
        r.Errors.Should().Contain(e => e.Contains("connectorId"));
    }

    [Fact]
    public void Validate_MissingSourceType_ReturnsError()
    {
        var d = Postgres(); d.SourceType = "";
        var r = _sut.Validate(d);
        r.IsValid.Should().BeFalse();
        r.Errors.Should().Contain(e => e.Contains("sourceType"));
    }

    [Fact]
    public void Validate_UnknownSourceType_ReturnsError()
    {
        var d = Postgres(); d.SourceType = "oracle";
        var r = _sut.Validate(d);
        r.IsValid.Should().BeFalse();
        r.Errors.Should().Contain(e => e.Contains("oracle"));
    }

    // ── Mode validation ───────────────────────────────────────────────────────

    [Fact]
    public void Validate_UnsupportedMode_ReturnsError()
    {
        var d = Postgres(); d.ChangeDetection.Mode = "delta"; // postgres doesn't support delta
        var r = _sut.Validate(d);
        r.IsValid.Should().BeFalse();
        r.Errors.Should().Contain(e => e.Contains("delta"));
    }

    [Theory]
    [InlineData("neo4j",      "cdc")]
    [InlineData("seeq",       "cdc")]
    [InlineData("avevapi",    "cdc")]
    [InlineData("sharepoint", "cdc")]
    [InlineData("sharepoint", "polling")]
    [InlineData("sap",        "cdc")]
    [InlineData("sap",        "polling")]
    public void Validate_UnsupportedModePerSourceType_ReturnsError(string sourceType, string mode)
    {
        var d = FullDescriptor(sourceType);
        d.ChangeDetection.Mode = mode;
        _sut.Validate(d).IsValid.Should().BeFalse();
    }

    [Theory]
    [InlineData("postgres",   "cdc")]
    [InlineData("postgres",   "polling")]
    [InlineData("sqlserver",  "cdc")]
    [InlineData("sqlserver",  "polling")]
    [InlineData("mongodb",    "cdc")]
    [InlineData("mongodb",    "polling")]
    [InlineData("sharepoint", "delta")]
    [InlineData("sap",        "delta")]
    public void Validate_SupportedMode_ReturnsValid(string sourceType, string mode)
    {
        var d = FullDescriptor(sourceType);
        d.ChangeDetection.Mode = mode;
        _sut.Validate(d).IsValid.Should().BeTrue();
    }

    // ── Connection field errors ───────────────────────────────────────────────

    [Fact]
    public void Validate_PostgresMissingHostAndConnectionString_ReturnsError()
    {
        var d = Postgres();
        d.Connection.Host = null;
        d.Connection.ConnectionString = null;
        _sut.Validate(d).IsValid.Should().BeFalse();
    }

    [Fact]
    public void Validate_PostgresWithConnectionString_ReturnsValid()
    {
        var d = new ConnectorDescriptor
        {
            ConnectorId = "pg",
            SourceType  = "postgres",
            Connection  = new() { ConnectionString = "Host=localhost;Database=db;Username=u;" }
        };
        _sut.Validate(d).IsValid.Should().BeTrue();
    }

    [Fact]
    public void Validate_Neo4jMissingUri_ReturnsError()
    {
        var d = FullDescriptor("neo4j");
        d.Connection.Uri = null;
        _sut.Validate(d).IsValid.Should().BeFalse();
    }

    [Fact]
    public void Validate_MongoDbMissingDatabase_ReturnsError()
    {
        var d = FullDescriptor("mongodb");
        d.Connection.Database = null;
        _sut.Validate(d).IsValid.Should().BeFalse();
    }

    // ── FieldMapping rule errors ──────────────────────────────────────────────

    [Fact]
    public void Validate_FieldMappingRuleEmptySource_ReturnsError()
    {
        var d = Postgres();
        d.FieldMapping.Add(new FieldMappingRule { Source = "" });
        _sut.Validate(d).IsValid.Should().BeFalse();
    }

    [Fact]
    public void Validate_FieldMappingExcludeAndIsKey_ReturnsError()
    {
        var d = Postgres();
        d.FieldMapping.Add(new FieldMappingRule { Source = "id", Exclude = true, IsKey = true });
        _sut.Validate(d).IsValid.Should().BeFalse();
    }

    // ── Warnings ──────────────────────────────────────────────────────────────

    [Fact]
    public void Validate_NoEntitiesAndAutoDiscoverFalse_ReturnsWarning()
    {
        var d = Postgres();
        d.Watch.AutoDiscover = false;
        d.Watch.Entities.Clear();
        var r = _sut.Validate(d);
        r.IsValid.Should().BeTrue();
        r.Warnings.Should().Contain(w => w.Contains("no entities"));
    }

    [Fact]
    public void Validate_PostgresCdcMissingReplicationSlot_ReturnsWarning()
    {
        var d = Postgres();
        d.ChangeDetection.Mode = "cdc";
        d.ChangeDetection.ReplicationSlot = "";
        var r = _sut.Validate(d);
        r.IsValid.Should().BeTrue();
        r.Warnings.Should().Contain(w => w.Contains("replicationSlot"));
    }

    [Fact]
    public void Validate_RetryDelayLessThanOne_ReturnsWarning()
    {
        var d = Postgres();
        d.Resilience.RetryDelaySeconds = 0;
        var r = _sut.Validate(d);
        r.IsValid.Should().BeTrue();
        r.Warnings.Should().Contain(w => w.Contains("retryDelaySeconds"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ConnectorDescriptor Postgres() => new()
    {
        ConnectorId = "test-pg",
        SourceType  = "postgres",
        Connection  = new() { Host = "localhost", Database = "testdb", Username = "postgres" }
    };

    private static ConnectorDescriptor FullDescriptor(string sourceType) => sourceType.ToLower() switch
    {
        "postgres"   => new() { ConnectorId = "t", SourceType = "postgres",
                                Connection  = new() { Host = "h", Database = "d", Username = "u" } },
        "sqlserver"  => new() { ConnectorId = "t", SourceType = "sqlserver",
                                Connection  = new() { Host = "h", Database = "d" } },
        "neo4j"      => new() { ConnectorId = "t", SourceType = "neo4j",
                                Connection  = new() { Uri = "bolt://localhost", Username = "neo4j" } },
        "mongodb"    => new() { ConnectorId = "t", SourceType = "mongodb",
                                Connection  = new() { Uri = "mongodb://localhost", Database = "db" } },
        "databricks" => new() { ConnectorId = "t", SourceType = "databricks",
                                Connection  = new() { Host = "h", HttpPath = "/p", ApiToken = "tok" } },
        "seeq"       => new() { ConnectorId = "t", SourceType = "seeq",
                                Connection  = new() { BaseUrl = "https://seeq", Username = "u" } },
        "avevapi"    => new() { ConnectorId = "t", SourceType = "avevapi",
                                Connection  = new() { BaseUrl = "https://pi", PiServerName = "srv" } },
        "sharepoint" => new() { ConnectorId = "t", SourceType = "sharepoint",
                                Connection  = new() { TenantId = "tid", ClientId = "cid",
                                                      ClientSecret = "sec", BaseUrl = "https://sp" },
                                ChangeDetection = new() { Mode = "delta" } },
        "sap"        => new() { ConnectorId = "t", SourceType = "sap",
                                Connection  = new() { BaseUrl = "https://sap", Username = "u" },
                                ChangeDetection = new() { Mode = "delta" } },
        _            => throw new ArgumentException($"Unknown type: {sourceType}")
    };
}
