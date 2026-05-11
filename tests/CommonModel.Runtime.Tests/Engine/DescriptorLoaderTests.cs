using CommonModel.Runtime.Drivers.Generic.Engine;

namespace CommonModel.Runtime.Tests.Engine;

public class DescriptorLoaderTests : IDisposable
{
    private readonly DescriptorLoader _sut = new(new DescriptorValidator());
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

    public DescriptorLoaderTests() => Directory.CreateDirectory(_tempDir);
    public void Dispose()          => Directory.Delete(_tempDir, recursive: true);

    // ── YAML parsing ──────────────────────────────────────────────────────────

    [Fact]
    public void LoadFromString_ValidYaml_ReturnsParsedDescriptor()
    {
        var yaml = """
            connectorId: my-connector
            sourceType: postgres
            connection:
              host: localhost
              database: testdb
              username: postgres
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeTrue();
        r.Descriptor!.ConnectorId.Should().Be("my-connector");
        r.Descriptor.SourceType.Should().Be("postgres");
    }

    [Fact]
    public void LoadFromString_ValidYaml_ParsesConnectionFields()
    {
        var yaml = """
            connectorId: pg
            sourceType: postgres
            connection:
              host: myhost
              port: 5433
              database: mydb
              username: myuser
              password: secret
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeTrue();
        r.Descriptor!.Connection.Host.Should().Be("myhost");
        r.Descriptor.Connection.Port.Should().Be(5433);
        r.Descriptor.Connection.Database.Should().Be("mydb");
    }

    [Fact]
    public void LoadFromString_ValidYaml_ParsesWatchEntities()
    {
        var yaml = """
            connectorId: pg
            sourceType: postgres
            connection:
              host: h
              database: d
              username: u
            watch:
              autoDiscover: false
              entities:
                - name: public.orders
                  primaryKey: [order_id]
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeTrue();
        r.Descriptor!.Watch.Entities.Should().HaveCount(1);
        r.Descriptor.Watch.Entities[0].Name.Should().Be("public.orders");
        r.Descriptor.Watch.Entities[0].PrimaryKey.Should().Contain("order_id");
    }

    [Fact]
    public void LoadFromString_ValidYaml_ParsesFieldMappingRules()
    {
        var yaml = """
            connectorId: pg
            sourceType: postgres
            connection:
              host: h
              database: d
              username: u
            fieldMapping:
              - source: internal_id
                target: id
                isKey: true
              - source: secret_col
                exclude: true
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeTrue();
        r.Descriptor!.FieldMapping.Should().HaveCount(2);
        r.Descriptor.FieldMapping[0].Source.Should().Be("internal_id");
        r.Descriptor.FieldMapping[0].Target.Should().Be("id");
        r.Descriptor.FieldMapping[0].IsKey.Should().BeTrue();
        r.Descriptor.FieldMapping[1].Exclude.Should().BeTrue();
    }

    // ── JSON parsing ──────────────────────────────────────────────────────────

    [Fact]
    public void LoadFromString_ValidJson_ReturnsParsedDescriptor()
    {
        var json = """
            {
              "connectorId": "json-connector",
              "sourceType": "postgres",
              "connection": { "host": "h", "database": "d", "username": "u" }
            }
            """;

        var r = _sut.LoadFromString(json, format: "json");

        r.Success.Should().BeTrue();
        r.Descriptor!.ConnectorId.Should().Be("json-connector");
    }

    // ── Environment variable interpolation ───────────────────────────────────

    [Fact]
    public void LoadFromString_WithEnvVar_InterpolatesValue()
    {
        Environment.SetEnvironmentVariable("TEST_DB_PASSWORD", "supersecret");

        var yaml = """
            connectorId: pg
            sourceType: postgres
            connection:
              host: h
              database: d
              username: u
              password: ${TEST_DB_PASSWORD}
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeTrue();
        r.Descriptor!.Connection.Password.Should().Be("supersecret");

        Environment.SetEnvironmentVariable("TEST_DB_PASSWORD", null);
    }

    [Fact]
    public void LoadFromString_MissingEnvVar_ReturnsFail()
    {
        Environment.SetEnvironmentVariable("MISSING_VAR_XYZ", null);

        var yaml = """
            connectorId: pg
            sourceType: postgres
            connection:
              host: ${MISSING_VAR_XYZ}
              database: d
              username: u
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeFalse();
        r.Error.Should().Contain("MISSING_VAR_XYZ");
    }

    // ── Validation integration ────────────────────────────────────────────────

    [Fact]
    public void LoadFromString_DescriptorFailsValidation_ReturnsFail()
    {
        var yaml = """
            connectorId: ""
            sourceType: postgres
            connection:
              host: h
              database: d
              username: u
            """;

        var r = _sut.LoadFromString(yaml);

        r.Success.Should().BeFalse();
        r.Error.Should().Contain("connectorId");
    }

    [Fact]
    public void LoadFromString_InvalidYaml_ReturnsFail()
    {
        var r = _sut.LoadFromString("{ this is: not: valid yaml {{{{");

        r.Success.Should().BeFalse();
        r.Error.Should().NotBeNullOrEmpty();
    }

    // ── File & directory loading ──────────────────────────────────────────────

    [Fact]
    public async Task LoadFromDirectoryAsync_NonExistentDirectory_ReturnsEmpty()
    {
        var r = await _sut.LoadFromDirectoryAsync("/does/not/exist", CancellationToken.None);

        r.Should().BeEmpty();
    }

    [Fact]
    public async Task LoadFromDirectoryAsync_DirectoryWithYamlFiles_LoadsAll()
    {
        var yaml1 = ValidYaml("connector-1");
        var yaml2 = ValidYaml("connector-2");
        await File.WriteAllTextAsync(Path.Combine(_tempDir, "c1.yaml"), yaml1);
        await File.WriteAllTextAsync(Path.Combine(_tempDir, "c2.yaml"), yaml2);

        var results = await _sut.LoadFromDirectoryAsync(_tempDir, CancellationToken.None);

        results.Should().HaveCount(2);
        results.Should().AllSatisfy(r => r.Success.Should().BeTrue());
    }

    [Fact]
    public async Task LoadFromDirectoryAsync_MixedValidAndInvalid_ReturnsAllResults()
    {
        await File.WriteAllTextAsync(Path.Combine(_tempDir, "good.yaml"), ValidYaml("good"));
        await File.WriteAllTextAsync(Path.Combine(_tempDir, "bad.yaml"), "connectorId: \"\"\nsourceType: postgres\n");

        var results = await _sut.LoadFromDirectoryAsync(_tempDir, CancellationToken.None);

        results.Should().HaveCount(2);
        results.Count(r => r.Success).Should().Be(1);
        results.Count(r => !r.Success).Should().Be(1);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string ValidYaml(string connectorId) => $"""
        connectorId: {connectorId}
        sourceType: postgres
        connection:
          host: localhost
          database: testdb
          username: postgres
        """;
}
