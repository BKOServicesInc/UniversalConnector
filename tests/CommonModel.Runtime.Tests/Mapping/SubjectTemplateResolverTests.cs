using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Drivers.Generic.Mapping;

namespace CommonModel.Runtime.Tests.Mapping;

public class SubjectTemplateResolverTests
{
    private static RawChangeEvent MakeEvent(
        string context    = "ctx:PID",
        string entityPath = "public.assets",
        ChangeType change = ChangeType.Insert,
        string driverId   = "pg-assets",
        string sourceType = "postgres") => new()
    {
        SourceType  = sourceType,
        DriverId    = driverId,
        Context     = context,
        EntityPath  = entityPath,
        ChangeType  = change
    };

    [Fact]
    public void Resolve_AllTokens_SubstitutesCorrectly()
    {
        var evt = MakeEvent();
        var result = SubjectTemplateResolver.Resolve(
            "cdc.{context}.{entityPath}.{changeType}", evt);

        result.Should().Be("cdc.ctx-pid.public.assets.insert");
    }

    [Fact]
    public void Resolve_ContextColon_NormalizedToDash()
    {
        var evt = MakeEvent(context: "ctx:AF-Process");
        var result = SubjectTemplateResolver.Resolve("{context}", evt);

        result.Should().Be("ctx-af-process");
    }

    [Fact]
    public void Resolve_DriverIdToken_Substituted()
    {
        var evt = MakeEvent();
        var result = SubjectTemplateResolver.Resolve("cdc.{driverId}.events", evt);

        result.Should().Be("cdc.pg-assets.events");
    }

    [Fact]
    public void Resolve_SourceTypeToken_Substituted()
    {
        var evt = MakeEvent();
        var result = SubjectTemplateResolver.Resolve("raw.{sourceType}.{changeType}", evt);

        result.Should().Be("raw.postgres.insert");
    }

    [Fact]
    public void Resolve_NoTokens_ReturnsTemplateUnchanged()
    {
        var evt = MakeEvent();
        var result = SubjectTemplateResolver.Resolve("static.subject.name", evt);

        result.Should().Be("static.subject.name");
    }

    [Theory]
    [InlineData(ChangeType.Insert,       "insert")]
    [InlineData(ChangeType.Update,       "update")]
    [InlineData(ChangeType.Delete,       "delete")]
    [InlineData(ChangeType.Snapshot,     "snapshot")]
    [InlineData(ChangeType.Heartbeat,    "heartbeat")]
    public void Resolve_ChangeTypeToken_IsLowercase(ChangeType ct, string expected)
    {
        var evt = MakeEvent(change: ct);
        var result = SubjectTemplateResolver.Resolve("{changeType}", evt);

        result.Should().Be(expected);
    }

    [Fact]
    public void Resolve_ExampleYamlTemplate_ProducesExpectedSubject()
    {
        // Matches the template in example-postgres.yaml
        var evt = MakeEvent(context: "ctx:PID", entityPath: "public.assets", change: ChangeType.Snapshot);
        var result = SubjectTemplateResolver.Resolve("cdc.{context}.assets.{changeType}", evt);

        result.Should().Be("cdc.ctx-pid.assets.snapshot");
    }
}
