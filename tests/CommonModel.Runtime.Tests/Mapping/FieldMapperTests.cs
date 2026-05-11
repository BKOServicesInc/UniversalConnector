using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Drivers.Generic.Mapping;

namespace CommonModel.Runtime.Tests.Mapping;

public class FieldMapperTests
{
    private readonly FieldMapper _sut = new();

    private static readonly EntityConfig DefaultEntity = new()
    {
        Name       = "public.assets",
        PrimaryKey = ["asset_id"]
    };

    // ── Pass-through (no rules) ───────────────────────────────────────────────

    [Fact]
    public void Apply_NoRules_PkColumnsGoToPrimaryKey()
    {
        var fields = F("asset_id", "A1", "name", "Widget");
        var (pk, payload, _) = _sut.Apply(fields, Empty, [], DefaultEntity);

        pk.Should().ContainKey("asset_id").WhoseValue.Should().Be("A1");
        payload.Should().ContainKey("name").WhoseValue.Should().Be("Widget");
        payload.Should().NotContainKey("asset_id");
    }

    [Fact]
    public void Apply_NoRulesNoEntityConfig_AllFieldsGoToPayload()
    {
        var fields = F("id", "1", "name", "Test");
        var (pk, payload, _) = _sut.Apply(fields, Empty, [], entityConfig: null);

        pk.Should().BeEmpty();
        payload.Should().HaveCount(2);
    }

    // ── Exclude rule ──────────────────────────────────────────────────────────

    [Fact]
    public void Apply_ExcludeRule_FieldOmittedFromAllDicts()
    {
        var rules  = Rules(new FieldMappingRule { Source = "secret", Exclude = true });
        var fields = F("asset_id", "A1", "secret", "hidden");
        var (pk, payload, _) = _sut.Apply(fields, Empty, rules, DefaultEntity);

        payload.Should().NotContainKey("secret");
        pk.Should().NotContainKey("secret");
    }

    // ── Target rename ─────────────────────────────────────────────────────────

    [Fact]
    public void Apply_TargetRule_RenamesField()
    {
        var rules  = Rules(new FieldMappingRule { Source = "asset_nm", Target = "name" });
        var fields = F("asset_id", "A1", "asset_nm", "Widget");
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, DefaultEntity);

        payload.Should().ContainKey("name").WhoseValue.Should().Be("Widget");
        payload.Should().NotContainKey("asset_nm");
    }

    // ── isKey rule ────────────────────────────────────────────────────────────

    [Fact]
    public void Apply_IsKeyRule_RoutesFieldToPrimaryKey()
    {
        var rules  = Rules(new FieldMappingRule { Source = "custom_pk", IsKey = true });
        var fields = F("asset_id", "A1", "custom_pk", "PK99");
        var (pk, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        pk.Should().ContainKey("custom_pk").WhoseValue.Should().Be("PK99");
        payload.Should().NotContainKey("custom_pk");
    }

    // ── Static value injection ────────────────────────────────────────────────

    [Fact]
    public void Apply_StaticValueRule_InjectsIntoPayload()
    {
        var rules  = Rules(new FieldMappingRule { Source = "source_system", StaticValue = "ERP" });
        var fields = F("asset_id", "A1");
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, DefaultEntity);

        payload.Should().ContainKey("source_system").WhoseValue.Should().Be("ERP");
    }

    [Fact]
    public void Apply_StaticValueWithIsKey_InjectsIntoPrimaryKey()
    {
        var rules  = Rules(new FieldMappingRule { Source = "pk_override", StaticValue = "STATIC", IsKey = true });
        var fields = F("asset_id", "A1");
        var (pk, _, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        pk.Should().ContainKey("pk_override").WhoseValue.Should().Be("STATIC");
    }

    // ── Type casting ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData("42",   "int",    42)]
    [InlineData("42",   "long",   42L)]
    [InlineData("3.14", "double", 3.14)]
    [InlineData("true", "bool",   true)]
    [InlineData("99",   "string", "99")]
    public void Apply_TypeCasting_CastsToCorrectType(string raw, string type, object expected)
    {
        var rules  = Rules(new FieldMappingRule { Source = "val", Type = type });
        var fields = F("val", raw);
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["val"].Should().Be(expected);
    }

    [Fact]
    public void Apply_TimestampCast_ReturnsDateTimeOffset()
    {
        var rules  = Rules(new FieldMappingRule { Source = "ts", Type = "timestamp" });
        var fields = F("ts", "2025-01-15T10:30:00Z");
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["ts"].Should().BeOfType<DateTimeOffset>();
    }

    [Fact]
    public void Apply_DateCast_ReturnsDateOnly()
    {
        var rules  = Rules(new FieldMappingRule { Source = "dt", Type = "date" });
        var fields = F("dt", "2025-01-15");
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["dt"].Should().BeOfType<DateOnly>();
    }

    [Fact]
    public void Apply_CastFailure_RetainsOriginalValue()
    {
        var rules  = Rules(new FieldMappingRule { Source = "val", Type = "int" });
        var fields = F("val", "not-a-number");
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["val"].Should().Be("not-a-number");
    }

    [Fact]
    public void Apply_NullValue_CastsToNull()
    {
        var rules  = Rules(new FieldMappingRule { Source = "val", Type = "int" });
        var fields = new Dictionary<string, object?> { ["val"] = null };
        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["val"].Should().BeNull();
    }

    // ── Previous fields ───────────────────────────────────────────────────────

    [Fact]
    public void Apply_PreviousFields_AppliesSameRules()
    {
        var rules    = Rules(new FieldMappingRule { Source = "secret", Exclude = true });
        var fields   = F("asset_id", "A1", "secret", "new");
        var previous = F("asset_id", "A1", "secret", "old");

        var (_, _, prevPayload) = _sut.Apply(fields, previous, rules, DefaultEntity);

        prevPayload.Should().NotContainKey("secret");
    }

    [Fact]
    public void Apply_PreviousFields_PkColumnsExcluded()
    {
        var fields   = F("asset_id", "A2", "name", "New");
        var previous = F("asset_id", "A1", "name", "Old");

        var (_, _, prevPayload) = _sut.Apply(fields, previous, [], DefaultEntity);

        prevPayload.Should().ContainKey("name").WhoseValue.Should().Be("Old");
        prevPayload.Should().NotContainKey("asset_id");
    }

    [Fact]
    public void Apply_EmptyPreviousFields_ReturnEmptyPreviousPayload()
    {
        var fields = F("asset_id", "A1", "name", "Test");
        var (_, _, prevPayload) = _sut.Apply(fields, Empty, [], DefaultEntity);

        prevPayload.Should().BeEmpty();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static IReadOnlyDictionary<string, object?> Empty =>
        new Dictionary<string, object?>();

    private static IReadOnlyDictionary<string, object?> F(params object?[] pairs)
    {
        var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < pairs.Length - 1; i += 2)
            d[pairs[i]!.ToString()!] = pairs[i + 1];
        return d;
    }

    private static IReadOnlyList<FieldMappingRule> Rules(params FieldMappingRule[] r) => r;
}
