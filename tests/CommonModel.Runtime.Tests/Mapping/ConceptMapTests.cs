using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Drivers.Generic.Mapping;

namespace CommonModel.Runtime.Tests.Mapping;

public class ConceptMapTests
{
    private readonly FieldMapper _sut = new();

    private static readonly Dictionary<string, string> EquipMap = new()
    {
        ["PUMP"]  = "pid:Pump",
        ["VALVE"] = "pid:Valve",
        ["HX"]    = "pid:HeatExchanger"
    };

    [Fact]
    public void Apply_ConceptMapHit_SubstitutesIri()
    {
        var rules  = Rules(new FieldMappingRule { Source = "eq_type", ConceptMap = EquipMap });
        var fields = F("eq_type", "PUMP");

        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["eq_type"].Should().Be("pid:Pump");
    }

    [Fact]
    public void Apply_ConceptMapMiss_RetainsOriginalValue()
    {
        var rules  = Rules(new FieldMappingRule { Source = "eq_type", ConceptMap = EquipMap });
        var fields = F("eq_type", "UNKNOWN");

        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["eq_type"].Should().Be("UNKNOWN");
    }

    [Fact]
    public void Apply_ConceptMapWithRename_AppliesRenameAndSubstitution()
    {
        var rules  = Rules(new FieldMappingRule { Source = "eq_type", Target = "rdf_type", ConceptMap = EquipMap });
        var fields = F("eq_type", "VALVE");

        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload.Should().ContainKey("rdf_type").WhoseValue.Should().Be("pid:Valve");
        payload.Should().NotContainKey("eq_type");
    }

    [Fact]
    public void Apply_ConceptMapWithTypeCast_CastFirstThenLookup()
    {
        // Field value arrives as int 42; concept map key is "42"
        var map   = new Dictionary<string, string> { ["42"] = "ontology:State42" };
        var rules = Rules(new FieldMappingRule { Source = "state", Type = "string", ConceptMap = map });
        var fields = new Dictionary<string, object?> { ["state"] = 42 };

        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["state"].Should().Be("ontology:State42");
    }

    [Fact]
    public void Apply_ConceptMapNullValue_RetainsNull()
    {
        var rules  = Rules(new FieldMappingRule { Source = "eq_type", ConceptMap = EquipMap });
        var fields = new Dictionary<string, object?> { ["eq_type"] = null };

        var (_, payload, _) = _sut.Apply(fields, Empty, rules, entityConfig: null);

        payload["eq_type"].Should().BeNull();
    }

    [Fact]
    public void Apply_ConceptMapAppliedToPreviousFields()
    {
        var rules    = Rules(new FieldMappingRule { Source = "eq_type", ConceptMap = EquipMap });
        var fields   = F("eq_type", "PUMP");
        var previous = F("eq_type", "VALVE");

        var (_, _, prevPayload) = _sut.Apply(fields, previous, rules, entityConfig: null);

        prevPayload["eq_type"].Should().Be("pid:Valve");
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
