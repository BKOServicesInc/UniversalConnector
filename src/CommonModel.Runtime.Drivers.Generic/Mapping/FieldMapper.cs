using System.Globalization;
using CommonModel.Runtime.Core.Descriptors;

namespace CommonModel.Runtime.Drivers.Generic.Mapping;

public sealed class FieldMapper
{
    public (
        IReadOnlyDictionary<string, object?> PrimaryKey,
        IReadOnlyDictionary<string, object?> Fields,
        IReadOnlyDictionary<string, object?> PreviousFields
    ) Apply(
        IReadOnlyDictionary<string, object?> fields,
        IReadOnlyDictionary<string, object?> previousFields,
        IReadOnlyList<FieldMappingRule> rules,
        EntityConfig? entityConfig)
    {
        var ruleMap = rules.ToDictionary(r => r.Source, r => r, StringComparer.OrdinalIgnoreCase);
        var entityPrimaryKeys = entityConfig?.PrimaryKey ?? new List<string>();

        var primaryKey = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var payload = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var previousPayload = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        // Inject static values first
        foreach (var rule in rules.Where(r => r.StaticValue is not null && !r.Exclude))
        {
            var targetName = rule.Target ?? rule.Source;
            if (rule.IsKey)
                primaryKey[targetName] = rule.StaticValue;
            else
                payload[targetName] = rule.StaticValue;
        }

        // Process source fields
        foreach (var (fieldName, rawValue) in fields)
        {
            if (ruleMap.TryGetValue(fieldName, out var rule))
            {
                if (rule.Exclude) continue;

                var targetName = rule.Target ?? fieldName;
                var value = rule.Type is not null ? CastValue(rawValue, rule.Type) : rawValue;
                value = ApplyConceptMap(rule, value);

                if (rule.IsKey)
                    primaryKey[targetName] = value;
                else
                    payload[targetName] = value;
            }
            else
            {
                // Pass-through: entity primaryKey columns → PK dict, rest → payload
                if (entityPrimaryKeys.Contains(fieldName, StringComparer.OrdinalIgnoreCase))
                    primaryKey[fieldName] = rawValue;
                else
                    payload[fieldName] = rawValue;
            }
        }

        // Process previous fields (same mapping, no static injection)
        foreach (var (fieldName, rawValue) in previousFields)
        {
            if (ruleMap.TryGetValue(fieldName, out var rule))
            {
                if (rule.Exclude) continue;
                var targetName = rule.Target ?? fieldName;
                var value = rule.Type is not null ? CastValue(rawValue, rule.Type) : rawValue;
                value = ApplyConceptMap(rule, value);
                if (!rule.IsKey) previousPayload[targetName] = value;
            }
            else
            {
                if (!entityPrimaryKeys.Contains(fieldName, StringComparer.OrdinalIgnoreCase))
                    previousPayload[fieldName] = rawValue;
            }
        }

        return (primaryKey, payload, previousPayload);
    }

    private static object? ApplyConceptMap(FieldMappingRule rule, object? value)
    {
        if (rule.ConceptMap is null || value is null) return value;
        var key = Convert.ToString(value, CultureInfo.InvariantCulture) ?? "";
        return rule.ConceptMap.TryGetValue(key, out var mapped) ? mapped : value;
    }

    private static object? CastValue(object? value, string type)
    {
        if (value is null) return null;
        var str = Convert.ToString(value, CultureInfo.InvariantCulture) ?? "";

        return type.ToLowerInvariant() switch
        {
            "string"    => str,
            "int"       => int.TryParse(str, out var i) ? i : value,
            "long"      => long.TryParse(str, out var l) ? l : value,
            "double"    => double.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : value,
            "bool"      => bool.TryParse(str, out var b) ? b : value,
            "timestamp" => DateTimeOffset.TryParse(str, out var ts) ? ts : value,
            "date"      => DateOnly.TryParse(str, out var dt) ? dt : value,
            _           => value
        };
    }
}
