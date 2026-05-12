using System.Text.RegularExpressions;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Mapping;

public static class SubjectTemplateResolver
{
    private static readonly HashSet<string> KnownTokens =
    [
        "{context}", "{entityPath}", "{changeType}", "{driverId}", "{sourceType}"
    ];

    // Detects any remaining {word} pattern after substitution.
    private static readonly Regex UnknownTokenPattern = new(@"\{[^}]+\}", RegexOptions.Compiled);

    // Supported tokens: {context}, {entityPath}, {changeType}, {driverId}, {sourceType}
    public static string Resolve(string template, RawChangeEvent evt)
    {
        var resolved = template
            .Replace("{context}",    NormalizeSegment(evt.Context))
            .Replace("{entityPath}", evt.EntityPath)
            .Replace("{changeType}", evt.ChangeType.ToString().ToLowerInvariant())
            .Replace("{driverId}",   evt.DriverId)
            .Replace("{sourceType}", evt.SourceType);

        var unknown = UnknownTokenPattern.Match(resolved);
        if (unknown.Success)
            throw new ArgumentException(
                $"Subject template '{template}' contains unknown token '{unknown.Value}'. " +
                $"Known tokens: {string.Join(", ", KnownTokens)}");

        return resolved;
    }

    // Colons are invalid in NATS subjects — normalize to dashes and lowercase.
    private static string NormalizeSegment(string s) =>
        s.Replace(':', '-').ToLowerInvariant();
}
