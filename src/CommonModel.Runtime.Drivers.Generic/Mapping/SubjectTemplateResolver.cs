using System.Text;
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
            .Replace("{entityPath}", SanitizeEntityPath(evt.EntityPath))
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

    // NATS subjects only accept [A-Za-z0-9_.-] with '.' as token separator.
    // PI AF paths like "element/\\Aveva-Pi\BKO_LULU_DB\veda_element_1" contain
    // '/', '\\', and spaces — all illegal. Normalize separators to '.' (so
    // consumers can wildcard-subscribe per level) and collapse runs.
    // Mirrors NatsPublisher.SanitizeSubjectSegment.
    private static string SanitizeEntityPath(string raw)
    {
        if (string.IsNullOrEmpty(raw)) return raw;
        var sb = new StringBuilder(raw.Length);
        foreach (var c in raw)
        {
            if (c == '/' || c == '\\') sb.Append('.');
            else if (c == '.' || c == '-' || c == '_' || char.IsLetterOrDigit(c)) sb.Append(c);
            else sb.Append('_');
        }
        var s = sb.ToString();
        while (s.Contains("..")) s = s.Replace("..", ".");
        return s.Trim('.');
    }
}
