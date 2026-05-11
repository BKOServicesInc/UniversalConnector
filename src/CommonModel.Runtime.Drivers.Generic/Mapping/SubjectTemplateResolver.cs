using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Mapping;

public static class SubjectTemplateResolver
{
    // Supported tokens: {context}, {entityPath}, {changeType}, {driverId}, {sourceType}
    public static string Resolve(string template, RawChangeEvent evt) =>
        template
            .Replace("{context}",    NormalizeSegment(evt.Context))
            .Replace("{entityPath}", evt.EntityPath)
            .Replace("{changeType}", evt.ChangeType.ToString().ToLowerInvariant())
            .Replace("{driverId}",   evt.DriverId)
            .Replace("{sourceType}", evt.SourceType);

    // Colons are invalid in NATS subjects — normalize to dashes and lowercase.
    private static string NormalizeSegment(string s) =>
        s.Replace(':', '-').ToLowerInvariant();
}
