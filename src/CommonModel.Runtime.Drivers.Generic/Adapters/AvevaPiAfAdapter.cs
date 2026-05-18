// AVEVA PI System Explorer adapter — connects to PI Asset Framework via the
// official OSIsoft.AFSDK and supports bidirectional sync of element templates
// and elements. Server identity, AF system name, AF database, and the
// authentication mode are read entirely from the connector descriptor — no
// server-specific code anywhere in this class.
//
// To use this adapter the host machine (or container) must have PI AF Client
// installed and OSIsoft.AFSDK.dll on the assembly probe path. Activating a
// second PI AF server is purely a config exercise: drop another descriptor
// yaml file in `connectors/` pointing at the new server.

using Microsoft.Extensions.Logging;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class AvevaPiAfAdapter : BaseProtocolAdapter, IWritableProtocolAdapter
{
    public const string SourceTypeName    = "avevapi-af";
    public const string EntityTypeTemplate = "elementTemplate";
    public const string EntityTypeElement  = "element";
    public const string ReplicaSessionHeader = "replicaSession";
    private const string CookieEntityPath = "afcookie";

    private readonly ILogger<AvevaPiAfAdapter> _logger;
    private readonly ICheckpointStore _checkpoints;

    // One PISystem connection is shared across all descriptors that target the
    // same AF server. The adapter is a singleton, so we cache by AF system name.
    private readonly Dictionary<string, PISystem> _systems = new(StringComparer.OrdinalIgnoreCase);

    // AF Server-side change cookies — per driver. AFDatabase.FindChangedItems
    // returns a cookie that resumes the next call from where this one left off.
    // On first open per driver, we either restore the cookie from the checkpoint
    // store (survives restarts) or anchor it to "now" via GetFindChangedItemsCookie
    // — never start from null, which would replay the entire AF change-buffer
    // history on every cold start.
    private readonly Dictionary<string, object?> _changeCookies = new();

    // Tracks replica-session IDs we have applied via the reverse path so that the
    // forward poll can drop the resulting echo CDC event (loop prevention L1).
    private readonly HashSet<string> _selfWriteSessions = new(StringComparer.OrdinalIgnoreCase);

    public AvevaPiAfAdapter(ILogger<AvevaPiAfAdapter> logger, ICheckpointStore checkpoints)
    {
        _logger      = logger;
        _checkpoints = checkpoints;
    }

    public override string SourceType => SourceTypeName;

    public IReadOnlyList<string> SupportedEntityTypes =>
        new[] { EntityTypeTemplate, EntityTypeElement };

    // ─── Open / close ────────────────────────────────────────────────────────

    protected override async Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var systemName = ResolveSystemName(descriptor);
        if (!_systems.TryGetValue(systemName, out var system))
        {
            var systems = new PISystems();
            system = systems[systemName]
                ?? throw new InvalidOperationException(
                    $"PI AF system '{systemName}' is not registered on this host. " +
                    $"Add it in PI System Explorer or via `afdiag /pisystem:{systemName}`.");

            var c = descriptor.Connection;
            if (!string.IsNullOrWhiteSpace(c.Username))
                system.Connect(new System.Net.NetworkCredential(c.Username, c.Password ?? ""));
            else
                system.Connect();

            _systems[systemName] = system;
            _logger.LogInformation(
                "Connected to PI AF system '{System}' (server: {Server}, version: {Version})",
                systemName, system.Name, system.ServerVersion);
        }

        await EnsureCookieAsync(descriptor.DriverId, system, ct);
    }

    // Per-driver cookie initialization — runs once per driver lifetime. Order
    // of preference:
    //   1. Already in-memory (we've been polling this run) → no-op.
    //   2. Restore from checkpoint store (KV bucket cm-checkpoints) → resume
    //      exactly where we left off across restarts.
    //   3. Anchor with PISystem.GetFindChangedItemsCookie(searchSandbox:false)
    //      → "start from now", ignoring everything already in the AF change
    //      buffer at startup. Matches the legacy 4.8 connector behavior and
    //      prevents the days-of-history ghost replay on first run.
    private async Task EnsureCookieAsync(string driverId, PISystem system, CancellationToken ct)
    {
        if (_changeCookies.ContainsKey(driverId)) return;

        var checkpoint = await _checkpoints.GetAsync(driverId, CookieEntityPath, ct);
        if (checkpoint is not null && TryDeserializeCookie(checkpoint.Position, out var restored))
        {
            _changeCookies[driverId] = restored;
            _logger.LogInformation(
                "Restored PI AF change-cookie for driver '{Driver}' from checkpoint (updated {When:o})",
                driverId, checkpoint.UpdatedAt);
            return;
        }

        // No persisted cookie — anchor to "now" so we don't replay historical
        // commits that pre-date the connector.
        var anchor = system.GetFindChangedItemsCookie(searchSandbox: false);
        _changeCookies[driverId] = anchor;
        _logger.LogInformation(
            "Anchored PI AF change-cookie for driver '{Driver}' to current server state — " +
            "pre-existing changes in the AF buffer will be ignored.", driverId);

        // Persist the anchor immediately so a crash before the first delta save
        // still leaves us with a valid resume point.
        await TrySaveCookieAsync(driverId, anchor, ct);
    }

    protected override Task CloseCoreAsync(CancellationToken ct)
    {
        foreach (var sys in _systems.Values)
        {
            try { sys.Disconnect(); } catch { /* swallow */ }
        }
        _systems.Clear();
        return Task.CompletedTask;
    }

    // ─── Forward path: poll for template / element changes ──────────────────

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(descriptor.ChangeDetection.PollIntervalSeconds, 5));
        var db = ResolveDatabase(descriptor);
        var watchTypes = ResolveWatchedTypes(descriptor);

        while (!ct.IsCancellationRequested)
        {
            foreach (var rec in DrainChanges(db, descriptor.DriverId, watchTypes))
            {
                if (rec.AdapterMetadata.TryGetValue(ReplicaSessionHeader, out var sid) &&
                    _selfWriteSessions.Contains(sid))
                {
                    _selfWriteSessions.Remove(sid);
                    continue;
                }
                yield return rec;
            }

            await TrySaveCookieAsync(descriptor.DriverId, _changeCookies[descriptor.DriverId], ct);

            try { await Task.Delay(interval, ct); }
            catch (OperationCanceledException) { yield break; }
        }
    }

    // AFDatabase.FindChangedItems is the official server-side delta API: it
    // returns only items changed since the previous cookie. We drain in pages
    // until the server reports no more changes for this round.
    //
    // searchSandbox: false → committed server-side changes only. Setting this
    // to true would surface the calling SDK session's own pending check-outs.
    private IEnumerable<RawChangeRecord> DrainChanges(
        AFDatabase db, string driverId, HashSet<string> watchTypes)
    {
        var key = driverId;
        _changeCookies.TryGetValue(key, out var cookie);

        const int pageSize = 500;
        while (true)
        {
            var changes = db.FindChangedItems(false, pageSize, cookie, out var nextCookie);
            cookie = nextCookie;
            if (changes is null || changes.Count == 0) break;

            foreach (var ci in changes)
            {
                var rec = ToRecord(db, ci, watchTypes);
                if (rec is not null) yield return rec;
            }

            if (changes.Count < pageSize) break;
        }

        _changeCookies[key] = cookie;
    }

    // ─── Cookie persistence ─────────────────────────────────────────────────
    //
    // The AF SDK returns the cookie as an opaque `object` (concrete type varies
    // by AF SDK version — typically string or a small DTO). We serialize via
    // XmlSerializer (the same approach the legacy 4.8 connector used), prefix
    // with the AssemblyQualifiedName so the loader can recover the type, and
    // store the result as a base64 string in Checkpoint.Position.
    //
    // All operations are best-effort: a serialization failure logs a warning
    // and falls back to in-memory cookie only. The connector keeps polling.

    private async Task TrySaveCookieAsync(string driverId, object? cookie, CancellationToken ct)
    {
        if (cookie is null) return;
        var serialized = SerializeCookie(cookie);
        if (serialized is null) return;

        try
        {
            await _checkpoints.SaveAsync(new Checkpoint
            {
                DriverId   = driverId,
                EntityPath = CookieEntityPath,
                Position   = serialized
            }, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to persist PI AF change-cookie for driver '{Driver}' — " +
                "next restart will re-anchor to current server state.", driverId);
        }
    }

    private string? SerializeCookie(object cookie)
    {
        try
        {
            var type = cookie.GetType();
            var ser  = new XmlSerializer(type);
            using var sw = new StringWriter();
            ser.Serialize(sw, cookie);
            var bytes = Encoding.UTF8.GetBytes(sw.ToString());
            return $"{type.AssemblyQualifiedName}|{Convert.ToBase64String(bytes)}";
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "PI AF change-cookie is not XML-serializable ({Type}) — checkpoint persistence disabled.",
                cookie.GetType().FullName);
            return null;
        }
    }

    private bool TryDeserializeCookie(string? position, out object? cookie)
    {
        cookie = null;
        if (string.IsNullOrWhiteSpace(position)) return false;
        var sep = position.IndexOf('|');
        if (sep <= 0) return false;

        try
        {
            var type = Type.GetType(position[..sep]);
            if (type is null) return false;
            var xml = Encoding.UTF8.GetString(Convert.FromBase64String(position[(sep + 1)..]));
            var ser = new XmlSerializer(type);
            using var sr = new StringReader(xml);
            cookie = ser.Deserialize(sr);
            return cookie is not null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to deserialize persisted PI AF change-cookie — will re-anchor to current state.");
            return false;
        }
    }

    private static RawChangeRecord? ToRecord(AFDatabase db, AFChangeInfo info, HashSet<string> watchTypes)
    {
        var changeType = info.Action switch
        {
            AFChangeInfoAction.Added   => ChangeType.Insert,
            AFChangeInfoAction.Updated => ChangeType.Update,
            AFChangeInfoAction.Removed => ChangeType.Delete,
            _                          => (ChangeType?)null
        };
        if (changeType is null) return null;

        var ts = new DateTimeOffset(info.ChangeTime.UtcTime, TimeSpan.Zero);

        switch (info.Identity)
        {
            case AFIdentity.Element when watchTypes.Contains(EntityTypeElement):
            {
                if (changeType == ChangeType.Delete)
                    return DeletedRecord(EntityTypeElement, info, ts);
                var el = db.Elements[info.ID];
                if (el is null) return null;
                return new RawChangeRecord
                {
                    EntityPath      = $"{EntityTypeElement}/{el.GetPath()}",
                    ChangeType      = changeType.Value,
                    SourceTimestamp = ts,
                    Fields          = BuildElementFields(el),
                    AdapterMetadata = new Dictionary<string, string>
                    {
                        ["source"]     = SourceTypeName,
                        ["entityType"] = EntityTypeElement
                    }
                };
            }
            case AFIdentity.ElementTemplate when watchTypes.Contains(EntityTypeTemplate):
            {
                if (changeType == ChangeType.Delete)
                    return DeletedRecord(EntityTypeTemplate, info, ts);
                var t = db.ElementTemplates[info.ID];
                if (t is null) return null;
                return new RawChangeRecord
                {
                    EntityPath      = $"{EntityTypeTemplate}/{t.Name}",
                    ChangeType      = changeType.Value,
                    SourceTimestamp = ts,
                    Fields          = BuildTemplateFields(t),
                    AdapterMetadata = new Dictionary<string, string>
                    {
                        ["source"]     = SourceTypeName,
                        ["entityType"] = EntityTypeTemplate
                    }
                };
            }
        }
        return null;
    }

    private static RawChangeRecord DeletedRecord(string entityType, AFChangeInfo info, DateTimeOffset ts) =>
        new()
        {
            EntityPath      = $"{entityType}/{info.ID}",
            ChangeType      = ChangeType.Delete,
            SourceTimestamp = ts,
            Fields          = new Dictionary<string, object?>
            {
                ["uniqueId"] = info.ID.ToString()
            },
            AdapterMetadata = new Dictionary<string, string>
            {
                ["source"]     = SourceTypeName,
                ["entityType"] = entityType
            }
        };

    private static Dictionary<string, object?> BuildTemplateFields(AFElementTemplate t)
    {
        var attrs = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var a in t.AttributeTemplates.Cast<AFAttributeTemplate>())
        {
            attrs[a.Name] = new Dictionary<string, object?>
            {
                ["type"]         = a.Type?.Name,
                ["defaultValue"] = SafeDefaultValue(a),
                ["uom"]          = a.DefaultUOM?.Abbreviation,
                ["isConfig"]     = a.IsConfigurationItem
            };
        }

        return new Dictionary<string, object?>
        {
            ["name"]         = t.Name,
            ["description"]  = t.Description,
            ["baseTemplate"] = t.BaseTemplate?.Name,
            ["categories"]   = t.Categories.Select(c => c.Name).ToList(),
            ["attributes"]   = attrs,
            ["uniqueId"]     = t.UniqueID.ToString()
        };
    }

    private static string? SafeDefaultValue(AFAttributeTemplate a)
    {
        try { return a.GetValue(null)?.ToString(); }
        catch { return null; }
    }

    private static Dictionary<string, object?> BuildElementFields(AFElement e)
    {
        var values = new Dictionary<string, object?>();
        foreach (var a in e.Attributes)
        {
            try { values[a.Name] = a.GetValue()?.Value?.ToString(); }
            catch { values[a.Name] = null; }
        }

        return new Dictionary<string, object?>
        {
            ["name"]        = e.Name,
            ["description"] = e.Description,
            ["template"]    = e.Template?.Name,
            ["parent"]      = e.Parent?.GetPath(),
            ["path"]        = e.GetPath(),
            ["attributes"]  = values,
            ["categories"]  = e.Categories.Select(c => c.Name).ToList(),
            ["uniqueId"]    = e.UniqueID.ToString()
        };
    }

    private static HashSet<string> ResolveWatchedTypes(ConnectorDescriptor d)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (d.Watch.Entities.Count == 0)
        {
            set.Add(EntityTypeElement);
            set.Add(EntityTypeTemplate);
            return set;
        }
        foreach (var e in d.Watch.Entities)
            set.Add(string.IsNullOrWhiteSpace(e.Filter) ? EntityTypeElement : e.Filter!);
        return set;
    }

    // ─── Reverse path: template + element CRUD ──────────────────────────────

    public async Task<WriteResult> ApplyAsync(
        ConnectorDescriptor descriptor, WriteCommand command, CancellationToken ct)
    {
        await Task.Yield();

        AFDatabase? db = null;
        try
        {
            db = ResolveDatabase(descriptor);

            // PI AF write lifecycle:
            //   1. Mutate in-memory (Add / Remove / property sets)
            //      AF SDK implicitly checks out each touched object.
            //   2. db.CheckIn() — pushes the pending change set to the AF server.
            //      Until CheckIn runs, NOTHING is persisted; the change exists
            //      only in this client's transaction buffer.
            //   3. On failure, db.UndoCheckOut(true) reverts every dirty object
            //      so the next command starts from a clean transaction.
            var result = command.EntityType switch
            {
                EntityTypeTemplate => ApplyTemplate(db, command),
                EntityTypeElement  => ApplyElement (db, command),
                _                  => WriteResult.Fail(
                    $"Unsupported entityType '{command.EntityType}'. " +
                    $"Supported: {string.Join(", ", SupportedEntityTypes)}")
            };

            if (!result.Success)
            {
                TryUndoCheckOut(db, command.CorrelationId);
                return result;
            }

            try
            {
                db.CheckIn();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "PI AF CheckIn failed for {Op} {EntityType} (corr={Corr}) — reverting",
                    command.Operation, command.EntityType, command.CorrelationId);
                TryUndoCheckOut(db, command.CorrelationId);
                return WriteResult.Fail($"CheckIn failed: {ex.Message}");
            }

            var sid = Ulid.NewUlid().ToString();
            _selfWriteSessions.Add(sid);
            _logger.LogInformation(
                "PI AF CheckIn ► {Op} {EntityType} (corr={Corr}, replicaSession={Sid})",
                command.Operation, command.EntityType, command.CorrelationId, sid);
            return WriteResult.Ok(sid, result.Fields);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PI AF write failed for {EntityType} {Pk}",
                command.EntityType, string.Join(",", command.PrimaryKey.Values));
            if (db is not null) TryUndoCheckOut(db, command.CorrelationId);
            return WriteResult.Fail(ex.Message);
        }
    }

    private void TryUndoCheckOut(AFDatabase db, string correlationId)
    {
        try
        {
            db.UndoCheckOut(true);
            _logger.LogWarning(
                "Reverted pending AF check-outs after failure (corr={Corr})",
                correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "UndoCheckOut failed (corr={Corr}) — database may be left with dirty state",
                correlationId);
        }
    }

    private static WriteResult ApplyTemplate(AFDatabase db, WriteCommand cmd)
    {
        var name = ResolveName(cmd, "name");
        var existing = db.ElementTemplates[name];

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"Template '{name}' already exists.");
                var created = db.ElementTemplates.Add(name);
                ApplyTemplateFields(created, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?> { ["uniqueId"] = created.UniqueID.ToString() });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"Template '{name}' not found.");
                ApplyTemplateFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?> { ["uniqueId"] = existing.UniqueID.ToString() });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");   // idempotent
                db.ElementTemplates.Remove(existing);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static void ApplyTemplateFields(AFElementTemplate template, IReadOnlyDictionary<string, object?> fields)
    {
        if (fields.TryGetValue("description", out var desc))
            template.Description = desc?.ToString() ?? "";

        if (fields.TryGetValue("baseTemplate", out var bt) && bt is string baseName && baseName.Length > 0)
            template.BaseTemplate = template.Database.ElementTemplates[baseName];

        if (fields.TryGetValue("attributes", out var attrsObj) &&
            attrsObj is IReadOnlyDictionary<string, object?> attrs)
        {
            foreach (var (attrName, spec) in attrs)
            {
                var attr = template.AttributeTemplates[attrName] ?? template.AttributeTemplates.Add(attrName);
                if (spec is IReadOnlyDictionary<string, object?> specMap)
                {
                    if (specMap.TryGetValue("type", out var t) && t is string typeName)
                        attr.Type = Type.GetType(typeName) ?? attr.Type;
                    if (specMap.TryGetValue("defaultValue", out var dv))
                        attr.SetValue(dv, null);
                    if (specMap.TryGetValue("uom", out var uom) && uom is string uomAbbrev)
                        attr.DefaultUOM = template.Database.PISystem.UOMDatabase.UOMs[uomAbbrev];
                    if (specMap.TryGetValue("isConfig", out var ic) && ic is bool isConfig)
                        attr.IsConfigurationItem = isConfig;
                }
            }
        }
    }

    private static WriteResult ApplyElement(AFDatabase db, WriteCommand cmd)
    {
        // Path can be supplied either via primary key "path" or fields["path"] / fields["name"]
        var path     = cmd.PrimaryKey.TryGetValue("path", out var p) ? p?.ToString() : null;
        var name     = ResolveName(cmd, "name");
        var existing = !string.IsNullOrWhiteSpace(path)
            ? AFObject.FindObject(path, db) as AFElement
            : db.Elements[name];

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"Element '{name}' already exists.");
                var templateName = cmd.Fields.TryGetValue("template", out var tpl) ? tpl?.ToString() : null;
                var template = !string.IsNullOrWhiteSpace(templateName)
                    ? db.ElementTemplates[templateName]
                    : null;
                var parentPath = cmd.Fields.TryGetValue("parent", out var pp) ? pp?.ToString() : null;
                var parent = !string.IsNullOrWhiteSpace(parentPath)
                    ? AFObject.FindObject(parentPath, db) as AFElement
                    : null;
                var created = parent is null
                    ? db.Elements.Add(name, template)
                    : parent.Elements.Add(name, template);
                ApplyElementFields(created, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = created.UniqueID.ToString(),
                    ["path"]     = created.GetPath()
                });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"Element '{name}' not found at '{path}'.");
                ApplyElementFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = existing.UniqueID.ToString(),
                    ["path"]     = existing.GetPath()
                });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");
                if (existing.Parent is AFElement parentEl)
                    parentEl.Elements.Remove(existing);
                else
                    db.Elements.Remove(existing);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static void ApplyElementFields(AFElement element, IReadOnlyDictionary<string, object?> fields)
    {
        if (fields.TryGetValue("description", out var desc))
            element.Description = desc?.ToString() ?? "";

        if (fields.TryGetValue("attributes", out var attrsObj) &&
            attrsObj is IReadOnlyDictionary<string, object?> attrs)
        {
            foreach (var (attrName, value) in attrs)
            {
                var attr = element.Attributes[attrName];
                if (attr is null) continue;
                try { attr.SetValue(new AFValue(value)); }
                catch { /* attribute may be PI-bound — skip silently */ }
            }
        }
    }

    // ─── Validation ─────────────────────────────────────────────────────────

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(descriptor.Connection.AfSystemName))
            errors.Add("connection.afSystemName is required for avevapi-af");
        if (string.IsNullOrWhiteSpace(descriptor.Connection.AfDatabase))
            errors.Add("connection.afDatabase is required for avevapi-af");
        return errors;
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private PISystem ResolveSystem(ConnectorDescriptor descriptor)
    {
        var name = ResolveSystemName(descriptor);
        if (_systems.TryGetValue(name, out var sys) && sys.ConnectionInfo.IsConnected)
            return sys;
        // Lazy-open if needed (e.g. write command arrived before stream started)
        OpenCoreAsync(descriptor, CancellationToken.None).GetAwaiter().GetResult();
        return _systems[name];
    }

    private AFDatabase ResolveDatabase(ConnectorDescriptor descriptor)
    {
        var system = ResolveSystem(descriptor);
        var dbName = descriptor.Connection.AfDatabase
            ?? throw new InvalidOperationException("connection.afDatabase is required for avevapi-af");
        return system.Databases[dbName]
            ?? throw new InvalidOperationException(
                $"AF database '{dbName}' not found on system '{system.Name}'");
    }

    private static string ResolveSystemName(ConnectorDescriptor descriptor) =>
        descriptor.Connection.AfSystemName
            ?? descriptor.Connection.Host
            ?? throw new InvalidOperationException("connection.afSystemName (or connection.host) is required");

    // Each entity entry is { name: "...", filter?: "template|element" } — filter
    // chooses which AF object class. Defaults to element.
    private static string ResolveName(WriteCommand cmd, string fallbackKey)
    {
        if (cmd.PrimaryKey.TryGetValue("name", out var pk) && pk is string pkName && pkName.Length > 0)
            return pkName;
        if (cmd.Fields.TryGetValue(fallbackKey, out var fb) && fb is string fbName && fbName.Length > 0)
            return fbName;
        throw new InvalidOperationException("Write command must include primaryKey.name or fields.name");
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override async ValueTask DisposeAsync()
    {
        foreach (var sys in _systems.Values)
        {
            try { sys.Disconnect(); } catch { }
        }
        _systems.Clear();
        await base.DisposeAsync();
    }
}
