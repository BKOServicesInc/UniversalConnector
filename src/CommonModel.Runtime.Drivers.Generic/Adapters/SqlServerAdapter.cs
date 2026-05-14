using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Xml;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class SqlServerAdapter : BaseProtocolAdapter
{
    private readonly ILogger<SqlServerAdapter> _logger;
    private string _connectionString = "";
    private readonly Dictionary<string, long> _versions = new();

    public SqlServerAdapter(ILogger<SqlServerAdapter> logger) => _logger = logger;

    public override string SourceType => "sqlserver";

    protected override Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        _connectionString = BuildConnectionString(descriptor);
        return Task.CompletedTask;
    }

    protected override Task CloseCoreAsync(CancellationToken ct) => Task.CompletedTask;

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var mode = descriptor.ChangeDetection.Mode.ToLowerInvariant();

        if (mode == "cdc")
        {
            await foreach (var r in StreamCdcAsync(descriptor, ct))
                yield return r;
        }
        else
        {
            await foreach (var r in StreamPollingAsync(descriptor, ct))
                yield return r;
        }
    }

    private async IAsyncEnumerable<RawChangeRecord> StreamCdcAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var entities = await ResolveEntities(descriptor, ct);

        if (descriptor.ChangeDetection.AutoEnableChangeTracking)
            await EnableChangeTracking(descriptor, entities, ct);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in entities)
            {
                if (!_versions.TryGetValue(entity.Name, out var fromVersion))
                    fromVersion = await GetCurrentVersion(ct);

                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync(ct);

                var joinOn  = string.Join(" AND ", entity.PrimaryKey.Select(pk => $"t.[{pk}] = ct.[{pk}]"));
                // Always select the PK columns from the ct (CHANGETABLE) side so that
                // DELETE events still carry the primary key — the LEFT JOIN to the table
                // returns NULL for all t.* columns once the row has been removed.
                var ctPkCols = string.Join(", ", entity.PrimaryKey.Select(pk => $"ct.[{pk}]"));
                var sql = $@"
                    SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, ct.SYS_CHANGE_COLUMNS,
                           {ctPkCols},
                           t.*
                    FROM CHANGETABLE(CHANGES {entity.Name}, @from) AS ct
                    LEFT JOIN {entity.Name} t ON {joinOn}
                    ORDER BY ct.SYS_CHANGE_VERSION";

                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@from", fromVersion);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                long maxVersion = fromVersion;
                while (await reader.ReadAsync(ct))
                {
                    var version   = reader.GetInt64(0);
                    var operation = reader.GetString(1);
                    if (version > maxVersion) maxVersion = version;

                    // Columns 3..3+pkCount-1 = PK values from ct (always present, even on DELETE)
                    var pkCount  = entity.PrimaryKey.Count;
                    var pkValues = new Dictionary<string, object?>(pkCount);
                    for (int i = 0; i < pkCount; i++)
                        pkValues[entity.PrimaryKey[i]] = reader.IsDBNull(3 + i) ? null : reader.GetValue(3 + i);

                    // Columns 3+pkCount onwards = t.* (NULL on DELETE because row is gone)
                    var fields = new Dictionary<string, object?>();
                    int tStart = 3 + pkCount;
                    for (int i = tStart; i < reader.FieldCount; i++)
                        fields[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

                    var changeType = operation switch
                    {
                        "I" => ChangeType.Insert,
                        "U" => ChangeType.Update,
                        "D" => ChangeType.Delete,
                        _   => ChangeType.Snapshot
                    };

                    // For DELETE the table row is gone — keep only the PK so the event
                    // carries a meaningful identity rather than a bag of empty strings.
                    if (changeType == ChangeType.Delete)
                        fields = pkValues;
                    else
                        foreach (var (k, v) in pkValues)
                            fields[k] = v;   // ensure ct PK values take precedence over t.*

                    yield return new RawChangeRecord
                    {
                        EntityPath = entity.Name,
                        ChangeType = changeType,
                        Fields = fields
                    };
                }

                _versions[entity.Name] = maxVersion;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async IAsyncEnumerable<RawChangeRecord> StreamPollingAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var watermarkCol = descriptor.ChangeDetection.WatermarkColumn;
        var entities = await ResolveEntities(descriptor, ct);
        var watermarks = new Dictionary<string, DateTimeOffset>();

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in entities)
            {
                if (!watermarks.TryGetValue(entity.Name, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync(ct);

                var sql = $"SELECT * FROM {entity.Name} WHERE {watermarkCol} > @since ORDER BY {watermarkCol}";
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@since", since.UtcDateTime);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                DateTimeOffset maxWm = since;
                while (await reader.ReadAsync(ct))
                {
                    var fields = new Dictionary<string, object?>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        fields[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

                    if (fields.TryGetValue(watermarkCol, out var wm) && wm is DateTime dt)
                    {
                        var wmOffset = new DateTimeOffset(dt, TimeSpan.Zero);
                        if (wmOffset > maxWm) maxWm = wmOffset;
                    }

                    yield return new RawChangeRecord
                    {
                        EntityPath = entity.Name,
                        ChangeType = ChangeType.Snapshot,
                        Fields = fields
                    };
                }

                watermarks[entity.Name] = maxWm;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task EnableChangeTracking(ConnectorDescriptor d, List<EntityConfig> entities, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var dbName = conn.Database;
        await using var dbCmd = new SqlCommand(
            $"IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()) " +
            $"ALTER DATABASE [{dbName}] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)", conn);
        await dbCmd.ExecuteNonQueryAsync(ct);

        foreach (var entity in entities)
        {
            await using var tCmd = new SqlCommand(
                $"IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('{entity.Name}')) " +
                $"ALTER TABLE {entity.Name} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)", conn);
            try { await tCmd.ExecuteNonQueryAsync(ct); }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not enable change tracking on {Table}", entity.Name);
            }
        }
    }

    private async Task<long> GetCurrentVersion(CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", conn);
        return (long)(await cmd.ExecuteScalarAsync(ct) ?? 0L);
    }

    private async Task<List<EntityConfig>> ResolveEntities(ConnectorDescriptor d, CancellationToken ct)
    {
        if (!d.Watch.AutoDiscover)
            return d.Watch.Entities;

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Discover tables and their primary key columns
        await using var cmd = new SqlCommand(@"
            SELECT
                t.TABLE_SCHEMA + '.' + t.TABLE_NAME AS TableName,
                c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
                ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND c.TABLE_NAME = tc.TABLE_NAME
            JOIN INFORMATION_SCHEMA.TABLES t
                ON t.TABLE_NAME = tc.TABLE_NAME AND t.TABLE_SCHEMA = tc.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY TableName", conn);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var pkMap = new Dictionary<string, List<string>>();
        while (await reader.ReadAsync(ct))
        {
            var table = reader.GetString(0);
            var col = reader.GetString(1);
            if (!pkMap.TryGetValue(table, out var cols))
                pkMap[table] = cols = new List<string>();
            cols.Add(col);
        }

        return pkMap.Select(kv => new EntityConfig { Name = kv.Key, PrimaryKey = kv.Value }).ToList();
    }

    private static string BuildConnectionString(ConnectorDescriptor d)
    {
        if (!string.IsNullOrWhiteSpace(d.Connection.ConnectionString))
            return d.Connection.ConnectionString!;

        var b = new SqlConnectionStringBuilder
        {
            DataSource = d.Connection.Port.HasValue
                ? $"{d.Connection.Host},{d.Connection.Port}"
                : d.Connection.Host ?? "",
            InitialCatalog = d.Connection.Database ?? "",
        };
        if (!string.IsNullOrWhiteSpace(d.Connection.Username))
        {
            b.UserID = d.Connection.Username;
            b.Password = d.Connection.Password ?? "";
        }
        else
        {
            b.IntegratedSecurity = true;
        }
        return b.ConnectionString;
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor) => Array.Empty<string>();
}
