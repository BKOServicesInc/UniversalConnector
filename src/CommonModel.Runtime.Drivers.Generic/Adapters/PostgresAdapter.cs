using Microsoft.Extensions.Logging;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System.Runtime.CompilerServices;
using System.Xml;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class PostgresAdapter : BaseProtocolAdapter
{
    private readonly ILogger<PostgresAdapter> _logger;

    // Keyed by connection string — one data source per unique Postgres endpoint.
    // Required because the adapter is a singleton shared across all postgres connectors.
    private readonly Dictionary<string, NpgsqlDataSource> _dataSources = new();

    // Keyed by "{connectorId}:{entityPath}" to prevent watermark collisions
    // when multiple connectors poll different databases via the same adapter instance.
    private readonly Dictionary<string, DateTimeOffset> _watermarks = new();

    public PostgresAdapter(ILogger<PostgresAdapter> logger) => _logger = logger;

    public override string SourceType => "postgres";

    protected override Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var cs = BuildConnectionString(descriptor);
        if (!_dataSources.ContainsKey(cs))
            _dataSources[cs] = NpgsqlDataSource.Create(cs);
        return Task.CompletedTask;
    }

    protected override async Task CloseCoreAsync(CancellationToken ct)
    {
        // Nothing to do here — data sources are disposed in DisposeAsync
        // to avoid tearing down a shared source that other connectors may still use.
        await Task.CompletedTask;
    }

    private NpgsqlDataSource GetDataSource(ConnectorDescriptor descriptor)
    {
        var cs = BuildConnectionString(descriptor);
        if (_dataSources.TryGetValue(cs, out var ds))
            return ds;
        // Lazy-create if OpenCoreAsync was skipped (e.g. tests)
        ds = NpgsqlDataSource.Create(cs);
        _dataSources[cs] = ds;
        return ds;
    }

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        if (descriptor.ChangeDetection.Mode.Equals("cdc", StringComparison.OrdinalIgnoreCase))
        {
            await foreach (var r in StreamCdcAsync(descriptor, ct)) yield return r;
        }
        else
        {
            await foreach (var r in StreamPollingAsync(descriptor, ct)) yield return r;
        }
    }

    private async IAsyncEnumerable<RawChangeRecord> StreamCdcAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var slot = descriptor.ChangeDetection.ReplicationSlot;
        var publication = descriptor.ChangeDetection.Publication;
        var cs = BuildConnectionString(descriptor);

        await EnsureReplicationSlotAndPublication(descriptor, slot, publication, ct);

        // LogicalReplicationConnection takes a plain Npgsql connection string
        await using var conn = new LogicalReplicationConnection(cs);
        await conn.Open(ct);

        var options = new PgOutputReplicationOptions(publication, PgOutputProtocolVersion.V2, true);
        var replicationSlot = new PgOutputReplicationSlot(slot);

        await foreach (var msg in conn.StartReplication(replicationSlot, options, ct))
        {
            RawChangeRecord? record = null;

            if (msg is InsertMessage ins)
            {
                var fields = await ReadFields(ins.Relation, ins.NewRow, ct);
                record = new RawChangeRecord
                {
                    EntityPath = $"{ins.Relation.Namespace}.{ins.Relation.RelationName}",
                    ChangeType = ChangeType.Insert,
                    SourceTimestamp = ins.ServerClock,
                    Fields = fields
                };
            }
            else if (msg is FullUpdateMessage fullUpd)
            {
                // OldRow must be read before NewRow — ReplicationTuple is a forward-only stream
                // and the pgoutput protocol encodes old row data before new row data on the wire.
                var oldFields = await ReadFields(fullUpd.Relation, fullUpd.OldRow, ct);
                var newFields = await ReadFields(fullUpd.Relation, fullUpd.NewRow, ct);
                record = new RawChangeRecord
                {
                    EntityPath = $"{fullUpd.Relation.Namespace}.{fullUpd.Relation.RelationName}",
                    ChangeType = ChangeType.Update,
                    SourceTimestamp = fullUpd.ServerClock,
                    Fields = newFields,
                    PreviousFields = oldFields
                };
            }
            else if (msg is UpdateMessage upd)
            {
                var fields = await ReadFields(upd.Relation, upd.NewRow, ct);
                record = new RawChangeRecord
                {
                    EntityPath = $"{upd.Relation.Namespace}.{upd.Relation.RelationName}",
                    ChangeType = ChangeType.Update,
                    SourceTimestamp = upd.ServerClock,
                    Fields = fields
                };
            }
            else if (msg is KeyDeleteMessage keyDel)
            {
                var fields = await ReadFields(keyDel.Relation, keyDel.Key, ct);
                record = new RawChangeRecord
                {
                    EntityPath = $"{keyDel.Relation.Namespace}.{keyDel.Relation.RelationName}",
                    ChangeType = ChangeType.Delete,
                    SourceTimestamp = keyDel.ServerClock,
                    Fields = fields
                };
            }
            else if (msg is FullDeleteMessage fullDel)
            {
                var fields = await ReadFields(fullDel.Relation, fullDel.OldRow, ct);
                record = new RawChangeRecord
                {
                    EntityPath = $"{fullDel.Relation.Namespace}.{fullDel.Relation.RelationName}",
                    ChangeType = ChangeType.Delete,
                    SourceTimestamp = fullDel.ServerClock,
                    Fields = fields
                };
            }

            conn.SetReplicationStatus(msg.WalEnd);

            if (record is not null)
                yield return record;
        }
    }

    private async IAsyncEnumerable<RawChangeRecord> StreamPollingAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(descriptor.ChangeDetection.PollIntervalSeconds);
        var watermarkCol = descriptor.ChangeDetection.WatermarkColumn;
        var entities = await ResolveEntities(descriptor, ct);
        var dataSource = GetDataSource(descriptor);

        while (!ct.IsCancellationRequested)
        {
            foreach (var entity in entities)
            {
                var wmKey = $"{descriptor.ConnectorId}:{entity}";
                if (!_watermarks.TryGetValue(wmKey, out var since))
                    since = DateTimeOffset.UtcNow - ParseDuration(descriptor.ChangeDetection.LookbackDuration);

                await using var conn = await dataSource.OpenConnectionAsync(ct);
                var sql = $"SELECT * FROM {entity} WHERE {watermarkCol} > @since ORDER BY {watermarkCol}";
                await using var cmd = new NpgsqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("since", since.UtcDateTime);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                DateTimeOffset maxWatermark = since;
                while (await reader.ReadAsync(ct))
                {
                    var fields = new Dictionary<string, object?>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        fields[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

                    if (fields.TryGetValue(watermarkCol, out var wm) && wm is DateTime dt)
                    {
                        var wmOffset = new DateTimeOffset(dt, TimeSpan.Zero);
                        if (wmOffset > maxWatermark) maxWatermark = wmOffset;
                    }

                    yield return new RawChangeRecord
                    {
                        EntityPath = entity,
                        ChangeType = ChangeType.Snapshot,
                        SourceTimestamp = maxWatermark,
                        Fields = fields
                    };
                }

                _watermarks[wmKey] = maxWatermark;
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task EnsureReplicationSlotAndPublication(
        ConnectorDescriptor descriptor, string slot, string pub, CancellationToken ct)
    {
        await using var conn = await GetDataSource(descriptor).OpenConnectionAsync(ct);

        // Validate wal_level first — logical replication requires it to be 'logical'.
        // Without this check, StartReplication fails later with a cryptic protocol error.
        await using (var walCmd = new NpgsqlCommand("SHOW wal_level", conn))
        {
            var walLevel = (await walCmd.ExecuteScalarAsync(ct))?.ToString();
            if (!string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException(
                    $"PostgreSQL wal_level must be 'logical' for CDC replication but is '{walLevel}'. " +
                    "Set 'wal_level = logical' in postgresql.conf and restart the server.");
        }

        await using var pubCmd = new NpgsqlCommand(
            "SELECT 1 FROM pg_publication WHERE pubname = @pub", conn);
        pubCmd.Parameters.AddWithValue("pub", pub);
        var pubExists = await pubCmd.ExecuteScalarAsync(ct) is not null;
        if (!pubExists)
        {
            await using var createPub = new NpgsqlCommand($"CREATE PUBLICATION {pub} FOR ALL TABLES", conn);
            await createPub.ExecuteNonQueryAsync(ct);
            _logger.LogInformation("Created publication '{Publication}'", pub);
        }

        await using var slotCmd = new NpgsqlCommand(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot", conn);
        slotCmd.Parameters.AddWithValue("slot", slot);
        var slotExists = await slotCmd.ExecuteScalarAsync(ct) is not null;
        if (!slotExists)
        {
            await using var createSlot = new NpgsqlCommand(
                "SELECT pg_create_logical_replication_slot(@slot, 'pgoutput')", conn);
            createSlot.Parameters.AddWithValue("slot", slot);
            await createSlot.ExecuteNonQueryAsync(ct);
            _logger.LogInformation("Created replication slot '{Slot}'", slot);
        }
    }

    private async Task<List<string>> ResolveEntities(ConnectorDescriptor d, CancellationToken ct)
    {
        if (!d.Watch.AutoDiscover)
            return d.Watch.Entities.Select(e => e.Name).ToList();

        await using var conn = await GetDataSource(d).OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var tables = new List<string>();
        while (await reader.ReadAsync(ct))
            tables.Add(reader.GetString(0));
        return tables;
    }

    // Npgsql 10: column names come from the RelationMessage's Columns array
    private static async Task<Dictionary<string, object?>> ReadFields(
        RelationMessage relation, ReplicationTuple tuple, CancellationToken ct)
    {
        var fields = new Dictionary<string, object?>();
        var columns = relation.Columns;
        int i = 0;

        await foreach (var val in tuple)
        {
            if (i < columns.Count)
            {
                var colName = columns[i].ColumnName;
                object? value = val.Kind switch
                {
                    TupleDataKind.TextValue   => await val.Get<string>(ct),
                    TupleDataKind.BinaryValue => await val.Get<object>(ct),
                    _                         => null
                };
                fields[colName] = value;
            }
            i++;
        }

        return fields;
    }

    private static string BuildConnectionString(ConnectorDescriptor d)
    {
        if (!string.IsNullOrWhiteSpace(d.Connection.ConnectionString))
            return d.Connection.ConnectionString!;

        var b = new NpgsqlConnectionStringBuilder
        {
            Host = d.Connection.Host,
            Port = d.Connection.Port ?? 5432,
            Database = d.Connection.Database,
            Username = d.Connection.Username,
            Password = d.Connection.Password
        };
        return b.ConnectionString;
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor) => Array.Empty<string>();

    public override async ValueTask DisposeAsync()
    {
        foreach (var ds in _dataSources.Values)
            await ds.DisposeAsync();
        _dataSources.Clear();
        await base.DisposeAsync();
    }
}
