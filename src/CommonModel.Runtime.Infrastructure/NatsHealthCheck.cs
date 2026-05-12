using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsHealthCheck : IHealthCheck
{
    private readonly NatsConnectionFactory _factory;

    public NatsHealthCheck(NatsConnectionFactory factory) => _factory = factory;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(3));

            var conn = await _factory.GetSharedConnectionAsync(cts.Token);
            await conn.PingAsync(cts.Token);
            return HealthCheckResult.Healthy("NATS reachable");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("NATS unreachable", ex);
        }
    }
}
