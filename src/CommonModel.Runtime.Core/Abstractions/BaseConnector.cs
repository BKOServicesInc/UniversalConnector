using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public abstract class BaseConnector : ISourceDriver
{
    protected readonly ILogger Logger;
    private long _sequenceNumber;
    private int _consecutiveFailures;
    private DriverState _state = DriverState.Disconnected;
    private DateTimeOffset? _lastEventAt;
    private long _totalEvents;
    private string? _lastError;

    protected BaseConnector(ILogger logger) => Logger = logger;

    public abstract string DriverId { get; }
    public abstract string SourceType { get; }

    protected virtual int MaxConsecutiveFailures => 5;
    protected virtual int RetryDelaySeconds => 10;
    protected virtual double BackoffMultiplier => 1.5;
    protected virtual int MaxRetryDelaySeconds => 120;

    protected abstract Task ConnectCoreAsync(CancellationToken ct);
    protected abstract Task DisconnectCoreAsync(CancellationToken ct);
    protected abstract IAsyncEnumerable<RawChangeEvent> PollOrStreamAsync(CancellationToken ct);

    public async Task ConnectAsync(CancellationToken ct)
    {
        _state = DriverState.Connecting;
        try
        {
            await ConnectCoreAsync(ct);
            _state = DriverState.Connected;
            Logger.LogInformation("Driver {Id} connected", DriverId);
        }
        catch (Exception ex)
        {
            _state = DriverState.Failed;
            _lastError = ex.Message;
            Logger.LogError(ex, "Driver {Id} failed to connect", DriverId);
            throw;
        }
    }

    public async IAsyncEnumerable<RawChangeEvent> StreamChangesAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        _state = DriverState.Streaming;
        var retryDelay = TimeSpan.FromSeconds(Math.Max(RetryDelaySeconds, 1));

        while (!ct.IsCancellationRequested)
        {
            // C# forbids yield return inside try/catch, so we step the enumerator manually:
            // MoveNextAsync is awaited inside try/catch; yield return happens outside it.
            var enumerator = PollOrStreamAsync(ct).GetAsyncEnumerator(ct);
            await using var _ = enumerator;

            RawChangeEvent? ready = null;
            Exception? failure = null;
            bool more = true;

            while (more && !ct.IsCancellationRequested)
            {
                ready = null;
                failure = null;

                try
                {
                    more = await enumerator.MoveNextAsync();
                    if (more)
                    {
                        _consecutiveFailures = 0;
                        retryDelay = TimeSpan.FromSeconds(Math.Max(RetryDelaySeconds, 1));
                        _lastEventAt = DateTimeOffset.UtcNow;
                        Interlocked.Increment(ref _totalEvents);
                        var seq = Interlocked.Increment(ref _sequenceNumber);
                        ready = enumerator.Current with { SequenceNumber = seq };
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    yield break;
                }
                catch (Exception ex)
                {
                    failure = ex;
                    more = false;
                }

                if (ready is not null)
                    yield return ready;

                if (failure is not null)
                {
                    _consecutiveFailures++;
                    _lastError = failure.Message;
                    Logger.LogError(failure, "Driver {Id} stream error (failure {N}/{Max})",
                        DriverId, _consecutiveFailures, MaxConsecutiveFailures);

                    if (_consecutiveFailures >= MaxConsecutiveFailures)
                    {
                        _state = DriverState.Failed;
                        Logger.LogCritical("Driver {Id} exceeded max consecutive failures, stopping", DriverId);
                        yield break;
                    }

                    _state = DriverState.Reconnecting;
                    try
                    {
                        await Task.Delay(retryDelay, ct);
                        retryDelay = TimeSpan.FromMilliseconds(
                            Math.Min(retryDelay.TotalMilliseconds * BackoffMultiplier,
                                     MaxRetryDelaySeconds * 1000.0));
                        await ConnectCoreAsync(ct);
                        _state = DriverState.Streaming;
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        yield break;
                    }
                    catch (Exception reconnectEx)
                    {
                        Logger.LogError(reconnectEx, "Driver {Id} reconnect failed", DriverId);
                    }
                }
            }

            // Inner stream ended cleanly — exit the outer loop too
            if (failure is null)
                yield break;
        }
    }

    public async Task DisconnectAsync(CancellationToken ct)
    {
        try
        {
            await DisconnectCoreAsync(ct);
        }
        finally
        {
            _state = DriverState.Disconnected;
            Logger.LogInformation("Driver {Id} disconnected", DriverId);
        }
    }

    public HealthStatus GetHealth() => new()
    {
        DriverId = DriverId,
        SourceType = SourceType,
        State = _state,
        LastChecked = DateTimeOffset.UtcNow,
        LastEventAt = _lastEventAt,
        TotalEventsEmitted = Interlocked.Read(ref _totalEvents),
        ConsecutiveFailures = _consecutiveFailures,
        LastError = _lastError
    };

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
