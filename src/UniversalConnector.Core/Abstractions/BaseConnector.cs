using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Core.Abstractions;

public abstract class BaseConnector : IDataSourceConnector
{
    protected readonly ILogger Logger;
    private long _sequenceNumber;
    private int _consecutiveFailures;
    private ConnectorState _state = ConnectorState.Disconnected;
    private DateTimeOffset? _lastEventAt;
    private long _totalEvents;
    private string? _lastError;

    protected BaseConnector(ILogger logger) => Logger = logger;

    public abstract string ConnectorId { get; }
    public abstract string SourceType { get; }

    protected virtual int MaxConsecutiveFailures => 5;
    protected virtual int RetryDelaySeconds => 10;
    protected virtual double BackoffMultiplier => 1.5;
    protected virtual int MaxRetryDelaySeconds => 120;

    protected abstract Task ConnectCoreAsync(CancellationToken ct);
    protected abstract Task DisconnectCoreAsync(CancellationToken ct);
    protected abstract IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(CancellationToken ct);

    public async Task ConnectAsync(CancellationToken ct)
    {
        _state = ConnectorState.Connecting;
        try
        {
            await ConnectCoreAsync(ct);
            _state = ConnectorState.Connected;
            Logger.LogInformation("Connector {Id} connected", ConnectorId);
        }
        catch (Exception ex)
        {
            _state = ConnectorState.Failed;
            _lastError = ex.Message;
            Logger.LogError(ex, "Connector {Id} failed to connect", ConnectorId);
            throw;
        }
    }

    public async IAsyncEnumerable<DataChangeEvent> StreamChangesAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        _state = ConnectorState.Streaming;
        var retryDelay = TimeSpan.FromSeconds(Math.Max(RetryDelaySeconds, 1));

        while (!ct.IsCancellationRequested)
        {
            // C# forbids yield return inside try/catch, so we step the enumerator manually:
            // MoveNextAsync is awaited inside try/catch; yield return happens outside it.
            var enumerator = PollOrStreamAsync(ct).GetAsyncEnumerator(ct);
            await using var _ = enumerator;

            DataChangeEvent? ready = null;
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
                    Logger.LogError(failure, "Connector {Id} stream error (failure {N}/{Max})",
                        ConnectorId, _consecutiveFailures, MaxConsecutiveFailures);

                    if (_consecutiveFailures >= MaxConsecutiveFailures)
                    {
                        _state = ConnectorState.Failed;
                        Logger.LogCritical("Connector {Id} exceeded max consecutive failures, stopping", ConnectorId);
                        yield break;
                    }

                    _state = ConnectorState.Reconnecting;
                    try
                    {
                        await Task.Delay(retryDelay, ct);
                        retryDelay = TimeSpan.FromMilliseconds(
                            Math.Min(retryDelay.TotalMilliseconds * BackoffMultiplier,
                                     MaxRetryDelaySeconds * 1000.0));
                        await ConnectCoreAsync(ct);
                        _state = ConnectorState.Streaming;
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        yield break;
                    }
                    catch (Exception reconnectEx)
                    {
                        Logger.LogError(reconnectEx, "Connector {Id} reconnect failed", ConnectorId);
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
            _state = ConnectorState.Disconnected;
            Logger.LogInformation("Connector {Id} disconnected", ConnectorId);
        }
    }

    public ConnectorHealthReport GetHealthReport() => new()
    {
        ConnectorId = ConnectorId,
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
