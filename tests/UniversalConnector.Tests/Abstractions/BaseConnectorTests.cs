using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using NSubstitute;
using UniversalConnector.Core.Abstractions;
using UniversalConnector.Core.Models;

namespace UniversalConnector.Tests.Abstractions;

public class BaseConnectorTests
{
    // ── Sequence numbers ──────────────────────────────────────────────────────

    [Fact]
    public async Task StreamChangesAsync_AssignsIncrementingSequenceNumbers()
    {
        var connector = new StubConnector(Events(3));

        var results = await CollectAsync(connector, maxItems: 3);

        results.Select(e => e.SequenceNumber).Should().BeInAscendingOrder();
        results[0].SequenceNumber.Should().Be(1);
        results[1].SequenceNumber.Should().Be(2);
        results[2].SequenceNumber.Should().Be(3);
    }

    // ── Clean stream completion ───────────────────────────────────────────────

    [Fact]
    public async Task StreamChangesAsync_StreamEndsCleanly_StopsYielding()
    {
        var connector = new StubConnector(Events(2));

        var results = await CollectAsync(connector, maxItems: 10);

        results.Should().HaveCount(2);
    }

    // ── Cancellation ─────────────────────────────────────────────────────────

    [Fact]
    public async Task StreamChangesAsync_Cancelled_StopsGracefully()
    {
        using var cts = new CancellationTokenSource();
        var connector = new StubConnector(InfiniteEvents());

        var results = new List<DataChangeEvent>();
        await foreach (var evt in connector.StreamChangesAsync(cts.Token))
        {
            results.Add(evt);
            if (results.Count >= 3) cts.Cancel();
        }

        results.Should().HaveCountGreaterThanOrEqualTo(3);
    }

    // ── Failure and retry ─────────────────────────────────────────────────────

    [Fact]
    public async Task StreamChangesAsync_TransientError_RetriesAndContinues()
    {
        // Fails once then succeeds
        var connector = new StubConnector(
            failFirstN: 1,
            retryDelaySeconds: 0,
            events: Events(2));

        var results = await CollectAsync(connector, maxItems: 2, timeoutMs: 5000);

        results.Should().HaveCount(2);
        connector.ConnectCallCount.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task StreamChangesAsync_ExceedsMaxFailures_Stops()
    {
        var connector = new StubConnector(
            alwaysFail: true,
            maxFailures: 3,
            retryDelaySeconds: 0);

        var results = await CollectAsync(connector, maxItems: 100, timeoutMs: 5000);

        results.Should().BeEmpty();
        connector.ConnectCallCount.Should().BeGreaterThanOrEqualTo(3);
    }

    // ── Health report ─────────────────────────────────────────────────────────

    [Fact]
    public async Task GetHealthReport_AfterEvents_ReflectsCorrectCount()
    {
        var connector = new StubConnector(Events(3));
        await CollectAsync(connector, maxItems: 3);

        var report = connector.GetHealthReport();

        report.TotalEventsEmitted.Should().Be(3);
        report.ConnectorId.Should().Be("stub");
        report.SourceType.Should().Be("test");
    }

    [Fact]
    public async Task GetHealthReport_AfterFailure_RecordsError()
    {
        var connector = new StubConnector(alwaysFail: true, maxFailures: 1, retryDelaySeconds: 0);
        await CollectAsync(connector, maxItems: 5, timeoutMs: 2000);

        var report = connector.GetHealthReport();

        report.LastError.Should().NotBeNullOrEmpty();
        report.ConsecutiveFailures.Should().BeGreaterThan(0);
    }

    [Fact]
    public void GetHealthReport_BeforeConnect_ShowsDisconnectedState()
    {
        var connector = new StubConnector(Events(0));
        var report    = connector.GetHealthReport();

        report.State.Should().Be(ConnectorState.Disconnected);
        report.TotalEventsEmitted.Should().Be(0);
    }

    // ── Connect / Disconnect ──────────────────────────────────────────────────

    [Fact]
    public async Task ConnectAsync_CallsConnectCore()
    {
        var connector = new StubConnector(Events(0));
        await connector.ConnectAsync(CancellationToken.None);

        connector.ConnectCallCount.Should().Be(1);
    }

    [Fact]
    public async Task DisconnectAsync_CallsDisconnectCore()
    {
        var connector = new StubConnector(Events(0));
        await connector.DisconnectAsync(CancellationToken.None);

        connector.DisconnectCallCount.Should().Be(1);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<List<DataChangeEvent>> CollectAsync(
        StubConnector connector,
        int maxItems,
        int timeoutMs = 2000)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        var results   = new List<DataChangeEvent>();

        try
        {
            await connector.ConnectAsync(cts.Token);

            await foreach (var evt in connector.StreamChangesAsync(cts.Token))
            {
                results.Add(evt);
                if (results.Count >= maxItems) break;
            }
        }
        catch (OperationCanceledException) { }

        return results;
    }

    private static IEnumerable<DataChangeEvent> Events(int count) =>
        Enumerable.Range(1, count).Select(i => new DataChangeEvent
        {
            SourceType = "test",
            ConnectorId = "stub",
            EntityPath = "public.items",
            ChangeType = ChangeType.Insert,
            PrimaryKey = new Dictionary<string, object?> { ["id"] = i },
            Payload    = new Dictionary<string, object?> { ["value"] = i }
        });

    private static IEnumerable<DataChangeEvent> InfiniteEvents()
    {
        var i = 0;
        while (true) yield return new DataChangeEvent
        {
            SourceType  = "test",
            ConnectorId = "stub",
            EntityPath  = "public.items",
            ChangeType  = ChangeType.Insert,
            PrimaryKey  = new Dictionary<string, object?> { ["id"] = ++i },
            Payload     = new Dictionary<string, object?> { ["value"] = i }
        };
    }

    // ── Stub connector ────────────────────────────────────────────────────────

    private sealed class StubConnector : BaseConnector
    {
        private readonly IEnumerable<DataChangeEvent> _events;
        private readonly bool _alwaysFail;
        private int _failFirstN;
        private int _failCount;

        public int ConnectCallCount    { get; private set; }
        public int DisconnectCallCount { get; private set; }

        protected override int MaxConsecutiveFailures { get; }
        protected override int RetryDelaySeconds      { get; }

        public StubConnector(
            IEnumerable<DataChangeEvent>? events = null,
            bool alwaysFail = false,
            int failFirstN = 0,
            int maxFailures = 5,
            int retryDelaySeconds = 0)
            : base(Substitute.For<ILogger>())
        {
            _events       = events ?? Enumerable.Empty<DataChangeEvent>();
            _alwaysFail   = alwaysFail;
            _failFirstN   = failFirstN;
            MaxConsecutiveFailures = maxFailures;
            RetryDelaySeconds      = retryDelaySeconds;
        }

        public override string ConnectorId => "stub";
        public override string SourceType  => "test";

        protected override Task ConnectCoreAsync(CancellationToken ct)
        {
            ConnectCallCount++;
            return Task.CompletedTask;
        }

        protected override Task DisconnectCoreAsync(CancellationToken ct)
        {
            DisconnectCallCount++;
            return Task.CompletedTask;
        }

        protected override async IAsyncEnumerable<DataChangeEvent> PollOrStreamAsync(
            [EnumeratorCancellation] CancellationToken ct)
        {
            if (_alwaysFail)
                throw new InvalidOperationException("Simulated stream failure");

            if (_failFirstN > 0)
            {
                _failCount++;
                if (_failCount <= _failFirstN)
                    throw new InvalidOperationException($"Simulated transient failure #{_failCount}");
            }

            foreach (var evt in _events)
            {
                ct.ThrowIfCancellationRequested();
                yield return evt;
                await Task.Yield();
            }
        }
    }
}
