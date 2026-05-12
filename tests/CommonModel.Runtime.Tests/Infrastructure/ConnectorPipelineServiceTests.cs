using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Host;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class ConnectorPipelineServiceTests
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ConnectorPipelineService MakeSut(
        IConnectorRegistry? registry = null,
        IEventPipeline? pipeline = null)
    {
        registry ??= Substitute.For<IConnectorRegistry>();
        pipeline ??= Substitute.For<IEventPipeline>();
        return new ConnectorPipelineService(
            registry, pipeline,
            NullLogger<ConnectorPipelineService>.Instance);
    }

    private static ISourceDriver MakeDriver(
        string driverId,
        DriverState state = DriverState.Streaming)
    {
        var driver = Substitute.For<ISourceDriver>();
        driver.DriverId.Returns(driverId);
        driver.SourceType.Returns("test");
        driver.GetHealth().Returns(new HealthStatus
        {
            DriverId   = driverId,
            SourceType = "test",
            State      = state
        });
        driver.ConnectAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        driver.DisconnectAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        driver.StreamChangesAsync(Arg.Any<CancellationToken>())
              .Returns(AsyncEnumerable.Empty<RawChangeEvent>());
        return driver;
    }

    // ── GetAllHealth ──────────────────────────────────────────────────────────

    [Fact]
    public void GetAllHealth_BeforeStart_ReturnsEmpty()
    {
        var sut = MakeSut();
        sut.GetAllHealth().Should().BeEmpty();
    }

    [Fact]
    public async Task GetAllHealth_AfterDriversSet_ReturnsOneEntryPerDriver()
    {
        var d1 = MakeDriver("d1");
        var d2 = MakeDriver("d2");

        var registry = Substitute.For<IConnectorRegistry>();
        registry.ResolveAll().Returns(new List<ISourceDriver> { d1, d2 });

        var sut = MakeSut(registry: registry);

        using var cts = new CancellationTokenSource();
        await sut.StartAsync(cts.Token);
        // Give the background task time to reach _allDrivers = _registry.ResolveAll()
        await Task.Delay(150);

        sut.GetAllHealth().Should().ContainKeys("d1", "d2");

        await sut.StopAsync(cts.Token);
    }

    // ── StopAsync ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task StopAsync_UnknownDriver_ReturnsFalse()
    {
        var sut = MakeSut();
        var result = await sut.StopAsync("no-such-driver");
        result.Should().BeFalse();
    }

    // ── StartAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task StartAsync_UnknownDriver_ReturnsFalse()
    {
        var sut = MakeSut();
        var result = await sut.StartAsync("no-such-driver");
        result.Should().BeFalse();
    }

    // ── RestartAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task RestartAsync_UnknownDriver_ReturnsFalse()
    {
        var sut = MakeSut();
        var result = await sut.RestartAsync("no-such-driver");
        result.Should().BeFalse();
    }

    // ── Pipeline integration ──────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_NoDrivers_LogsWarning_AndCompletes()
    {
        var registry = Substitute.For<IConnectorRegistry>();
        registry.ResolveAll().Returns(new List<ISourceDriver>());

        var sut = MakeSut(registry: registry);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await sut.StartAsync(cts.Token);
        await sut.StopAsync(cts.Token);
    }

    [Fact]
    public async Task ExecuteAsync_SingleEvent_CallsPipeline()
    {
        var evt = new RawChangeEvent
        {
            DriverId   = "d1",
            SourceType = "test",
            EntityPath = "t",
            ChangeType = ChangeType.Insert
        };

        var driver = MakeDriver("d1");
        driver.StreamChangesAsync(Arg.Any<CancellationToken>())
              .Returns(OneEvent(evt));

        var registry = Substitute.For<IConnectorRegistry>();
        registry.ResolveAll().Returns(new List<ISourceDriver> { driver });

        var pipeline = Substitute.For<IEventPipeline>();
        var sut      = MakeSut(registry: registry, pipeline: pipeline);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await sut.StartAsync(cts.Token);

        // Allow the background loop to run
        await Task.Delay(300);
        await sut.StopAsync(cts.Token);

        await pipeline.Received().ProcessAsync(Arg.Is<RawChangeEvent>(e => e.EventId == evt.EventId),
            Arg.Any<CancellationToken>());
    }

    private static async IAsyncEnumerable<RawChangeEvent> OneEvent(RawChangeEvent evt)
    {
        await Task.Yield();
        yield return evt;
    }
}
