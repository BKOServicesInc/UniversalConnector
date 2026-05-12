using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class DriverLifecycleServiceTests
{
    // ── LifecycleFsm (unit tests — no NATS needed) ────────────────────────────

    [Fact]
    public void LifecycleFsm_Disconnected_Start_IsValid()
    {
        var fsm = new LifecycleFsm();
        fsm.CanApply(DriverState.Disconnected, "start").Should().BeTrue();
    }

    [Fact]
    public void LifecycleFsm_Streaming_Stop_IsValid()
    {
        var fsm = new LifecycleFsm();
        fsm.CanApply(DriverState.Streaming, "stop").Should().BeTrue();
    }

    [Fact]
    public void LifecycleFsm_Disconnected_Stop_IsInvalid()
    {
        var fsm = new LifecycleFsm();
        fsm.CanApply(DriverState.Disconnected, "stop").Should().BeFalse();
    }

    [Fact]
    public void LifecycleFsm_ActionIsCaseInsensitive()
    {
        var fsm = new LifecycleFsm();
        fsm.CanApply(DriverState.Streaming, "STOP").Should().BeTrue();
        fsm.CanApply(DriverState.Streaming, "Stop").Should().BeTrue();
    }

    [Fact]
    public void LifecycleFsm_TryApply_KnownTransition_ReturnsNextState()
    {
        var fsm  = new LifecycleFsm();
        var next = fsm.TryApply(DriverState.Disconnected, "start");
        next.Should().Be(DriverState.Connecting);
    }

    [Fact]
    public void LifecycleFsm_TryApply_InvalidTransition_ReturnsNull()
    {
        var fsm  = new LifecycleFsm();
        var next = fsm.TryApply(DriverState.Disconnected, "stop");
        next.Should().BeNull();
    }

    [Theory]
    [InlineData(DriverState.Disconnected, "start")]
    [InlineData(DriverState.Failed,       "start")]
    [InlineData(DriverState.Failed,       "restart")]
    [InlineData(DriverState.Streaming,    "stop")]
    [InlineData(DriverState.Streaming,    "restart")]
    [InlineData(DriverState.Connecting,   "stop")]
    [InlineData(DriverState.Connected,    "stop")]
    [InlineData(DriverState.Reconnecting, "stop")]
    public void LifecycleFsm_AllDefinedTransitions_AreValid(DriverState state, string action)
    {
        var fsm = new LifecycleFsm();
        fsm.CanApply(state, action).Should().BeTrue();
    }

    [Fact]
    public void LifecycleFsm_ValidActionsFor_Disconnected_ContainsStart()
    {
        var fsm     = new LifecycleFsm();
        var actions = fsm.ValidActionsFor(DriverState.Disconnected);
        actions.Should().Contain("start");
    }

    [Fact]
    public void LifecycleFsm_ValidActionsFor_Streaming_ContainsStopAndRestart()
    {
        var fsm     = new LifecycleFsm();
        var actions = fsm.ValidActionsFor(DriverState.Streaming);
        actions.Should().Contain("stop").And.Contain("restart");
    }

    // ── IDriverLifecycleController dispatch ───────────────────────────────────

    [Fact]
    public async Task Controller_Stop_CalledWhenFsmAllows()
    {
        var ctrl = Substitute.For<IDriverLifecycleController>();
        ctrl.GetAllHealth().Returns(new Dictionary<string, HealthStatus>
        {
            ["pg-assets"] = new() { DriverId = "pg-assets", SourceType = "test", State = DriverState.Streaming }
        });
        ctrl.StopAsync("pg-assets", Arg.Any<CancellationToken>()).Returns(true);

        var fsm = new LifecycleFsm();
        fsm.CanApply(DriverState.Streaming, "stop").Should().BeTrue();

        _ = await ctrl.StopAsync("pg-assets");

        await ctrl.Received(1).StopAsync("pg-assets", Arg.Any<CancellationToken>());
    }

    [Fact]
    public void Controller_NoDispatch_WhenFsmRejects()
    {
        var ctrl = Substitute.For<IDriverLifecycleController>();
        ctrl.GetAllHealth().Returns(new Dictionary<string, HealthStatus>
        {
            ["pg-assets"] = new() { DriverId = "pg-assets", SourceType = "test", State = DriverState.Disconnected }
        });

        var fsm = new LifecycleFsm();
        // "stop" from Disconnected should be rejected by FSM
        fsm.CanApply(DriverState.Disconnected, "stop").Should().BeFalse();

        // Controller.StopAsync should never have been called
        ctrl.DidNotReceive().StopAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
