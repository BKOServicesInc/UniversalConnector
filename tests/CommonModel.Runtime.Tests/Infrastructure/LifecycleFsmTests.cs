using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class LifecycleFsmTests
{
    private readonly LifecycleFsm _sut = new();

    // ── CanApply ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(DriverState.Disconnected, "start",   true)]
    [InlineData(DriverState.Disconnected, "stop",    false)]
    [InlineData(DriverState.Disconnected, "restart", false)]
    [InlineData(DriverState.Failed,       "start",   true)]
    [InlineData(DriverState.Failed,       "restart", true)]
    [InlineData(DriverState.Failed,       "stop",    false)]
    [InlineData(DriverState.Connecting,   "stop",    true)]
    [InlineData(DriverState.Connecting,   "start",   false)]
    [InlineData(DriverState.Connected,    "stop",    true)]
    [InlineData(DriverState.Connected,    "start",   false)]
    [InlineData(DriverState.Streaming,    "stop",    true)]
    [InlineData(DriverState.Streaming,    "restart", true)]
    [InlineData(DriverState.Streaming,    "start",   false)]
    [InlineData(DriverState.Reconnecting, "stop",    true)]
    [InlineData(DriverState.Reconnecting, "start",   false)]
    public void CanApply_ValidatesTransitions(DriverState state, string action, bool expected)
    {
        _sut.CanApply(state, action).Should().Be(expected);
    }

    [Fact]
    public void CanApply_ActionIsCaseInsensitive()
    {
        _sut.CanApply(DriverState.Streaming, "STOP").Should().BeTrue();
        _sut.CanApply(DriverState.Streaming, "Stop").Should().BeTrue();
    }

    [Fact]
    public void CanApply_UnknownAction_ReturnsFalse()
    {
        _sut.CanApply(DriverState.Streaming, "pause").Should().BeFalse();
    }

    // ── TryApply ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(DriverState.Disconnected, "start",   DriverState.Connecting)]
    [InlineData(DriverState.Failed,       "start",   DriverState.Connecting)]
    [InlineData(DriverState.Failed,       "restart", DriverState.Connecting)]
    [InlineData(DriverState.Connecting,   "stop",    DriverState.Disconnected)]
    [InlineData(DriverState.Connected,    "stop",    DriverState.Disconnected)]
    [InlineData(DriverState.Streaming,    "stop",    DriverState.Disconnected)]
    [InlineData(DriverState.Streaming,    "restart", DriverState.Connecting)]
    [InlineData(DriverState.Reconnecting, "stop",    DriverState.Disconnected)]
    public void TryApply_ValidTransition_ReturnsExpectedNextState(
        DriverState current, string action, DriverState expected)
    {
        _sut.TryApply(current, action).Should().Be(expected);
    }

    [Fact]
    public void TryApply_InvalidTransition_ReturnsNull()
    {
        _sut.TryApply(DriverState.Disconnected, "stop").Should().BeNull();
    }

    [Fact]
    public void TryApply_UnknownAction_ReturnsNull()
    {
        _sut.TryApply(DriverState.Streaming, "pause").Should().BeNull();
    }

    // ── ValidActionsFor ───────────────────────────────────────────────────────

    [Fact]
    public void ValidActionsFor_Disconnected_OnlyStart()
    {
        _sut.ValidActionsFor(DriverState.Disconnected).Should().BeEquivalentTo(["start"]);
    }

    [Fact]
    public void ValidActionsFor_Streaming_StopAndRestart()
    {
        _sut.ValidActionsFor(DriverState.Streaming).Should().BeEquivalentTo(["stop", "restart"]);
    }

    [Fact]
    public void ValidActionsFor_Failed_StartAndRestart()
    {
        _sut.ValidActionsFor(DriverState.Failed).Should().BeEquivalentTo(["start", "restart"]);
    }

    [Fact]
    public void ValidActionsFor_Connecting_OnlyStop()
    {
        _sut.ValidActionsFor(DriverState.Connecting).Should().BeEquivalentTo(["stop"]);
    }

    // ── Round-trip consistency ────────────────────────────────────────────────

    [Fact]
    public void CanApply_AndTryApply_AreConsistent()
    {
        // Every state + action where CanApply=true must produce a non-null TryApply result
        var allStates  = Enum.GetValues<DriverState>();
        var allActions = new[] { "start", "stop", "restart" };

        foreach (var state in allStates)
        foreach (var action in allActions)
        {
            var can    = _sut.CanApply(state, action);
            var result = _sut.TryApply(state, action);
            can.Should().Be(result.HasValue,
                $"CanApply({state}, {action})={can} but TryApply returned {result}");
        }
    }
}
