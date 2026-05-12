using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class HealthHeartbeatTests
{
    private static HealthHeartbeatService MakeSut(
        IDriverLifecycleController? controller = null,
        HeartbeatOptions? opts = null)
    {
        controller ??= Substitute.For<IDriverLifecycleController>();
        var heartbeatOpts = Options.Create(opts ?? new HeartbeatOptions());
        var factory       = new NatsConnectionFactory(Options.Create(new NatsOptions()));
        return new HealthHeartbeatService(
            controller, heartbeatOpts, factory,
            NullLogger<HealthHeartbeatService>.Instance);
    }

    private static HealthStatus MakeHealth(string driverId, DriverState state = DriverState.Streaming) =>
        new()
        {
            DriverId            = driverId,
            SourceType          = "postgres",
            State               = state,
            TotalEventsEmitted  = 42,
            ConsecutiveFailures = 0
        };

    // ── Subject formula ───────────────────────────────────────────────────────

    [Fact]
    public void BuildSubject_UsesPrefix_AndLowercasesDriverId()
    {
        var sut = MakeSut(opts: new HeartbeatOptions { SubjectPrefix = "cdc.health" });
        sut.BuildSubject("PG-Assets").Should().Be("cdc.health.pg-assets");
    }

    [Fact]
    public void BuildSubject_DefaultPrefix_IsHealthPrefix()
    {
        var sut = MakeSut();
        sut.BuildSubject("my-driver").Should().StartWith("cdc.health.");
    }

    [Fact]
    public void BuildSubject_DriverIdPreservesDashes()
    {
        var sut = MakeSut();
        sut.BuildSubject("pg-assets-polling").Should().Be("cdc.health.pg-assets-polling");
    }

    // ── HealthStatus JSON serialization ───────────────────────────────────────

    [Fact]
    public void HealthStatus_SerializesWithCamelCase()
    {
        var health = MakeHealth("pg-assets", DriverState.Streaming);
        var json   = JsonSerializer.Serialize(health, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        json.Should().Contain("\"driverId\"");
        json.Should().Contain("\"sourceType\"");
        json.Should().Contain("\"state\"");
        json.Should().Contain("\"totalEventsEmitted\"");
    }

    [Fact]
    public void HealthStatus_AllDriverStates_Serialize()
    {
        foreach (var state in Enum.GetValues<DriverState>())
        {
            var health = MakeHealth("d1", state);
            var act = () => JsonSerializer.Serialize(health);
            act.Should().NotThrow($"DriverState.{state} should serialize without error");
        }
    }

    // ── LifecycleEvent JSON ────────────────────────────────────────────────────

    [Fact]
    public void LifecycleEvent_Serializes_WithNullPreviousState()
    {
        var evt = new LifecycleEvent
        {
            DriverId         = "pg-assets",
            State            = DriverState.Streaming,
            PreviousState    = null,
            TriggeringAction = "start"
        };

        var json   = JsonSerializer.Serialize(evt);
        var parsed = JsonSerializer.Deserialize<LifecycleEvent>(json)!;

        parsed.DriverId.Should().Be("pg-assets");
        parsed.State.Should().Be(DriverState.Streaming);
        parsed.PreviousState.Should().BeNull();
    }

    [Fact]
    public void LifecycleEvent_RoundTrip_PreservesAllFields()
    {
        var evt = new LifecycleEvent
        {
            DriverId         = "my-driver",
            State            = DriverState.Disconnected,
            PreviousState    = DriverState.Streaming,
            TriggeringAction = "stop",
            CommandId        = "01HXY123"
        };

        var json   = JsonSerializer.Serialize(evt);
        var parsed = JsonSerializer.Deserialize<LifecycleEvent>(json)!;

        parsed.State.Should().Be(DriverState.Disconnected);
        parsed.PreviousState.Should().Be(DriverState.Streaming);
        parsed.TriggeringAction.Should().Be("stop");
        parsed.CommandId.Should().Be("01HXY123");
    }

    // ── HeartbeatOptions defaults ─────────────────────────────────────────────

    [Fact]
    public void HeartbeatOptions_DefaultInterval_Is30Seconds()
    {
        new HeartbeatOptions().IntervalSeconds.Should().Be(30);
    }

    [Fact]
    public void HeartbeatOptions_DefaultSubjectPrefix_IsCdcHealth()
    {
        new HeartbeatOptions().SubjectPrefix.Should().Be("cdc.health");
    }

    [Fact]
    public void HeartbeatOptions_DefaultUseJetStream_IsTrue()
    {
        new HeartbeatOptions().UseJetStream.Should().BeTrue();
    }
}
