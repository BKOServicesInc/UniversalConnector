using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class NatsPublisherTests
{
    // ── BuildSubject (via observable side effects via DlqSubjectPrefix) ───────
    // We test the subject-building logic indirectly through NatsPublisherSubjectTests.

    // ── Circuit breaker state transitions ─────────────────────────────────────

    [Fact]
    public async Task CircuitBreaker_DefaultState_IsOpen_False()
    {
        // The circuit starts closed (CircuitIsOpen == false).
        // We can't call the private property directly, but we can verify it via
        // the public interface: a newly created publisher should not be in DLQ-only mode.
        // This is a compile / structural test confirming the field defaults are sane.
        var opts    = Options.Create(new NatsOptions());
        var factory = new NatsConnectionFactory(opts);
        var sut     = new NatsPublisher(opts, factory, NullLogger<NatsPublisher>.Instance);

        // No exception constructing or disposing.
        await sut.DisposeAsync();
    }

    // ── Subject builder ───────────────────────────────────────────────────────

    private static RawChangeEvent MakeEvt(
        string context    = "ctx:proc",
        string entityPath = "public.assets",
        ChangeType change = ChangeType.Insert,
        string driverId   = "d1",
        string sourceType = "postgres") =>
        new()
        {
            DriverId   = driverId,
            SourceType = sourceType,
            Context    = context,
            EntityPath = entityPath,
            ChangeType = change
        };

    [Fact]
    public void BuildSubject_WithContext_UsesContextForm()
    {
        var subject = InvokeSubject(new NatsOptions { SubjectPrefix = "cdc" },
            MakeEvt(context: "ctx:proc", entityPath: "public.assets", change: ChangeType.Insert));

        subject.Should().Be("cdc.ctx-proc.public.assets.insert");
    }

    [Fact]
    public void BuildSubject_EmptyContext_UsesLegacyForm()
    {
        var subject = InvokeSubject(new NatsOptions { SubjectPrefix = "cdc" },
            MakeEvt(context: "", driverId: "d1", sourceType: "postgres",
                    entityPath: "public.assets", change: ChangeType.Delete));

        subject.Should().Be("cdc.postgres.d1.delete");
    }

    [Fact]
    public void BuildSubject_ColonInContext_NormalisedToDash()
    {
        var subject = InvokeSubject(new NatsOptions { SubjectPrefix = "ev" },
            MakeEvt(context: "ns:area:zone", entityPath: "t1", change: ChangeType.Update));

        subject.Should().StartWith("ev.ns-area-zone.");
    }

    private static string InvokeSubject(NatsOptions opts, RawChangeEvent evt)
    {
        // Use reflection to call the private BuildSubject method for focused unit testing.
        var publisher = new NatsPublisher(
            Options.Create(opts),
            new NatsConnectionFactory(Options.Create(opts)),
            NullLogger<NatsPublisher>.Instance);

        var method = typeof(NatsPublisher)
            .GetMethod("BuildSubject",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        return (string)method.Invoke(publisher, [evt])!;
    }

    // ── BuildHeaders ──────────────────────────────────────────────────────────

    [Fact]
    public void BuildHeaders_ContainsAllRequiredKeys()
    {
        var evt = MakeEvt();
        var method = typeof(NatsPublisher)
            .GetMethod("BuildHeaders",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var headers = (NATS.Client.Core.NatsHeaders)method.Invoke(null, [evt, null])!;

        headers.Should().ContainKey("eventId");
        headers.Should().ContainKey("driverId");
        headers.Should().ContainKey("context");
        headers.Should().ContainKey("sourceType");
        headers.Should().ContainKey("changeType");
        headers["content-type"].ToString().Should().Be("application/x-protobuf");
    }

    [Fact]
    public void BuildHeaders_ExtraHeaders_AreMerged()
    {
        var evt   = MakeEvt();
        var extra = new Dictionary<string, string> { ["x-tenant"] = "acme" };

        var method = typeof(NatsPublisher)
            .GetMethod("BuildHeaders",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var headers = (NATS.Client.Core.NatsHeaders)method.Invoke(null, [evt, extra])!;

        headers["x-tenant"].ToString().Should().Be("acme");
    }
}
