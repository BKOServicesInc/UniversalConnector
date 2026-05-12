using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class StartupSelfTestServiceTests
{
    // ── Creds file check ──────────────────────────────────────────────────────

    [Fact]
    public async Task StartAsync_CredsFileConfigured_FileExists_DoesNotStopApp()
    {
        var tmpFile = Path.GetTempFileName();
        try
        {
            var (sut, lifetime) = MakeSut(new NatsOptions
            {
                CredsFile           = tmpFile,
                StopOnCriticalFailure = false  // doesn't matter here — file exists
            });

            await sut.StartAsync(CancellationToken.None);

            lifetime.DidNotReceive().StopApplication();
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [Fact]
    public async Task StartAsync_CredsFileMissing_StopOnCriticalFailure_StopsApp()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".creds");

        var (sut, lifetime) = MakeSut(new NatsOptions
        {
            CredsFile             = missingPath,
            StopOnCriticalFailure = true,
            // Avoid real NATS connection by pointing to unreachable server
            Servers = ["nats://127.0.0.1:9"]
        });

        await sut.StartAsync(CancellationToken.None);

        lifetime.Received().StopApplication();
    }

    [Fact]
    public async Task StartAsync_NullCredsFile_SkipsCredsCheck()
    {
        var (sut, lifetime) = MakeSut(new NatsOptions
        {
            CredsFile             = null,
            StopOnCriticalFailure = false,
            Servers = ["nats://127.0.0.1:9"]  // unreachable, but StopOnCriticalFailure=false
        });

        // Should complete without throwing, even though NATS isn't reachable.
        await sut.StartAsync(CancellationToken.None);
        // App not stopped because StopOnCriticalFailure is false.
        lifetime.DidNotReceive().StopApplication();
    }

    [Fact]
    public async Task StartAsync_NatsUnreachable_StopOnCriticalFailure_False_DoesNotStopApp()
    {
        var (sut, lifetime) = MakeSut(new NatsOptions
        {
            StopOnCriticalFailure = false,
            Servers = ["nats://127.0.0.1:9"]
        });

        await sut.StartAsync(CancellationToken.None);

        lifetime.DidNotReceive().StopApplication();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (StartupSelfTestService sut, IHostApplicationLifetime lifetime) MakeSut(
        NatsOptions? opts = null)
    {
        opts ??= new NatsOptions();
        var factory  = new NatsConnectionFactory(Options.Create(opts));
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var sut = new StartupSelfTestService(
            factory,
            Options.Create(opts),
            lifetime,
            NullLogger<StartupSelfTestService>.Instance);
        return (sut, lifetime);
    }
}
