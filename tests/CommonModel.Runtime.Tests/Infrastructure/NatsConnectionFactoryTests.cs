using Microsoft.Extensions.Options;
using NATS.Client.Core;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class NatsConnectionFactoryTests
{
    private static NatsConnectionFactory MakeSut(NatsOptions? opts = null) =>
        new(Options.Create(opts ?? new NatsOptions()));

    // ── URL assembly ─────────────────────────────────────────────────────────

    [Fact]
    public void BuildOpts_SingleServer_SetsUrl()
    {
        var sut = MakeSut(new NatsOptions { Servers = ["nats://localhost:4222"] });
        sut.BuildOpts().Url.Should().Be("nats://localhost:4222");
    }

    [Fact]
    public void BuildOpts_MultipleServers_JoinsWithComma()
    {
        var sut = MakeSut(new NatsOptions
        {
            Servers = ["nats://host1:4222", "nats://host2:4222"]
        });
        sut.BuildOpts().Url.Should().Be("nats://host1:4222,nats://host2:4222");
    }

    // ── Auth / creds wiring ──────────────────────────────────────────────────

    [Fact]
    public void BuildOpts_NullCredsFile_AuthOptsIsDefault()
    {
        var sut = MakeSut(new NatsOptions { CredsFile = null });
        sut.BuildOpts().AuthOpts.Should().Be(NatsAuthOpts.Default);
    }

    [Fact]
    public void BuildOpts_EmptyCredsFile_AuthOptsIsDefault()
    {
        var sut = MakeSut(new NatsOptions { CredsFile = "" });
        sut.BuildOpts().AuthOpts.Should().Be(NatsAuthOpts.Default);
    }

    [Fact]
    public void BuildOpts_WhitespaceCredsFile_AuthOptsIsDefault()
    {
        var sut = MakeSut(new NatsOptions { CredsFile = "   " });
        sut.BuildOpts().AuthOpts.Should().Be(NatsAuthOpts.Default);
    }

    [Fact]
    public void BuildOpts_CredsFileSet_WiredIntoAuthOpts()
    {
        const string path = "/etc/nats/creds/connector.creds";
        var sut = MakeSut(new NatsOptions { CredsFile = path });
        sut.BuildOpts().AuthOpts.CredsFile.Should().Be(path);
    }

    // ── Immutability — BuildOpts returns a fresh value each call ────────────

    [Fact]
    public void BuildOpts_CalledTwice_ReturnsDifferentInstances()
    {
        var sut = MakeSut();
        var a = sut.BuildOpts();
        var b = sut.BuildOpts();
        a.Should().NotBeSameAs(b);
    }
}
