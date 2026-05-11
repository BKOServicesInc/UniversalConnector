using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Drivers.Generic.Engine;

namespace CommonModel.Runtime.Tests.Engine;

public class DescriptorStoreTests
{
    private readonly DescriptorStore _sut = new();

    [Fact]
    public void Register_ThenGet_ReturnsDescriptor()
    {
        var d = Descriptor("conn-1");
        _sut.Register(d);
        _sut.Get("conn-1").Should().BeSameAs(d);
    }

    [Fact]
    public void Get_UnknownId_ReturnsNull()
    {
        _sut.Get("does-not-exist").Should().BeNull();
    }

    [Fact]
    public void Get_IsCaseInsensitive()
    {
        _sut.Register(Descriptor("MyConnector"));
        _sut.Get("myconnector").Should().NotBeNull();
        _sut.Get("MYCONNECTOR").Should().NotBeNull();
    }

    [Fact]
    public void Register_SameId_OverwritesPrevious()
    {
        var first  = Descriptor("conn");
        var second = Descriptor("conn");
        _sut.Register(first);
        _sut.Register(second);
        _sut.Get("conn").Should().BeSameAs(second);
        _sut.Count.Should().Be(1);
    }

    [Fact]
    public void GetAll_ReturnsAllRegistered()
    {
        _sut.Register(Descriptor("a"));
        _sut.Register(Descriptor("b"));
        _sut.Register(Descriptor("c"));
        _sut.GetAll().Should().HaveCount(3);
    }

    [Fact]
    public void GetEnabled_ReturnsOnlyEnabledDescriptors()
    {
        _sut.Register(Descriptor("enabled-1", enabled: true));
        _sut.Register(Descriptor("enabled-2", enabled: true));
        _sut.Register(Descriptor("disabled",  enabled: false));

        _sut.GetEnabled().Should().HaveCount(2);
        _sut.GetEnabled().Should().AllSatisfy(d => d.Enabled.Should().BeTrue());
    }

    [Fact]
    public void Remove_ExistingId_ReturnsTrueAndRemoves()
    {
        _sut.Register(Descriptor("to-remove"));
        _sut.Remove("to-remove").Should().BeTrue();
        _sut.Get("to-remove").Should().BeNull();
    }

    [Fact]
    public void Remove_UnknownId_ReturnsFalse()
    {
        _sut.Remove("ghost").Should().BeFalse();
    }

    [Fact]
    public void Count_ReflectsRegistrations()
    {
        _sut.Count.Should().Be(0);
        _sut.Register(Descriptor("a"));
        _sut.Count.Should().Be(1);
        _sut.Register(Descriptor("b"));
        _sut.Count.Should().Be(2);
        _sut.Remove("a");
        _sut.Count.Should().Be(1);
    }

    private static ConnectorDescriptor Descriptor(string id, bool enabled = true) =>
        new() { DriverId = id, Context = "ctx:test", SourceType = "postgres", Enabled = enabled };
}
