using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Tests.TestHelpers;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class CheckpointStoreTests
{
    private readonly InMemoryCheckpointStore _sut = new();

    [Fact]
    public async Task GetAsync_UnknownKey_ReturnsNull()
    {
        var result = await _sut.GetAsync("no-driver", "no.entity");

        result.Should().BeNull();
    }

    [Fact]
    public async Task SaveAsync_ThenGetAsync_ReturnsSavedCheckpoint()
    {
        var cp = new Checkpoint
        {
            DriverId   = "pg-assets",
            EntityPath = "public.assets",
            Position   = "2024-01-15T10:30:00Z"
        };

        await _sut.SaveAsync(cp);
        var result = await _sut.GetAsync(cp.DriverId, cp.EntityPath);

        result.Should().NotBeNull();
        result!.DriverId.Should().Be("pg-assets");
        result.EntityPath.Should().Be("public.assets");
        result.Position.Should().Be("2024-01-15T10:30:00Z");
    }

    [Fact]
    public async Task SaveAsync_Overwrite_ReturnsLatestPosition()
    {
        var first = new Checkpoint { DriverId = "d1", EntityPath = "e1", Position = "pos-1" };
        var second = new Checkpoint { DriverId = "d1", EntityPath = "e1", Position = "pos-2" };

        await _sut.SaveAsync(first);
        await _sut.SaveAsync(second);
        var result = await _sut.GetAsync("d1", "e1");

        result!.Position.Should().Be("pos-2");
    }

    [Fact]
    public async Task SaveAsync_DifferentEntities_StoredIndependently()
    {
        var a = new Checkpoint { DriverId = "d1", EntityPath = "table_a", Position = "pos-a" };
        var b = new Checkpoint { DriverId = "d1", EntityPath = "table_b", Position = "pos-b" };

        await _sut.SaveAsync(a);
        await _sut.SaveAsync(b);

        (await _sut.GetAsync("d1", "table_a"))!.Position.Should().Be("pos-a");
        (await _sut.GetAsync("d1", "table_b"))!.Position.Should().Be("pos-b");
        _sut.Count.Should().Be(2);
    }

    [Fact]
    public async Task SaveAsync_DifferentDrivers_StoredIndependently()
    {
        var a = new Checkpoint { DriverId = "driver-a", EntityPath = "table_x", Position = "a-pos" };
        var b = new Checkpoint { DriverId = "driver-b", EntityPath = "table_x", Position = "b-pos" };

        await _sut.SaveAsync(a);
        await _sut.SaveAsync(b);

        (await _sut.GetAsync("driver-a", "table_x"))!.Position.Should().Be("a-pos");
        (await _sut.GetAsync("driver-b", "table_x"))!.Position.Should().Be("b-pos");
    }

    [Fact]
    public async Task SaveAsync_SetsUpdatedAt_ToApproximatelyNow()
    {
        var before = DateTimeOffset.UtcNow;
        await _sut.SaveAsync(new Checkpoint { DriverId = "d", EntityPath = "e", Position = "p" });
        var after = DateTimeOffset.UtcNow;

        var result = await _sut.GetAsync("d", "e");

        result!.UpdatedAt.Should().BeOnOrAfter(before).And.BeOnOrBefore(after);
    }
}
