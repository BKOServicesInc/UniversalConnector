using CommonModel.Runtime.Core.Models;
using CommonModel.Runtime.Infrastructure;

namespace CommonModel.Runtime.Tests.Infrastructure;

public class OntologyCacheTests
{
    private static OntologyEntry Class(string iri, string? label = null, string? parent = null) =>
        new() { Iri = iri, Label = label, ParentIri = parent, Type = "class" };

    private static OntologyEntry Prop(string iri, string? label = null) =>
        new() { Iri = iri, Label = label, Type = "property" };

    // ── GetByIriAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task GetByIriAsync_KnownIri_ReturnsEntry()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump", "Pump"));

        var result = await sut.GetByIriAsync("pid:Pump");

        result.Should().NotBeNull();
        result!.Iri.Should().Be("pid:Pump");
        result.Label.Should().Be("Pump");
        result.Type.Should().Be("class");
    }

    [Fact]
    public async Task GetByIriAsync_UnknownIri_ReturnsNull()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump", "Pump"));

        var result = await sut.GetByIriAsync("pid:Unknown");

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetByIriAsync_IriLookupIsCaseSensitive()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump", "Pump"));

        var result = await sut.GetByIriAsync("pid:pump");

        result.Should().BeNull();
    }

    // ── FindByLabelAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task FindByLabelAsync_KnownLabel_ReturnsMatches()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump", "Pump"), Class("ns:WaterPump", "Pump"));

        var results = await sut.FindByLabelAsync("Pump");

        results.Should().HaveCount(2);
        results.Select(r => r.Iri).Should().Contain(["pid:Pump", "ns:WaterPump"]);
    }

    [Fact]
    public async Task FindByLabelAsync_LabelLookupIsCaseInsensitive()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump", "Pump"));

        var results = await sut.FindByLabelAsync("pump");

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task FindByLabelAsync_UnknownLabel_ReturnsEmpty()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump", "Pump"));

        var results = await sut.FindByLabelAsync("Turbine");

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task FindByLabelAsync_EntryWithoutLabel_NotIndexedByLabel()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(new OntologyEntry { Iri = "pid:X", Type = "class" }); // no label

        var results = await sut.FindByLabelAsync("pid:X");

        results.Should().BeEmpty();
    }

    // ── ContainsAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ContainsAsync_KnownIri_ReturnsTrue()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump"));

        (await sut.ContainsAsync("pid:Pump")).Should().BeTrue();
    }

    [Fact]
    public async Task ContainsAsync_UnknownIri_ReturnsFalse()
    {
        var sut = new InMemoryOntologyCache();

        (await sut.ContainsAsync("pid:Ghost")).Should().BeFalse();
    }

    // ── RefreshAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task RefreshAsync_ClearsAllEntries()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:Pump"), Class("pid:Valve"));

        await sut.RefreshAsync();

        sut.Count.Should().Be(0);
        (await sut.ContainsAsync("pid:Pump")).Should().BeFalse();
    }

    // ── Hierarchy / parent ─────────────────────────────────────────────────────

    [Fact]
    public async Task GetByIriAsync_WithParent_ExposesParentIri()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(Class("pid:CentrifugalPump", "Centrifugal Pump", parent: "pid:Pump"));

        var result = await sut.GetByIriAsync("pid:CentrifugalPump");

        result!.ParentIri.Should().Be("pid:Pump");
    }

    // ── Mixed types ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Seed_MixedTypes_AllStoredByIri()
    {
        var sut = new InMemoryOntologyCache();
        sut.Seed(
            Class("pid:Pump", "Pump"),
            Prop("pid:hasTag", "has tag"));

        sut.Count.Should().Be(2);
        (await sut.GetByIriAsync("pid:hasTag"))!.Type.Should().Be("property");
    }
}
