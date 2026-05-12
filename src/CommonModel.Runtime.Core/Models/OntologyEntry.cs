namespace CommonModel.Runtime.Core.Models;

public sealed record OntologyEntry
{
    public required string Iri { get; init; }
    public string? Label { get; init; }
    public string? ParentIri { get; init; }
    // "class" | "property" | "individual"
    public string? Type { get; init; }
}
