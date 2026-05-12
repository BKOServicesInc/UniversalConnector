namespace CommonModel.Runtime.Core.Configuration;

public class OntologyCacheOptions
{
    public string EndpointUrl { get; set; } = "";
    public string? GraphIri { get; set; }
    // NATS subject that triggers a cache reload when a message arrives.
    public string RefreshSubject { get; set; } = "cdc.ontology.refresh";
    // When true, the cache loads from Fuseki at startup before the first lookup.
    // Set false to run without Fuseki (cache stays empty).
    public bool LoadOnStartup { get; set; } = false;
}
