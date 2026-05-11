namespace CommonModel.Runtime.Drivers.Generic.Configuration;

public sealed class GenericConnectorOptions
{
    public string DescriptorDirectory { get; set; } = "C:\\Repos\\UniversalConnector\\connectors";
    public bool FailOnDescriptorError { get; set; } = false;
}
