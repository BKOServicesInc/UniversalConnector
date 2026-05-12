namespace CommonModel.Runtime.Drivers.Generic.Configuration;

public sealed class GenericConnectorOptions
{
    public string DescriptorDirectory { get; set; } = "connectors";
    public bool FailOnDescriptorError { get; set; } = true;
}
