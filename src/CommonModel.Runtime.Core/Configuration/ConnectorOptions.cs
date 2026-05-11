namespace CommonModel.Runtime.Core.Configuration;

public class ConnectorOptions
{
    public string ConnectorId { get; set; } = "";
    public string SourceType { get; set; } = "";
    public bool Enabled { get; set; } = true;
    public string? Description { get; set; }
    public int MaxConsecutiveFailures { get; set; } = 5;
    public int RetryDelaySeconds { get; set; } = 10;
    public double BackoffMultiplier { get; set; } = 1.5;
    public int MaxRetryDelaySeconds { get; set; } = 120;
}

public class NatsOptions
{
    public string[] Servers { get; set; } = ["nats://localhost:4222"];
    public string SubjectPrefix { get; set; } = "universal-connector";
    public bool UseJetStream { get; set; } = false;
    public string? CredsFile { get; set; }
}
