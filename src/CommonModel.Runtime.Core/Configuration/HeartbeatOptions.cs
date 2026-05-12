namespace CommonModel.Runtime.Core.Configuration;

public class HeartbeatOptions
{
    public int IntervalSeconds { get; set; } = 30;
    public string SubjectPrefix { get; set; } = "cdc.health";
    // When true, heartbeat messages are published to JetStream (durable).
    // Falls back to core NATS if no stream captures the subject.
    public bool UseJetStream { get; set; } = true;
}
