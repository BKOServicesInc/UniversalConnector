namespace CommonModel.Runtime.Core.Configuration;

public class HeartbeatOptions
{
    public int IntervalSeconds { get; set; } = 30;
    public string SubjectPrefix { get; set; } = "cdc.health";
    // Heartbeats are fire-and-forget signals — core NATS is sufficient.
    // Set true only if you have a JetStream stream covering the subject prefix.
    public bool UseJetStream { get; set; } = false;
}
