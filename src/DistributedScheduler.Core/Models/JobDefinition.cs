namespace DistributedScheduler.Core.Models;

/// <summary>
/// A job submitted to the scheduler. IdempotencyKey must be unique per logical
/// unit of work so that at-least-once Kafka delivery never causes double execution.
/// </summary>
public class JobDefinition
{
    public string JobId { get; set; } = Guid.NewGuid().ToString();

    /// <summary>
    /// Caller-supplied key used to detect duplicate submissions/deliveries.
    /// E.g. "send-invoice-email:invoice-4821"
    /// </summary>
    public string IdempotencyKey { get; set; } = default!;

    /// <summary>
    /// Maps to a registered IJobHandler in the worker's handler registry.
    /// </summary>
    public string JobType { get; set; } = default!;

    /// <summary>
    /// Arbitrary JSON payload the handler knows how to deserialize.
    /// </summary>
    public string PayloadJson { get; set; } = "{}";

    public int AttemptCount { get; set; } = 0;

    public int MaxAttempts { get; set; } = 5;

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// For retries: job should not be picked up before this time.
    /// </summary>
    public DateTime NotBeforeUtc { get; set; } = DateTime.UtcNow;
}
