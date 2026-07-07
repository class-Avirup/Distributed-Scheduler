namespace DistributedScheduler.Core.Models;

public enum JobStatus
{
    Pending,
    Running,
    Succeeded,
    Failed,
    DeadLettered
}

/// <summary>
/// One document per IdempotencyKey. The unique index on IdempotencyKey is what
/// actually enforces idempotency: a duplicate insert fails, telling the worker
/// "someone already claimed this job" — same pattern as the Zendesk dedup engine.
/// </summary>
public class JobExecutionRecord
{
    public string IdempotencyKey { get; set; } = default!;
    public string JobId { get; set; } = default!;
    public string JobType { get; set; } = default!;
    public JobStatus Status { get; set; } = JobStatus.Pending;
    public int AttemptCount { get; set; }
    public string? LastError { get; set; }
    public string? WorkerNodeId { get; set; }
    public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;
}
