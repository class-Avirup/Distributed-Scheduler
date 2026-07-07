namespace DistributedScheduler.Api.Contracts;

/// <summary>
/// Request body for POST /jobs.
/// </summary>
public record JobSubmissionRequest(string IdempotencyKey, string JobType, string PayloadJson, int? MaxAttempts);
