using DistributedScheduler.Core.Models;
using MongoDB.Driver;

namespace DistributedScheduler.Core.Idempotency;

/// <summary>
/// Enforces "exactly-once execution" on top of Kafka's "at-least-once delivery"
/// via a unique index on IdempotencyKey. This is the same pattern used for the
/// Zendesk alert deduplication engine, generalized to any job type.
/// </summary>
public class IdempotencyStore
{
    private readonly IMongoCollection<JobExecutionRecord> _executions;

    public IdempotencyStore(IMongoDatabase database)
    {
        _executions = database.GetCollection<JobExecutionRecord>("job_executions");

        var indexKeys = Builders<JobExecutionRecord>.IndexKeys.Ascending(r => r.IdempotencyKey);
        var indexOptions = new CreateIndexOptions { Unique = true };
        _executions.Indexes.CreateOne(new CreateIndexModel<JobExecutionRecord>(indexKeys, indexOptions));
    }

    /// <summary>
    /// Attempts to claim a job for execution. Returns false if another worker
    /// (or a previous delivery of the same message) already claimed it.
    /// </summary>
    public async Task<bool> TryClaimAsync(JobDefinition job, string workerNodeId, CancellationToken ct)
    {
        var record = new JobExecutionRecord
        {
            IdempotencyKey = job.IdempotencyKey,
            JobId = job.JobId,
            JobType = job.JobType,
            Status = JobStatus.Running,
            AttemptCount = job.AttemptCount + 1,
            WorkerNodeId = workerNodeId,
            UpdatedAtUtc = DateTime.UtcNow
        };

        try
        {
            await _executions.InsertOneAsync(record, cancellationToken: ct);
            return true;
        }
        catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            // Someone already claimed this idempotency key — could be a retry
            // that succeeded already, or a concurrent duplicate delivery.
            return false;
        }
    }

    public Task MarkSucceededAsync(string idempotencyKey, CancellationToken ct) =>
        UpdateStatusAsync(idempotencyKey, JobStatus.Succeeded, null, ct);

    public Task MarkFailedAsync(string idempotencyKey, string error, CancellationToken ct) =>
        UpdateStatusAsync(idempotencyKey, JobStatus.Failed, error, ct);

    public Task MarkDeadLetteredAsync(string idempotencyKey, string error, CancellationToken ct) =>
        UpdateStatusAsync(idempotencyKey, JobStatus.DeadLettered, error, ct);

    private async Task UpdateStatusAsync(string idempotencyKey, JobStatus status, string? error, CancellationToken ct)
    {
        var filter = Builders<JobExecutionRecord>.Filter.Eq(r => r.IdempotencyKey, idempotencyKey);
        var update = Builders<JobExecutionRecord>.Update
            .Set(r => r.Status, status)
            .Set(r => r.LastError, error)
            .Set(r => r.UpdatedAtUtc, DateTime.UtcNow);

        await _executions.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    /// <summary>
    /// Allows a retry attempt to proceed by resetting a Failed record back to
    /// claimable state. Only call this from the retry path, never for fresh jobs.
    /// </summary>
    public async Task<bool> TryReclaimForRetryAsync(JobDefinition job, string workerNodeId, CancellationToken ct)
    {
        var filter = Builders<JobExecutionRecord>.Filter.And(
            Builders<JobExecutionRecord>.Filter.Eq(r => r.IdempotencyKey, job.IdempotencyKey),
            Builders<JobExecutionRecord>.Filter.Eq(r => r.Status, JobStatus.Failed)
        );

        var update = Builders<JobExecutionRecord>.Update
            .Set(r => r.Status, JobStatus.Running)
            .Set(r => r.AttemptCount, job.AttemptCount + 1)
            .Set(r => r.WorkerNodeId, workerNodeId)
            .Set(r => r.UpdatedAtUtc, DateTime.UtcNow);

        var result = await _executions.UpdateOneAsync(filter, update, cancellationToken: ct);
        return result.ModifiedCount == 1;
    }
}
