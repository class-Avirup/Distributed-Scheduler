using DistributedScheduler.Core.Messaging;
using DistributedScheduler.Core.Models;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;

namespace DistributedScheduler.Api.Controllers;

[ApiController]
[Route("dlq")]
public class DlqController : ControllerBase
{
    private readonly IMongoDatabase _db;
    private readonly JobProducer _producer;

    public DlqController(IMongoDatabase db, JobProducer producer)
    {
        _db = db;
        _producer = producer;
    }

    // GET /dlq -- list dead-lettered jobs awaiting manual triage.
    [HttpGet]
    public async Task<IActionResult> ListDeadLettered(CancellationToken ct)
    {
        var collection = _db.GetCollection<JobExecutionRecord>("job_executions");
        var records = await collection.Find(r => r.Status == JobStatus.DeadLettered).ToListAsync(ct);
        return Ok(records);
    }

    // POST /dlq/{idempotencyKey}/replay -- manually resubmit a dead-lettered job.
    [HttpPost("{idempotencyKey}/replay")]
    public async Task<IActionResult> Replay(string idempotencyKey, CancellationToken ct)
    {
        var collection = _db.GetCollection<JobExecutionRecord>("job_executions");
        var record = await collection.Find(r => r.IdempotencyKey == idempotencyKey).FirstOrDefaultAsync(ct);
        if (record is null) return NotFound();

        var job = new JobDefinition
        {
            JobId = Guid.NewGuid().ToString(),
            IdempotencyKey = $"{idempotencyKey}-replay-{DateTime.UtcNow:yyyyMMddHHmmss}",
            JobType = record.JobType,
            AttemptCount = 0
        };

        await _producer.PublishAsync(Topics.Pending, job, ct);
        return Accepted($"/jobs/{job.IdempotencyKey}", new { job.JobId, job.IdempotencyKey });
    }
}
