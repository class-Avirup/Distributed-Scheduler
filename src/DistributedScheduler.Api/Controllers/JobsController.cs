using DistributedScheduler.Api.Contracts;
using DistributedScheduler.Core.Messaging;
using DistributedScheduler.Core.Models;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;

namespace DistributedScheduler.Api.Controllers;

[ApiController]
[Route("jobs")]
public class JobsController : ControllerBase
{
    private readonly IMongoDatabase _db;
    private readonly JobProducer _producer;

    public JobsController(IMongoDatabase db, JobProducer producer)
    {
        _db = db;
        _producer = producer;
    }

    // POST /jobs -- submit a new job for the cluster to execute.
    [HttpPost]
    public async Task<IActionResult> SubmitJob(JobSubmissionRequest request, CancellationToken ct)
    {
        var job = new JobDefinition
        {
            IdempotencyKey = request.IdempotencyKey,
            JobType = request.JobType,
            PayloadJson = request.PayloadJson,
            MaxAttempts = request.MaxAttempts ?? 5
        };

        await _producer.PublishAsync(Topics.Pending, job, ct);
        return Accepted($"/jobs/{job.IdempotencyKey}", new { job.JobId, job.IdempotencyKey });
    }

    // GET /jobs/{idempotencyKey} -- check status of a submitted job.
    [HttpGet("{idempotencyKey}")]
    public async Task<IActionResult> GetJobStatus(string idempotencyKey, CancellationToken ct)
    {
        var collection = _db.GetCollection<JobExecutionRecord>("job_executions");
        var record = await collection.Find(r => r.IdempotencyKey == idempotencyKey).FirstOrDefaultAsync(ct);
        return record is null ? NotFound() : Ok(record);
    }
}
