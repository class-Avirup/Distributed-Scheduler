using System.Text.Json;
using Confluent.Kafka;
using DistributedScheduler.Core.Idempotency;
using DistributedScheduler.Core.JobHandlers;
using DistributedScheduler.Core.Messaging;
using DistributedScheduler.Core.Models;
using DistributedScheduler.Core.Observability;
using DistributedScheduler.Core.Retry;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace DistributedScheduler.Worker;

/// <summary>
/// Runs on every node (they're all in the same Kafka consumer group, so Kafka
/// itself load-balances partitions across workers -- this is your horizontal
/// scaling story). For each message: claim idempotency key -> execute handler
/// -> succeed, or on failure -> requeue with backoff, or -> DLQ after MaxAttempts.
/// </summary>
public class JobExecutorBackgroundService : BackgroundService
{
    private readonly string _bootstrapServers;
    private readonly IdempotencyStore _idempotencyStore;
    private readonly JobHandlerRegistry _handlers;
    private readonly JobProducer _producer;
    private readonly ILogger<JobExecutorBackgroundService> _logger;
    private readonly string _nodeId;

    public JobExecutorBackgroundService(
        string bootstrapServers,
        IdempotencyStore idempotencyStore,
        JobHandlerRegistry handlers,
        JobProducer producer,
        ILogger<JobExecutorBackgroundService> logger,
        string nodeId)
    {
        _bootstrapServers = bootstrapServers;
        _idempotencyStore = idempotencyStore;
        _handlers = handlers;
        _producer = producer;
        _logger = logger;
        _nodeId = nodeId;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "job-executors",          // shared group => Kafka load-balances partitions across nodes
            EnableAutoCommit = false,             // we commit manually, after successful handling
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(new[] { Topics.Pending, Topics.Retry });

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result = null;
            try
            {
                result = consumer.Consume(stoppingToken);
                if (result?.Message is null) continue;

                var job = JsonSerializer.Deserialize<JobDefinition>(result.Message.Value)!;

                if (job.NotBeforeUtc > DateTime.UtcNow)
                {
                    // Not ready yet (retry delay hasn't elapsed). In production,
                    // use a delay-queue mechanism (e.g. a separate poller reading
                    // a Mongo collection sorted by NotBeforeUtc) instead of busy
                    // re-consuming; kept simple here for clarity.
                    await Task.Delay(500, stoppingToken);
                    consumer.Seek(result.TopicPartitionOffset);
                    continue;
                }

                await HandleJobAsync(job, stoppingToken);
                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error on node {NodeId}", _nodeId);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        consumer.Close();
    }

    private async Task HandleJobAsync(JobDefinition job, CancellationToken ct)
    {
        var isFirstAttempt = job.AttemptCount == 0;

        var claimed = isFirstAttempt
            ? await _idempotencyStore.TryClaimAsync(job, _nodeId, ct)
            : await _idempotencyStore.TryReclaimForRetryAsync(job, _nodeId, ct);

        if (!claimed)
        {
            AppMetrics.JobsProcessed.WithLabels("duplicate").Inc();
            _logger.LogInformation(
                "Job {JobId} (key={IdempotencyKey}) already claimed/completed elsewhere -- skipping duplicate delivery",
                job.JobId, job.IdempotencyKey);
            return;
        }

        using var timer = AppMetrics.JobExecutionDuration.WithLabels(job.JobType).NewTimer();

        try
        {
            var handler = _handlers.Resolve(job.JobType);
            await handler.ExecuteAsync(job.PayloadJson, ct);

            await _idempotencyStore.MarkSucceededAsync(job.IdempotencyKey, ct);
            AppMetrics.JobsProcessed.WithLabels("success").Inc();
            _logger.LogInformation("Job {JobId} succeeded on node {NodeId}", job.JobId, _nodeId);
        }
        catch (Exception ex)
        {
            await _idempotencyStore.MarkFailedAsync(job.IdempotencyKey, ex.Message, ct);
            AppMetrics.JobsProcessed.WithLabels("failure").Inc();
            await RouteFailureAsync(job, ex, ct);
        }
    }

    private async Task RouteFailureAsync(JobDefinition job, Exception ex, CancellationToken ct)
    {
        job.AttemptCount += 1;

        if (job.AttemptCount >= job.MaxAttempts)
        {
            await _idempotencyStore.MarkDeadLetteredAsync(job.IdempotencyKey, ex.Message, ct);
            await _producer.PublishAsync(Topics.DeadLetter, job, ct);
            AppMetrics.JobsDeadLettered.Inc();
            _logger.LogWarning(
                "Job {JobId} exhausted {MaxAttempts} attempts, moved to DLQ: {Error}",
                job.JobId, job.MaxAttempts, ex.Message);
            return;
        }

        job.NotBeforeUtc = DateTime.UtcNow.Add(BackoffPolicy.NextDelay(job.AttemptCount));
        await _producer.PublishAsync(Topics.Retry, job, ct);
        AppMetrics.JobRetries.Inc();
        _logger.LogWarning(
            "Job {JobId} failed (attempt {Attempt}/{Max}), requeued for {NotBefore:o}: {Error}",
            job.JobId, job.AttemptCount, job.MaxAttempts, job.NotBeforeUtc, ex.Message);
    }
}
