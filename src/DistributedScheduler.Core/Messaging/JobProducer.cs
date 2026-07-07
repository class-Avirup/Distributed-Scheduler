using System.Text.Json;
using Confluent.Kafka;
using DistributedScheduler.Core.Models;
using DistributedScheduler.Core.Observability;
using Microsoft.Extensions.Logging;

namespace DistributedScheduler.Core.Messaging;

public class JobProducer : IAsyncDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<JobProducer> _logger;

    public JobProducer(string bootstrapServers, ILogger<JobProducer> logger)
    {
        _logger = logger;
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,          // don't lose jobs on broker failover
            EnableIdempotence = true, // avoid Kafka-level duplicate sends on producer retry
            MessageSendMaxRetries = 5,
        };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync(string topic, JobDefinition job, CancellationToken ct)
    {
        var message = new Message<string, string>
        {
            Key = job.IdempotencyKey, // same key -> same partition -> preserves per-job ordering
            Value = JsonSerializer.Serialize(job)
        };

        var result = await _producer.ProduceAsync(topic, message, ct);
        AppMetrics.JobsPublished.WithLabels(topic).Inc();
        _logger.LogInformation("Published job {JobId} ({JobType}) to {Topic} @ offset {Offset}",
            job.JobId, job.JobType, topic, result.Offset);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
