using Prometheus;

namespace DistributedScheduler.Core.Observability;

/// <summary>
/// Every Prometheus metric the system exposes lives here, so any project
/// (Core, Worker, Api) can record against the same instruments without
/// duplicating metric names/labels in multiple places.
/// </summary>
public static class AppMetrics
{
    public static readonly Counter JobsPublished = Prometheus.Metrics.CreateCounter(
        "scheduler_jobs_published_total",
        "Number of jobs published to a Kafka topic",
        new CounterConfiguration { LabelNames = new[] { "topic" } });

    public static readonly Counter JobsProcessed = Prometheus.Metrics.CreateCounter(
        "scheduler_jobs_processed_total",
        "Number of jobs a worker finished processing, by outcome",
        new CounterConfiguration { LabelNames = new[] { "outcome" } }); // outcome = success|failure|duplicate

    public static readonly Counter JobRetries = Prometheus.Metrics.CreateCounter(
        "scheduler_job_retries_total",
        "Number of times a job was requeued for retry after failing");

    public static readonly Counter JobsDeadLettered = Prometheus.Metrics.CreateCounter(
        "scheduler_jobs_dead_lettered_total",
        "Number of jobs that exhausted retries and were moved to the DLQ");

    public static readonly Counter LeaderChanges = Prometheus.Metrics.CreateCounter(
        "scheduler_leader_changes_total",
        "Number of times a node acquired leadership (i.e. leadership changed hands)");

    public static readonly Gauge DlqSize = Prometheus.Metrics.CreateGauge(
        "scheduler_dlq_size",
        "Current number of jobs sitting in the dead-letter queue");

    public static readonly Gauge IsLeader = Prometheus.Metrics.CreateGauge(
        "scheduler_is_leader",
        "1 if this node currently holds the leader lease, 0 otherwise");

    public static readonly Histogram JobExecutionDuration = Prometheus.Metrics.CreateHistogram(
        "scheduler_job_execution_duration_seconds",
        "Time taken to execute a single job handler",
        new HistogramConfiguration { LabelNames = new[] { "job_type" } });
}
