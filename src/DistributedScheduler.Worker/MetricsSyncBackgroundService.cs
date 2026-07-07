using DistributedScheduler.Core.Models;
using DistributedScheduler.Core.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace DistributedScheduler.Worker;

/// <summary>
/// Gauges represent point-in-time state, not events -- so unlike the counters
/// (which increment inline where the event happens), DLQ size is kept accurate
/// by periodically re-querying Mongo for the true count, rather than trying to
/// increment/decrement it and risking drift if a step is ever missed.
/// </summary>
public class MetricsSyncBackgroundService : BackgroundService
{
    private readonly IMongoDatabase _database;
    private readonly ILogger<MetricsSyncBackgroundService> _logger;
    private readonly TimeSpan _interval = TimeSpan.FromSeconds(10);

    public MetricsSyncBackgroundService(IMongoDatabase database, ILogger<MetricsSyncBackgroundService> logger)
    {
        _database = database;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var collection = _database.GetCollection<JobExecutionRecord>("job_executions");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var count = await collection.CountDocumentsAsync(
                    r => r.Status == JobStatus.DeadLettered, cancellationToken: stoppingToken);

                AppMetrics.DlqSize.Set(count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to sync DLQ size metric");
            }

            await Task.Delay(_interval, stoppingToken).ContinueWith(_ => { });
        }
    }
}
