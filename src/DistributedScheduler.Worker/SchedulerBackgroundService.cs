using DistributedScheduler.Core.LeaderElection;
using DistributedScheduler.Core.Messaging;
using DistributedScheduler.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedScheduler.Worker;

/// <summary>
/// Runs on every node but only *acts* when this node currently holds the
/// leader lease. This is what prevents a cron-style job from firing N times
/// across an N-node cluster. Demo implementation dispatches one sample job
/// every 15s so you have something to watch in the logs; swap in a real
/// cron-expression evaluator (e.g. Cronos) for production use.
/// </summary>
public class SchedulerBackgroundService : BackgroundService
{
    private readonly LeaderLeaseService _lease;
    private readonly JobProducer _producer;
    private readonly ILogger<SchedulerBackgroundService> _logger;

    public SchedulerBackgroundService(
        LeaderLeaseService lease,
        JobProducer producer,
        ILogger<SchedulerBackgroundService> logger)
    {
        _lease = lease;
        _producer = producer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (_lease.IsLeader)
            {
                try
                {
                    var job = new JobDefinition
                    {
                        IdempotencyKey = $"demo-email-{DateTime.UtcNow:yyyyMMddHHmmss}",
                        JobType = "send-email",
                        PayloadJson = "{\"to\":\"customer@example.com\",\"template\":\"weekly-digest\"}"
                    };

                    await _producer.PublishAsync(Topics.Pending, job, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Scheduler dispatch failed on leader {NodeId}", _lease.NodeId);
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken).ContinueWith(_ => { });
        }
    }
}
