using DistributedScheduler.Core.LeaderElection;
using Microsoft.Extensions.Hosting;

namespace DistributedScheduler.Worker;

public class LeaderElectionBackgroundService : BackgroundService
{
    private readonly LeaderHeartbeatRunner _runner;

    public LeaderElectionBackgroundService(LeaderHeartbeatRunner runner)
    {
        _runner = runner;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        _runner.RunAsync(stoppingToken);
}
