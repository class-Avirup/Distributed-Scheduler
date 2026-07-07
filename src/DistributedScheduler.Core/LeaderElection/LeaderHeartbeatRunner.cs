using Microsoft.Extensions.Logging;

namespace DistributedScheduler.Core.LeaderElection;

/// <summary>
/// Drives LeaderLeaseService on a fixed interval. Run this as a long-lived
/// loop inside a BackgroundService.
/// </summary>
public class LeaderHeartbeatRunner
{
    private readonly LeaderLeaseService _lease;
    private readonly ILogger<LeaderHeartbeatRunner> _logger;
    private readonly TimeSpan _interval;

    public LeaderHeartbeatRunner(
        LeaderLeaseService lease,
        ILogger<LeaderHeartbeatRunner> logger,
        TimeSpan? interval = null)
    {
        _lease = lease;
        _logger = logger;
        _interval = interval ?? TimeSpan.FromSeconds(3);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await _lease.TryAcquireOrRenewAsync(ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Leader heartbeat failed for node {NodeId}", _lease.NodeId);
            }

            await Task.Delay(_interval, ct).ContinueWith(_ => { }); // swallow cancellation on shutdown
        }

        await _lease.ReleaseAsync(CancellationToken.None);
    }
}
