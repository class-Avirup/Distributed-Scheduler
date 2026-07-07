namespace DistributedScheduler.Core.JobHandlers;

/// <summary>
/// Placeholder handler so the system is runnable end-to-end out of the box.
/// Replace with real handlers (email, tenant sync, report generation, etc).
/// Throws intermittently so you can exercise the retry/DLQ path in demos.
/// </summary>
public class SampleEmailJobHandler : IJobHandler
{
    private static readonly Random Rng = new();

    public string JobType => "send-email";

    public Task ExecuteAsync(string payloadJson, CancellationToken ct)
    {
        // Simulate a flaky downstream dependency ~30% of the time.
        if (Rng.NextDouble() < 0.3)
        {
            throw new InvalidOperationException("Simulated transient email provider failure");
        }

        Console.WriteLine($"[send-email] delivered payload: {payloadJson}");
        return Task.CompletedTask;
    }
}
