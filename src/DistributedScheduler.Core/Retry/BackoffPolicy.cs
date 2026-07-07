namespace DistributedScheduler.Core.Retry;

public static class BackoffPolicy
{
    /// <summary>
    /// Exponential backoff with jitter: base * 2^attempt, capped, plus randomness
    /// to avoid thundering-herd retries all landing on the same instant.
    /// </summary>
    public static TimeSpan NextDelay(int attemptCount, TimeSpan? baseDelay = null, TimeSpan? maxDelay = null)
    {
        var basis = baseDelay ?? TimeSpan.FromSeconds(2);
        var cap = maxDelay ?? TimeSpan.FromMinutes(5);

        var exponential = basis.TotalMilliseconds * Math.Pow(2, attemptCount);
        var jitter = Random.Shared.NextDouble() * basis.TotalMilliseconds;
        var delayMs = Math.Min(exponential + jitter, cap.TotalMilliseconds);

        return TimeSpan.FromMilliseconds(delayMs);
    }
}
