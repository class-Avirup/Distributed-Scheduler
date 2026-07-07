namespace DistributedScheduler.Core.JobHandlers;

/// <summary>
/// Implement one of these per JobType (e.g. "send-email", "sync-tenant-data").
/// Handlers should be idempotent-friendly themselves where possible, but the
/// scheduler's idempotency layer is what actually guarantees exactly-once.
/// </summary>
public interface IJobHandler
{
    string JobType { get; }
    Task ExecuteAsync(string payloadJson, CancellationToken ct);
}

public class JobHandlerRegistry
{
    private readonly Dictionary<string, IJobHandler> _handlers = new();

    public void Register(IJobHandler handler) => _handlers[handler.JobType] = handler;

    public IJobHandler Resolve(string jobType)
    {
        if (!_handlers.TryGetValue(jobType, out var handler))
        {
            throw new InvalidOperationException($"No handler registered for job type '{jobType}'");
        }
        return handler;
    }
}
