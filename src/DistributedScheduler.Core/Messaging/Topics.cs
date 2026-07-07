namespace DistributedScheduler.Core.Messaging;

public static class Topics
{
    public const string Pending = "jobs.pending";
    public const string Retry = "jobs.retry";
    public const string DeadLetter = "jobs.dlq";
}
