using DistributedScheduler.Core.Idempotency;
using DistributedScheduler.Core.JobHandlers;
using DistributedScheduler.Core.LeaderElection;
using DistributedScheduler.Core.Messaging;
using DistributedScheduler.Worker;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using DistributedScheduler.Core.Observability;
using Prometheus;

var builder = Host.CreateApplicationBuilder(args);

var mongoConnectionString = builder.Configuration["Mongo:ConnectionString"] ?? "mongodb://localhost:27017";
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

// Each node needs a stable-ish identity for logging/fencing. Container hostname
// works well in Docker/Kubernetes since each replica gets a unique one.
var nodeId = Environment.GetEnvironmentVariable("NODE_ID") ?? Environment.MachineName;

builder.Services.AddSingleton<IMongoClient>(_ => new MongoClient(mongoConnectionString));
builder.Services.AddSingleton(sp => sp.GetRequiredService<IMongoClient>().GetDatabase("scheduler"));

builder.Services.AddSingleton(sp => new LeaderLeaseService(
    sp.GetRequiredService<IMongoDatabase>(),
    nodeId,
    sp.GetRequiredService<ILogger<LeaderLeaseService>>()));

builder.Services.AddSingleton<LeaderHeartbeatRunner>();
builder.Services.AddSingleton<IdempotencyStore>();

builder.Services.AddSingleton(sp => new JobProducer(
    kafkaBootstrapServers,
    sp.GetRequiredService<ILogger<JobProducer>>()));

builder.Services.AddSingleton(sp =>
{
    var registry = new JobHandlerRegistry();
    registry.Register(new SampleEmailJobHandler());
    // Register additional IJobHandler implementations here as you add job types.
    return registry;
});

builder.Services.AddHostedService<LeaderElectionBackgroundService>();
builder.Services.AddHostedService<SchedulerBackgroundService>();
builder.Services.AddHostedService<JobExecutorBackgroundService>(sp => new JobExecutorBackgroundService(
    kafkaBootstrapServers,
    sp.GetRequiredService<IdempotencyStore>(),
    sp.GetRequiredService<JobHandlerRegistry>(),
    sp.GetRequiredService<JobProducer>(),
    sp.GetRequiredService<ILogger<JobExecutorBackgroundService>>(),
    nodeId));
builder.Services.AddHostedService<MetricsSyncBackgroundService>();

var host = builder.Build();

// prometheus-net's own lightweight Kestrel listener, separate from the Api
// project's ASP.NET Core pipeline -- Worker is a plain console/Generic Host
// process with no web server of its own, so this gives it a /metrics
// endpoint without pulling in the full ASP.NET Core stack.
var metricsPort = builder.Configuration.GetValue<int?>("Metrics:Port") ?? 9090;
var metricServer = new MetricServer(hostname: "+",port: metricsPort);
metricServer.Start();

host.Run();
