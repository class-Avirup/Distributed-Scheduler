using DistributedScheduler.Core.Messaging;
using MongoDB.Driver;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

var mongoConnectionString = builder.Configuration["Mongo:ConnectionString"] ?? "mongodb://localhost:27017";
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

builder.Services.AddSingleton<IMongoClient>(_ => new MongoClient(mongoConnectionString));
builder.Services.AddSingleton(sp => sp.GetRequiredService<IMongoClient>().GetDatabase("scheduler"));
builder.Services.AddSingleton(sp => new JobProducer(
    kafkaBootstrapServers,
    sp.GetRequiredService<ILogger<JobProducer>>()));

builder.Services.AddControllers();

var app = builder.Build();

// UseHttpMetrics auto-records request count/duration for every endpoint below;
// MapMetrics exposes those (plus AppMetrics' custom counters/gauges, since
// they share the same underlying Prometheus.Metrics registry) at GET /metrics.
app.UseHttpMetrics();
app.MapMetrics();

app.MapControllers();

app.Run();
