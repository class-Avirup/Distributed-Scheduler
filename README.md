# Distributed Job Scheduler

A resilient, distributed task scheduler built in C#/.NET. It features lease-based leader election, Kafka-backed job dispatch, idempotent at-least-once execution guarantees, and event-driven autoscaling.

## 🎯 Why This Exists

Most "job scheduler" side projects are simple, single-process cron wrappers. This system is designed to survive the actual failure modes encountered in production distributed systems:

* **Node Crashes Mid-Job:** If a worker dies, another node picks up the retry. The idempotency layer guarantees the job won't run twice, even if the crashed node partially completed the work.
* **Leader Failures:** If the leader dies, a new leader is elected within one lease TTL (default 10s). Scheduled dispatch never silently stalls.
* **Exactly-Once Execution:** Kafka provides at-least-once redelivery. The unique index on `IdempotencyKey` in MongoDB deduplicates these redeliveries, achieving "exactly-once" execution.
* **Poison Pill Handling:** Jobs that consistently fail utilize exponential backoff with jitter before being automatically routed to a Dead-Letter Queue (DLQ) after `MaxAttempts`. 
* **Event-Driven Autoscaling:** Worker nodes scale dynamically based on Kafka consumer lag, scaling down to zero when idle and bursting to handle sudden spikes in background jobs.
* **First-Class Observability:** Native Prometheus metrics and Grafana integration for real-time monitoring of job throughput, failure rates, and leader elections.

## 🏗️ Architecture

```text
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │ Worker 1 │   │ Worker 2 │   │ Worker 3 │   <- dynamically scaled
        │ (leader) │   │          │   │          │      via KEDA
        └────┬─────┘   └────┬─────┘   └────┬─────┘
             │              │              │
             │   heartbeat  │   heartbeat  │
             └──────┬───────┴───────┬──────┘
                     ▼               ▼
              ┌─────────────────────────┐
              │   MongoDB: leader_lease │  <- lease-based election (CAS via
              │   MongoDB: job_executions│    findOneAndUpdate), no Raft needed
              └─────────────────────────┘
                     ▲               ▲
             (leader dispatches)  (all workers consume + claim)
                     │               │
              ┌──────┴───────────────┴──────┐
              │   Kafka: jobs.pending       │  <-- KEDA polls lag here
              │   Kafka: jobs.retry         │
              │   Kafka: jobs.dlq           │
              └─────────────────────────────┘
                              ▲
                       ┌──────┴──────┐
                       │  Api (REST) │  <- submit jobs, check status, replay DLQ
                       └─────────────┘
```

### Key Design Decisions & Trade-offs

| Decision | Why We Chose It | The Trade-off |
| :--- | :--- | :--- |
| **Lease-based election via MongoDB CAS** | Requires significantly less code than implementing Raft, while still handling real failure reasoning (split-brain, fencing tokens). | MongoDB becomes a coordination single point of failure (acceptable here, as Mongo typically runs as a replica set in production). |
| **Idempotency via unique index** | Simple, atomic database operation. No separate distributed lock service (like Redis) is required. | Requires API callers to supply a meaningful, stable `IdempotencyKey`. |
| **Kafka key = Idempotency key** | The same logical job always lands on the same partition, preserving execution order for that specific job. | Retries of the same job cannot be parallelized across multiple partitions. |
| **Manual offset commit post-execution** | No message is "acknowledged" until fully processed, enforcing at-least-once semantics. | A crash between execution and commit causes a harmless redelivery (caught by the idempotency layer). |

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose
* (Optional) Kubernetes cluster with KEDA installed for autoscaling testing.

### Running Locally
Spin up the entire infrastructure (MongoDB, Kafka, Zookeeper, Worker replicas, Prometheus, Grafana, and the API) with a single command:

```bash
docker compose up --build -d
```
* The API will be available at `http://localhost:5080`.
* Prometheus metrics are exposed at `http://localhost:9090`.
* Grafana Dashboards are available at `http://localhost:3000`.

---

## 💻 API Usage

**1. Submit a new job**
```bash
curl -X POST http://localhost:5080/jobs \
  -H "Content-Type: application/json" \
  -d '{"idempotencyKey":"send-invoice-4821","jobType":"send-email","payloadJson":"{\"to\":\"a@b.com\"}"}'
```

**2. Check job status**
```bash
curl http://localhost:5080/jobs/send-invoice-4821
```

**3. View the Dead-Letter Queue (DLQ)**
*(Note: The sample job handler is hardcoded to fail ~30% of the time to demonstrate this feature).*
```bash
curl http://localhost:5080/dlq
```

**4. Replay a dead-lettered job**
```bash
curl -X POST http://localhost:5080/dlq/send-invoice-4821/replay
```

## 📈 Autoscaling with KEDA

The worker deployment is completely decoupled from the API and scales based on consumer group lag in Kafka using KEDA. This ensures you only pay for compute when there are jobs to process.

The `ScaledObject` monitors the `jobs.pending` topic. If a sudden burst of jobs arrives, KEDA calculates the required number of pods based on a target `lagThreshold` and instructs the Kubernetes HPA to scale up. Once the queue is drained, the deployment scales back down to zero.

**Implementation (`worker-scaledobject.yaml`):**
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker-kafka-scaler
  namespace: default
spec:
  scaleTargetRef:
    name: distributed-scheduler-worker 
  minReplicaCount: 0 
  maxReplicaCount: 10
  pollingInterval: 15 
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka:9092 
      consumerGroup: job-executors
      topic: jobs.pending
      lagThreshold: "5" 
      offsetResetPolicy: earliest
```

## 💥 The "Chaos" Test: Proving Failover

This system is built for resilience. You can prove the failover mechanics work by intentionally breaking the cluster.

1.  **Find the Leader:** Watch the logs to see which worker currently holds leadership.
    ```bash
    docker compose logs -f worker-1 worker-2 worker-3 | grep -i leadership
    ```
2.  **Execute the Hit:** Submit a batch of jobs via the API, then forcefully kill the current leader mid-flight (replace `worker-1` with the actual leader).
    ```bash
    docker compose kill worker-1
    ```
3.  **Verify the Recovery:** * Watch the logs. A new leader will be elected within ~10 seconds (the lease TTL).
    * Scheduled dispatch will resume.
    * Check `/jobs/{key}` for your submitted jobs. You will verify that `Status == Succeeded` exactly once. No jobs are lost, and no jobs are executed twice.

## 🗺️ Roadmap

* **Cron-based Scheduling:** Swap the demo 15s dispatch loop for true cron expressions (e.g., using the `Cronos` NuGet package) to allow syntax like `"0 */5 * * * *"`.
* **Proper Delay-Queue Polling:** Replace the busy-wait retry-delay check in the executor with a background poller reading a Mongo collection sorted by `NotBeforeUtc`.
* **Automated Integration Testing:** Implement Testcontainers (Mongo + Kafka) to verify failover behavior automatically in CI pipelines.