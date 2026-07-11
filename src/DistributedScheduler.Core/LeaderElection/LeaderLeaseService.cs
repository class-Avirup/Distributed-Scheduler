using DistributedScheduler.Core.Observability;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DistributedScheduler.Core.LeaderElection;

/// <summary>
/// Lease-based leader election backed by a single MongoDB document with a TTL.
/// A node holds leadership only while it keeps renewing the lease before it
/// expires; if it crashes, the lease naturally expires and another node wins it.
///
/// This intentionally avoids implementing Raft: MongoDB's atomic findOneAndUpdate
/// gives us the compare-and-swap primitive we need, at the cost of MongoDB itself
/// being a single point of coordination (acceptable for this project's scope —
/// call this out explicitly in interviews as a known tradeoff).
/// </summary>
public class LeaderLeaseService
{
    private const string LeaseDocumentId = "scheduler-leader-lease";

    private readonly IMongoCollection<BsonDocument> _leaseCollection;
    private readonly string _nodeId;
    private readonly TimeSpan _leaseDuration;
    private readonly ILogger<LeaderLeaseService> _logger;

    private volatile bool _isLeader;
    private DateTime _fencingToken; // monotonically increasing "who was leader when" marker

    public bool IsLeader => _isLeader;
    public string NodeId => _nodeId;

    public LeaderLeaseService(
        IMongoDatabase database,
        string nodeId,
        ILogger<LeaderLeaseService> logger,
        TimeSpan? leaseDuration = null)
    {
        _leaseCollection = database.GetCollection<BsonDocument>("leader_lease");
        _nodeId = nodeId;
        _logger = logger;
        _leaseDuration = leaseDuration ?? TimeSpan.FromSeconds(10);
    }

    /// <summary>
    /// Attempts to acquire or renew the lease. Call this on a timer (e.g. every
    /// leaseDuration / 3) from a background loop. Safe to call from every node.
    /// </summary>
    public async Task<bool> TryAcquireOrRenewAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var newExpiry = now.Add(_leaseDuration);

        // Win the lease if: no one holds it, the current holder's lease expired,
        // or we already hold it (renewal case).
        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("_id", LeaseDocumentId),
            Builders<BsonDocument>.Filter.Or(
                Builders<BsonDocument>.Filter.Lte("expiresAtUtc", now),
                Builders<BsonDocument>.Filter.Eq("holderNodeId", _nodeId)
            )
        );

        var update = Builders<BsonDocument>.Update
            .Set("holderNodeId", _nodeId)
            .Set("expiresAtUtc", newExpiry)
            .Inc("fencingToken", _isLeader ? 0 : 1); // bump token only on takeover

        var options = new FindOneAndUpdateOptions<BsonDocument>
        {
            IsUpsert = true,
            ReturnDocument = ReturnDocument.After
        };

        try
        {
            var result = await _leaseCollection.FindOneAndUpdateAsync(filter, update, options, ct);
            var wonLease = result is not null && result["holderNodeId"].AsString == _nodeId;

            if (wonLease && !_isLeader)
            {
                _logger.LogWarning("Node {NodeId} acquired leadership (fencing token {Token})",
                    _nodeId, result!["fencingToken"].AsInt32);
                AppMetrics.LeaderChanges.Inc();
            }
            else if (!wonLease && _isLeader)
            {
                _logger.LogWarning("Node {NodeId} lost leadership", _nodeId);
            }

            AppMetrics.IsLeader.Set(wonLease ? 1 : 0);
            _isLeader = wonLease;
            return wonLease;
        }
        catch (MongoCommandException ex)
        {
            // Upsert race: another node upserted first. We simply didn't win this round.
            _logger.LogDebug(ex, "Lease acquisition contention for node {NodeId}", _nodeId);
            AppMetrics.IsLeader.Set(0);
            _isLeader = false;
            return false;
        }
    }

    /// <summary>
    /// Voluntarily release the lease on graceful shutdown so failover doesn't
    /// have to wait out the full lease TTL.
    /// </summary>
    public async Task ReleaseAsync(CancellationToken ct)
    {
        if (!_isLeader) return;

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("_id", LeaseDocumentId),
            Builders<BsonDocument>.Filter.Eq("holderNodeId", _nodeId)
        );
        var update = Builders<BsonDocument>.Update.Set("expiresAtUtc", DateTime.UtcNow);

        await _leaseCollection.UpdateOneAsync(filter, update, cancellationToken: ct);
        _isLeader = false;
        _logger.LogInformation("Node {NodeId} released leadership voluntarily", _nodeId);
    }
}
