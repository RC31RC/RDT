package io.github.rdt.redisson.queue;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService.Lease;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;

import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.UUID;

/** One recovery scanner per stream/group; normal XREADGROUP remains competitive on every node. */
final class PendingRecoveryCoordinator implements AutoCloseable {
    private final RedisOwnerLeaseService leases;
    private final String name;
    private final String owner;
    private final Duration ttl;
    private Lease lease;
    private long nextCheck;

    PendingRecoveryCoordinator(
            RedissonClient client,
            KeySpace keys,
            DistributedLockTemplate locks,
            QueueSubscription spec,
            RuntimeIdentity identity,
            QueuePolicy policy) {
        leases = new RedisOwnerLeaseService(client, keys, locks);
        name =
                "pending-recovery:"
                        + RdtSupport.component(spec.destination())
                        + ":"
                        + RdtSupport.component(spec.group());
        owner = identity.instanceId() + ":" + identity.runId() + ":" + UUID.randomUUID();
        ttl = policy.resolve(spec.options()).pollTimeout().multipliedBy(3).plusSeconds(3);
    }

    boolean isLeader() {
        long now = System.nanoTime();
        if (now < nextCheck) return lease != null;
        nextCheck = now + Duration.ofSeconds(1).toNanos();
        try {
            if (lease != null && !leases.renew(lease, ttl)) lease = null;
            if (lease == null) lease = leases.acquire(name, owner, ttl).orElse(null);
            return lease != null;
        } catch (RuntimeException error) {
            lease = null;
            throw error;
        }
    }

    @Override
    public void close() {
        if (lease != null) {
            Lease previous = lease;
            lease = null;
            leases.release(previous);
        }
    }
}
