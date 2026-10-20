package io.github.rdt.redisson.queue;

import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.KeySpace;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** One delayed-message scanner per namespace; duplicate transfer after lease loss remains safe. */
final class DelayTransferCoordinator implements AutoCloseable {
    private static final long CHECK_NANOS = Duration.ofSeconds(1).toNanos();
    private static final long LEASE_MILLIS = 5000;
    private final RedissonClient client;
    private final String key;
    private final String token;
    private long nextCheck;
    private boolean leader;
    private boolean closed;

    DelayTransferCoordinator(RedissonClient client, KeySpace keys, RuntimeIdentity identity) {
        this.client = client;
        key = keys.key("queue-maintenance", "delay-transfer");
        token = identity.instanceId() + ":" + identity.runId() + ":" + UUID.randomUUID();
    }

    synchronized boolean isLeader() {
        if (closed) return false;
        long now = System.nanoTime();
        if (now < nextCheck) return leader;
        // Failure also backs off. Cached permission never survives a long local pause.
        nextCheck = now + CHECK_NANOS;
        leader = false;
        Number result = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
                end
                if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return 1 end
                return 0
                """, RScript.ReturnType.LONG, List.of(key), token, LEASE_MILLIS);
        leader = result.longValue() == 1;
        return leader;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        if (!leader) return;
        leader = false;
        client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
                return 0
                """, RScript.ReturnType.LONG, List.of(key), token);
    }
}
