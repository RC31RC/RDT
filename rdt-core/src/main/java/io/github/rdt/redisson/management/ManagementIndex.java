package io.github.rdt.redisson.management;

import io.github.rdt.support.KeySpace;
import java.time.Duration;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Shared indexes keep interactive management requests independent of Redis keyspace size. */
public final class ManagementIndex {
    private final RedissonClient client;
    private final KeySpace keys;

    public ManagementIndex(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    public RScoredSortedSet<String> resources(String kind) {
        return client.getScoredSortedSet(keys.key("management-index", kind), StringCodec.INSTANCE);
    }

    public RScoredSortedSet<String> expiries(String kind) {
        return client.getScoredSortedSet(keys.key("management-expiry",kind),StringCodec.INSTANCE);
    }

    public void register(String kind, String name) { resources(kind).add(0, name); }

    public void record(String kind, String id, long time, Duration ttl) {
        resources(kind).add(time, id);
        expiries(kind).add(ttl.isZero() ? Long.MAX_VALUE : System.currentTimeMillis() + ttl.toMillis(), id);
        prune(kind);
    }

    public void remove(String kind, String id) {
        resources(kind).remove(id);
        expiries(kind).remove(id);
    }

    public void prune(String kind) {
        var expiry = expiries(kind);
        var expired = expiry.valueRange(0, true, System.currentTimeMillis(), true, 0, 128);
        if (!expired.isEmpty()) {
            resources(kind).removeAll(expired);
            expiry.removeAll(expired);
        }
    }
}
