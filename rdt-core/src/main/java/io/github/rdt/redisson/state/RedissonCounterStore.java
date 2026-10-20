package io.github.rdt.redisson.state;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.state.CounterStore;
import io.github.rdt.support.KeySpace;
import org.redisson.api.RedissonClient;

/** Atomic Redis counters, isolated from cache eviction and invalidation behavior. */
public final class RedissonCounterStore implements CounterStore {
    private final RedissonClient client;
    private final KeySpace keys;

    public RedissonCounterStore(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    @Override
    public long increment(String key, long delta) {
        return RedisSupport.redis("increment distributed counter", () ->
                client.getAtomicLong(keys.key("cache-counter", key)).addAndGet(delta));
    }
}
