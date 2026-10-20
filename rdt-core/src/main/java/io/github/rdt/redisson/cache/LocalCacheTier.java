package io.github.rdt.redisson.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.github.rdt.cache.CachePolicy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Optional bounded-staleness tier. Generation checks fence concurrent read fills. */
final class LocalCacheTier {
    private final Cache<String, Value> entries;
    private final CachePolicy policy;
    private final AtomicLong generation = new AtomicLong();

    LocalCacheTier(CachePolicy policy) {
        this.policy = policy;
        entries =
                Caffeine.newBuilder()
                        .maximumSize(policy.localMaximumSize())
                        .expireAfterWrite(policy.localTtl())
                        .build();
    }

    long generation() {
        return generation.get();
    }

    String get(String key, long now) {
        Value value = entries.getIfPresent(key);
        if (value != null && now < value.deadline()) {
            return value.json();
        }
        entries.invalidate(key);
        return null;
    }

    void fillLocal(
            String key, String json, long remainingMillis, long started, long observedGeneration) {
        if (remainingMillis <= 0) {
            return;
        }
        long ttl =
                Math.min(
                        TimeUnit.MILLISECONDS.toNanos(remainingMillis),
                        policy.localTtl().toNanos());
        entries.put(key, new Value(json, started + ttl));
        if (generation.get() != observedGeneration) {
            entries.invalidate(key);
        }
    }

    void invalidate(String key) {
        generation.incrementAndGet();
        if ("*".equals(key)) {
            entries.invalidateAll();
        } else {
            entries.invalidate(key);
        }
    }

    private record Value(String json, long deadline) {}
}
