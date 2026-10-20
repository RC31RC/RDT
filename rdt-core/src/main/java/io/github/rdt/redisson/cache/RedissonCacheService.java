package io.github.rdt.redisson.cache;

import io.github.rdt.cache.CacheEntry;
import io.github.rdt.cache.CachePolicy;
import io.github.rdt.cache.CacheService;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;

import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/** Disposable JSON buckets with an optional local tier and region-scoped invalidation. */
public final class RedissonCacheService implements CacheService, AutoCloseable {
    private static final String READ_WITH_TTL =
            """
            local v = redis.call('GET', KEYS[1])
            if not v then return {} end
            return {v, redis.call('PTTL', KEYS[1])}
            """;
    private final RedissonClient client;
    private final KeySpace keys;
    private final KeySpace rootKeys;
    private final JsonCodec json;
    private final CachePolicy policy;
    private final DistributedLockTemplate locks;
    private final LocalCacheTier local;
    private final CacheInvalidationBus invalidations;
    private final int listenerId;
    private final Map<String, RedissonCacheService> regions = new ConcurrentHashMap<>();

    @Override
    public CacheService region(String name) {
        RedissonCacheService selected = regions.get(name);
        RdtSupport.require(
                selected != null,
                "Unknown cache region: "
                        + name
                        + "; configure rdt.cache.regions or create it with an explicit"
                        + " CachePolicy");
        return selected;
    }

    @Override
    public CacheService region(String name, CachePolicy regionPolicy) {
        RdtSupport.component(name);
        return regions.compute(
                name,
                (id, existing) -> {
                    if (existing != null) {
                        RdtSupport.require(
                                existing.policy.equals(regionPolicy),
                                "Conflicting policy for cache region " + name);
                        return existing;
                    }
                    KeySpace regionKeys = new KeySpace(keys.namespace() + ":region:" + name);
                    return new RedissonCacheService(
                            client,
                            regionKeys,
                            json,
                            regionPolicy,
                            new RedissonLockTemplate(client, regionKeys),
                            rootKeys);
                });
    }

    /**
     * 创建缓存服务；当前实例仅管理自身的失效订阅，不持有共享客户端的生命周期。
     *
     * @param client 共享客户端
     * @param keys 命名空间
     * @param json 消息载荷编解码器
     * @param policy 缓存保留策略
     * @param locks 缓存回源互斥锁
     */
    public RedissonCacheService(
            RedissonClient client,
            KeySpace keys,
            JsonCodec json,
            CachePolicy policy,
            DistributedLockTemplate locks) {
        this(client, keys, json, policy, locks, keys);
    }

    private RedissonCacheService(
            RedissonClient client,
            KeySpace keys,
            JsonCodec json,
            CachePolicy policy,
            DistributedLockTemplate locks,
            KeySpace rootKeys) {
        this.rootKeys = rootKeys;
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.policy = policy;
        this.locks = locks;
        local = policy.localEnabled() ? new LocalCacheTier(policy) : null;
        invalidations = new CacheInvalidationBus(client, keys, rootKeys);
        listenerId = local == null ? -1 : invalidations.subscribe(local::invalidate);
    }

    @Override
    public <T> CacheEntry<T> get(String key, Type type) {
        return read(key, type, true);
    }

    private <T> CacheEntry<T> read(String key, Type type, boolean useLocal) {
        String physical = keys.key("cache", key);
        if (useLocal && local != null) {
            String hit = local.get(physical, System.nanoTime());
            if (hit != null) {
                return decode(hit, type);
            }
        }
        long version = local == null ? 0 : local.generation();
        long started = System.nanoTime();
        List<Object> result =
                RedisSupport.redis(
                        "read cache",
                        () ->
                                client.getScript(StringCodec.INSTANCE)
                                        .eval(
                                                RScript.Mode.READ_ONLY,
                                                READ_WITH_TTL,
                                                RScript.ReturnType.LIST,
                                                List.of(physical)));
        if (result.isEmpty()) {
            return new CacheEntry.Miss<>();
        }
        String value = (String) result.getFirst();
        long remaining = ((Number) result.get(1)).longValue();
        fillLocal(physical, value, remaining, started, version);
        return decode(value, type);
    }

    private void fillLocal(
            String physical,
            String value,
            long remainingMillis,
            long startedNanos,
            long observedGeneration) {
        if (local == null) {
            return;
        }
        local.fillLocal(physical, value, remainingMillis, startedNanos, observedGeneration);
    }

    private <T> CacheEntry<T> decode(String value, Type type) {
        return value == null
                ? new CacheEntry.Miss<>()
                : new CacheEntry.Hit<>(json.read(value, type));
    }

    @Override
    public void put(String key, Object value, Duration ttl) {
        String physical = keys.key("cache", key);
        String encoded = json.write(value);
        Duration retention = retention(value, ttl);
        RedisSupport.redisRun(
                "write cache",
                () ->
                        client.<String>getBucket(physical, StringCodec.INSTANCE)
                                .set(encoded, retention));
        invalidate(physical);
    }

    private Duration retention(Object value, Duration ttl) {
        Duration base = value == null ? policy.nullTtl() : ttl == null ? policy.ttl() : ttl;
        RdtSupport.positive(base, "cache ttl");
        long extra =
                (long)
                        (base.toMillis()
                                * policy.jitter()
                                * ThreadLocalRandom.current().nextDouble());
        return base.plusMillis(extra);
    }

    private void invalidate(String physical) {
        if (local != null) {
            local.invalidate(physical);
        }
        invalidations.publish(physical, local != null);
    }

    @Override
    public void evict(String key) {
        String physical = keys.key("cache", key);
        RedisSupport.redisRun(
                "evict cache", () -> client.getBucket(physical, StringCodec.INSTANCE).delete());
        invalidate(physical);
    }

    @Override
    public <T> T getOrLoad(
            String key, Type type, Duration ttl, boolean cacheNull, Callable<T> loader)
            throws Exception {
        CacheEntry<T> cached = get(key, type);
        if (cached instanceof CacheEntry.Hit<T>(T value)) {
            return value;
        }
        return locks.execute(
                "cache-load:" + key,
                LockOptions.defaults(),
                () -> {
                    CacheEntry<T> second = read(key, type, false);
                    if (second instanceof CacheEntry.Hit<T>(T value)) {
                        return value;
                    }
                    T loaded = loader.call();
                    if (loaded != null || cacheNull) {
                        put(key, loaded, ttl);
                    }
                    return loaded;
                });
    }

    @Override
    public <T> Map<String, CacheEntry<T>> getAll(Collection<String> names, Type type) {
        Map<String, CacheEntry<T>> result = new LinkedHashMap<>();
        RBatch batch = client.createBatch();
        Map<String, RFuture<List<Object>>> pending = new LinkedHashMap<>();
        long version = local == null ? 0 : local.generation();
        long started = System.nanoTime();
        for (String name : names) {
            String physical = keys.key("cache", name);
            String cached = local == null ? null : local.get(physical, started);
            if (cached != null) result.put(name, decode(cached, type));
            else
                pending.put(
                        name,
                        batch.getScript(StringCodec.INSTANCE)
                                .evalAsync(
                                        RScript.Mode.READ_ONLY,
                                        READ_WITH_TTL,
                                        RScript.ReturnType.LIST,
                                        List.of(physical)));
        }
        if (!pending.isEmpty()) batch.execute();
        pending.forEach(
                (name, future) -> {
                    List<Object> row = future.toCompletableFuture().join();
                    if (row.isEmpty()) result.put(name, new CacheEntry.Miss<>());
                    else {
                        String value = (String) row.getFirst();
                        long remaining = ((Number) row.get(1)).longValue();
                        String physical = keys.key("cache", name);
                        fillLocal(physical, value, remaining, started, version);
                        result.put(name, decode(value, type));
                    }
                });
        return result;
    }

    @Override
    public void putAll(Map<String, ?> values, Duration ttl) {
        RedisSupport.redisRun(
                "batch write cache",
                () -> {
                    RBatch batch = client.createBatch();
                    values.forEach(
                            (key, value) ->
                                    batch.<String>getBucket(
                                                    keys.key("cache", key), StringCodec.INSTANCE)
                                            .setAsync(json.write(value), retention(value, ttl)));
                    batch.execute();
                });
        values.keySet().forEach(key -> invalidate(keys.key("cache", key)));
    }

    @Override
    public void close() {
        regions.values().forEach(RedissonCacheService::close);
        regions.clear();
        if (listenerId >= 0 && !client.isShutdown()) {
            invalidations.unsubscribe(listenerId);
        }
        if (local != null) local.invalidate("*");
    }
}
