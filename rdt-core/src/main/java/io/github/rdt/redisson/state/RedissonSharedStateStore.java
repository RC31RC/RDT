package io.github.rdt.redisson.state;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.state.SharedStateStore;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Shared JSON facts without local caching or invalidation-bus traffic. */
public final class RedissonSharedStateStore implements SharedStateStore {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;

    public RedissonSharedStateStore(RedissonClient client, KeySpace keys, JsonCodec json) {
        this.client = client;
        this.keys = keys;
        this.json = json;
    }

    @Override
    public <T> Optional<T> get(String key, Type type) {
        String encoded = RedisSupport.redis("read shared state", () ->
                client.<String>getBucket(keys.key("state", key), StringCodec.INSTANCE).get());
        return Optional.ofNullable(encoded).map(value -> json.read(value, type));
    }

    @Override
    public <T> Map<String, T> getAll(Collection<String> names, Type type) {
        if (names.isEmpty()) {
            return Map.of();
        }
        return RedisSupport.redis("read shared states", () -> {
            RBatch batch = client.createBatch();
            Map<String, RFuture<String>> reads = new LinkedHashMap<>();
            names.stream().distinct().forEach(name -> reads.put(name,
                    batch.<String>getBucket(keys.key("state", name), StringCodec.INSTANCE).getAsync()));
            batch.execute();
            Map<String, T> values = new LinkedHashMap<>();
            reads.forEach((name, future) -> {
                String encoded = future.toCompletableFuture().join();
                if (encoded != null) {
                    values.put(name, json.read(encoded, type));
                }
            });
            return Map.copyOf(values);
        });
    }

    @Override
    public void put(String key, Object value, Duration ttl) {
        RdtSupport.require(value != null, "Shared state value must not be null");
        RdtSupport.positive(ttl, "shared state ttl");
        RedisSupport.redisRun("write shared state", () ->
                client.<String>getBucket(keys.key("state", key), StringCodec.INSTANCE)
                        .set(json.write(value), ttl));
    }

    @Override
    public void putAll(Map<String, ?> values, Duration ttl) {
        RdtSupport.positive(ttl, "shared state ttl");
        values.forEach((key, value) -> RdtSupport.require(value != null,
                "Shared state value must not be null: " + key));
        if (values.isEmpty()) {
            return;
        }
        RedisSupport.redisRun("write shared states", () -> {
            RBatch batch = client.createBatch();
            values.forEach((key, value) -> batch.<String>getBucket(keys.key("state", key), StringCodec.INSTANCE)
                    .setAsync(json.write(value), ttl));
            batch.execute();
        });
    }

    @Override
    public void evict(String key) {
        RedisSupport.redisRun("evict shared state", () ->
                client.getBucket(keys.key("state", key), StringCodec.INSTANCE).delete());
    }
}
