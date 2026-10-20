package io.github.rdt.redisson.state;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.state.ExpiringMapStore;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** JSON fields with independent TTLs. These writes do not publish cache invalidations. */
public final class RedissonExpiringMapStore implements ExpiringMapStore {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;

    public RedissonExpiringMapStore(RedissonClient client, KeySpace keys, JsonCodec json) {
        this.client = client;
        this.keys = keys;
        this.json = json;
    }

    @Override
    public <T> Optional<T> get(String map, String field, Type type) {
        String encoded = RedisSupport.redis("read expiring map field", () ->
                client.<String, String>getMapCache(keys.key("map-cache", map), StringCodec.INSTANCE).get(field));
        return Optional.ofNullable(encoded).map(value -> json.read(value, type));
    }

    @Override
    public <T> Map<String, T> getAll(String map, Type type) {
        Map<String, String> encoded = RedisSupport.redis("read expiring map", () ->
                client.<String, String>getMapCache(keys.key("map-cache", map), StringCodec.INSTANCE).readAllMap());
        Map<String, T> values = new LinkedHashMap<>();
        encoded.forEach((field, value) -> values.put(field, json.read(value, type)));
        return Map.copyOf(values);
    }

    @Override
    public void put(String map, String field, Object value, Duration ttl) {
        RdtSupport.require(value != null, "Expiring map value must not be null");
        RdtSupport.positive(ttl, "field ttl");
        RedisSupport.redisRun("write expiring map field", () ->
                client.<String, String>getMapCache(keys.key("map-cache", map), StringCodec.INSTANCE)
                        .fastPut(field, json.write(value), ttl.toMillis(), TimeUnit.MILLISECONDS));
    }

    @Override
    public void remove(String map, String field) {
        RedisSupport.redisRun("remove expiring map field", () ->
                client.<String, String>getMapCache(keys.key("map-cache", map), StringCodec.INSTANCE).fastRemove(field));
    }
}
