package io.github.rdt.redisson.state;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.state.PersistentMapStore;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Persistent JSON map with no expiration and no read-cache side effects. */
public final class RedissonPersistentMapStore implements PersistentMapStore {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;

    public RedissonPersistentMapStore(RedissonClient client, KeySpace keys, JsonCodec json) {
        this.client = client;
        this.keys = keys;
        this.json = json;
    }

    @Override
    public <T> Map<String, T> get(String map, Type type) {
        Map<String, String> encoded = RedisSupport.redis("read persistent map", () ->
                client.<String, String>getMap(keys.key("map", map), StringCodec.INSTANCE).readAllMap());
        Map<String, T> values = new LinkedHashMap<>();
        encoded.forEach((field, value) -> values.put(field, json.read(value, type)));
        return Map.copyOf(values);
    }

    @Override
    public void putAll(String map, Map<String, ?> values) {
        Map<String, String> encoded = new LinkedHashMap<>();
        values.forEach((field, value) -> encoded.put(field, json.write(value)));
        RedisSupport.redisRun("write persistent map", () ->
                client.<String, String>getMap(keys.key("map", map), StringCodec.INSTANCE).putAll(encoded));
    }
}
