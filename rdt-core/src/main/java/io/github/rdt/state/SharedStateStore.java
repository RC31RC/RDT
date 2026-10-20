package io.github.rdt.state;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/** Redis-backed shared facts and coordination state; never served from a local cache. */
public interface SharedStateStore {
    <T> Optional<T> get(String key, Type type);

    <T> Map<String, T> getAll(Collection<String> keys, Type type);

    void put(String key, Object value, Duration ttl);

    void putAll(Map<String, ?> values, Duration ttl);

    void evict(String key);
}
