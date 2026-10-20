package io.github.rdt.state;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Field-level TTL map storage, independent of disposable bucket-cache invalidations. */
public interface ExpiringMapStore {
    <T> Optional<T> get(String map, String field, Type type);

    <T> Map<String, T> getAll(String map, Type type);

    void put(String map, String field, Object value, Duration ttl);

    void remove(String map, String field);
}
