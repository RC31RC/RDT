package io.github.rdt.state;

import java.lang.reflect.Type;
import java.util.Map;

/** Persistent Redis map storage with no TTL or cache invalidation semantics. */
public interface PersistentMapStore {
    <T> Map<String, T> get(String map, Type type);

    void putAll(String map, Map<String, ?> values);
}
