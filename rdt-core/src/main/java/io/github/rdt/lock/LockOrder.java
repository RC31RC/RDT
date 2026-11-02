package io.github.rdt.lock;

import io.github.rdt.exception.RdtConfigurationException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Optional, immutable cross-type acquisition order. Supply the same fixed list on every node. Unknown types and
 * duplicate simple names fail rather than bypassing the declared order.
 */
public final class LockOrder {
    private final Map<String, Integer> positions;

    public LockOrder(List<Class<?>> types) {
        if (types == null || types.isEmpty()) {
            throw new RdtConfigurationException("Lock order must contain at least one resource type");
        }
        Map<String, Integer> entries = new LinkedHashMap<>();
        for (Class<?> type : types) {
            if (type == null || type.getSimpleName().isBlank()) {
                throw new RdtConfigurationException("Lock order contains an unnamed resource type");
            }
            String name = type.getSimpleName();
            if (entries.putIfAbsent(name, entries.size()) != null) {
                throw new RdtConfigurationException("Duplicate lock order resource type: " + name);
            }
        }
        positions = Map.copyOf(entries);
    }

    /** The position of a configured type; reject types omitted from a configured order. */
    public int position(String resourceType) {
        Integer position = positions.get(resourceType);
        if (position == null) {
            throw new RdtConfigurationException(
                "Unknown lock order resource type: " + resourceType + "; configured types: " + signature());
        }
        return position;
    }

    /** Stable configuration signature to compare across application instances. */
    public String signature() {
        return positions.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey)
            .reduce((a, b) -> a + "->" + b).orElseThrow();
    }
}
