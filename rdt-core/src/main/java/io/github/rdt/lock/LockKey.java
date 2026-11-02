package io.github.rdt.lock;

import io.github.rdt.exception.RdtConfigurationException;
import java.util.Objects;

/**
 * Immutable identity of a mutual-exclusion range, not necessarily a database row. The simple type name must be unique
 * within the application's shared Redis namespace.
 */
public record LockKey(String resourceType, String scopeId) {
    public LockKey {
        requirePart(resourceType, "Resource type");
        requirePart(scopeId, "Lock scope");
        if (resourceType.indexOf(':') >= 0) {
            throw new RdtConfigurationException("Resource type cannot contain a colon");
        }
    }

    /** Protect one resource identified by its stable ID. */
    public static LockKey forResource(Class<?> resourceType, Object id) {
        return new LockKey(typeName(resourceType), part(id, "Resource ID"));
    }

    /** Protect an operation, optionally scoped to a request, task or business identifier. */
    public static LockKey forOperation(Class<?> ownerType, String operation, Object scopeId) {
        String suffix = requirePart(operation, "Operation");
        if (scopeId != null) {
            suffix += ":" + part(scopeId, "Operation scope ID");
        }
        return new LockKey(typeName(ownerType), suffix);
    }

    /** Protect a named singleton coordination range. */
    public static LockKey named(Class<?> ownerType, String purpose) {
        return new LockKey(typeName(ownerType), requirePart(purpose, "Purpose"));
    }

    /** Logical key passed to the lock backend; the backend adds its namespace and lock key-space. */
    public String value() {
        return resourceType + ":" + scopeId;
    }

    private static String typeName(Class<?> type) {
        if (type == null) {
            throw new RdtConfigurationException("Resource type is required");
        }
        return requirePart(type.getSimpleName(), "Resource type simple name");
    }

    private static String part(Object value, String label) {
        return requirePart(Objects.toString(value, null), label);
    }

    private static String requirePart(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new RdtConfigurationException(label + " is required");
        }
        return value;
    }
}
