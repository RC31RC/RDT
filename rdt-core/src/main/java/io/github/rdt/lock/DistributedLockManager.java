package io.github.rdt.lock;

import java.util.Collection;

/**
 * Manage locks for one synchronous business scope. Scopes and handles stay on the owning thread; the default Spring
 * wrapper can postpone physical release until transaction completion.
 */
public interface DistributedLockManager {
    /** Open a scope; {@code @RdtLock} does this for a proxied method but does not acquire a lock. */
    LockScope beginScope();

    /** Add an actual mutual-exclusion range to the current scope. */
    void lock(LockKey key, LockOptions options);

    /** Add multiple keys in a stable order, deduplicating before acquisition. */
    void lockAll(Collection<LockKey> keys, LockOptions options);

    /** Default lock strategy supplied by the backend. */
    default LockOptions defaultOptions() {
        return LockOptions.defaults();
    }

    /** Acquire one lock using the effective default strategy. */
    default void lock(LockKey key) {
        lock(key, defaultOptions());
    }

    /** Acquire several locks using the effective default strategy. */
    default void lockAll(Collection<LockKey> keys) {
        lockAll(keys, defaultOptions());
    }

    /** Current thread's open scope, or null. */
    LockScope currentScope();
}
