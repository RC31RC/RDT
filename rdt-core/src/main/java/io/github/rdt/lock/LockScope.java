package io.github.rdt.lock;

import java.util.Collection;

/** One synchronous business call's lock set and release boundary. */
public interface LockScope extends AutoCloseable {
    /** Append a mutual-exclusion range to this scope. */
    void lock(LockKey key, LockOptions options);

    /** Deduplicate and order keys before acquiring them in this scope. */
    void lockAll(Collection<LockKey> keys, LockOptions options);

    /** Default lock strategy supplied by the backend. */
    default LockOptions defaultOptions() {
        return LockOptions.defaults();
    }

    /** Acquire one lock using the effective default strategy. */
    default void lock(LockKey key) {
        lock(key, defaultOptions());
    }

    /** Acquire multiple locks using the effective default strategy. */
    default void lockAll(Collection<LockKey> keys) {
        lockAll(keys, defaultOptions());
    }

    /** Whether this scope holds the specified logical key with the default lock type. */
    default boolean holds(LockKey key) {
        return holds(key, defaultOptions().type());
    }

    /** Whether this scope holds the specified logical key and lock type. */
    boolean holds(LockKey key, LockType type);

    /** Close this scope; physically release locks in reverse acquisition order unless deferred. */
    @Override
    void close();
}
