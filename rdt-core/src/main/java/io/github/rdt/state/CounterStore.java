package io.github.rdt.state;

/** Atomic Redis counters, distinct from read-cache entries. */
public interface CounterStore {
    long increment(String key, long delta);
}
