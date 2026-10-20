package io.github.rdt.runtime;

import java.util.function.ToDoubleFunction;

/** Low-cardinality runtime observations; identifiers of individual deliveries are never tags. */
public interface RdtMetrics {
    RdtMetrics NONE = new RdtMetrics() { };
    default void increment(String name, String... tags) { }
    default <T> void gauge(String name, T state, ToDoubleFunction<T> value, String... tags) { }
}
