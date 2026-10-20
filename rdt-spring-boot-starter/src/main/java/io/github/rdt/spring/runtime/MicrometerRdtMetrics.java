package io.github.rdt.spring.runtime;

import io.github.rdt.runtime.RdtMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.function.ToDoubleFunction;

public final class MicrometerRdtMetrics implements RdtMetrics {
    private final MeterRegistry registry;
    public MicrometerRdtMetrics(MeterRegistry registry) { this.registry = registry; }
    @Override
    public void increment(String name, String... tags) { registry.counter(name, tags).increment(); }
    @Override
    public <T> void gauge(String name, T state, ToDoubleFunction<T> value, String... tags) {
        registry.gauge(name, Tags.of(tags), state, value);
    }
}
