package io.github.rdt.spring.runtime;

/** Returns active/max connections in [0,1], or a negative value if no pool is active. */
@FunctionalInterface
public interface DatabasePressureProbe {
    double usage();
}
