package io.github.rdt.runtime;

import java.util.List;

/** Cached local admission decision; negative readings mean a sensor is unavailable. */
public record PressureSnapshot(boolean paused, double cpuLoad, double databaseUsage,
                               long sampledAtMillis, List<String> reasons) {
    public PressureSnapshot {
        reasons = List.copyOf(reasons);
    }

    public static PressureSnapshot unrestricted() {
        return new PressureSnapshot(false, -1, -1, 0, List.of());
    }
}
