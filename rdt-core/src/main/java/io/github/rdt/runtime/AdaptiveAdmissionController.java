package io.github.rdt.runtime;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** Hysteresis protects control traffic while new business work is paused. Refresh never acquires a DB connection. */
public final class AdaptiveAdmissionController implements Supplier<PressureSnapshot> {
    private final double cpuPause;
    private final double cpuResume;
    private final double databasePause;
    private final double databaseResume;
    private final long recoveryMillis;
    private volatile PressureSnapshot snapshot = PressureSnapshot.unrestricted();
    private long recoveredSince = -1;

    public AdaptiveAdmissionController(double cpuPause, double cpuResume, double databasePause,
                                        double databaseResume, Duration recoveryDelay) {
        RdtSupport.require(cpuResume > 0 && cpuResume < cpuPause && cpuPause <= 1,
                "CPU thresholds require 0 < resume < pause <= 1");
        RdtSupport.require(databaseResume > 0 && databaseResume < databasePause && databasePause <= 1,
                "Database thresholds require 0 < resume < pause <= 1");
        RdtSupport.require(recoveryDelay != null && !recoveryDelay.isNegative(), "Recovery delay must not be negative");
        this.cpuPause = cpuPause;
        this.cpuResume = cpuResume;
        this.databasePause = databasePause;
        this.databaseResume = databaseResume;
        recoveryMillis = recoveryDelay.toMillis();
    }

    public synchronized void refresh(double cpu, double database, long now) {
        if (!Double.isFinite(cpu) || !Double.isFinite(database)) { failed(now); return; }
        List<String> reasons = new ArrayList<>();
        boolean wasPaused = snapshot.paused();
        if (cpu >= (wasPaused ? cpuResume : cpuPause)) reasons.add("CPU_PRESSURE");
        if (database >= (wasPaused ? databaseResume : databasePause)) reasons.add("DATABASE_PRESSURE");
        if (!reasons.isEmpty()) {
            recoveredSince = -1;
        } else if (wasPaused) {
            if (recoveredSince < 0) recoveredSince = now;
            if (now - recoveredSince < recoveryMillis) reasons.add("RECOVERY_COOLDOWN");
        }
        snapshot = new PressureSnapshot(!reasons.isEmpty(), cpu, database, now, reasons);
    }

    public synchronized void failed(long now) {
        recoveredSince = -1;
        snapshot = new PressureSnapshot(true, -1, -1, now, List.of("PRESSURE_SENSOR_FAILURE"));
    }

    @Override
    public PressureSnapshot get() {
        return snapshot;
    }
}
