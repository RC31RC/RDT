package io.github.rdt.spring.runtime;

import com.sun.management.OperatingSystemMXBean;
import io.github.rdt.runtime.AdaptiveAdmissionController;
import io.github.rdt.runtime.RuntimeIdentity;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Samples local metrics outside Redis/Netty callbacks. Existing executions keep their lease renewal. */
public final class RdtPressureMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RdtPressureMonitor.class);
    private final RdtAdmissionProperties properties;
    private final List<DatabasePressureProbe> databases;
    private final AdaptiveAdmissionController admission;
    private ScheduledExecutorService timer;

    public RdtPressureMonitor(RdtAdmissionProperties properties, RuntimeIdentity identity,
                               List<DatabasePressureProbe> databases) {
        this.properties = properties;
        this.databases = List.copyOf(databases);
        admission = new AdaptiveAdmissionController(properties.cpuPause(), properties.cpuResume(),
                properties.databasePause(), properties.databaseResume(), properties.recoveryDelay());
        identity.admission(admission);
    }

    public void start() {
        if (!properties.enabled()) return;
        sample();
        timer = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("rdt-pressure").factory());
        timer.scheduleWithFixedDelay(this::sample, properties.sampleInterval().toMillis(),
                properties.sampleInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    public void sample() {
        boolean previous = admission.get().paused();
        try {
            double cpu = ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean os
                    ? Math.max(os.getCpuLoad(), os.getProcessCpuLoad()) : -1;
            double database = databases.stream().mapToDouble(DatabasePressureProbe::usage).max().orElse(-1);
            admission.refresh(cpu, database, System.currentTimeMillis());
        } catch (RuntimeException failure) {
            admission.failed(System.currentTimeMillis());
            if (!previous) log.warn("RDT pressure sampling failed; new work paused", failure);
        }
        if (previous != admission.get().paused()) {
            log.warn("RDT new work admission paused={} reasons={}", admission.get().paused(), admission.get().reasons());
        }
    }

    @Override
    public void close() {
        if (timer != null) timer.shutdownNow();
    }
}
