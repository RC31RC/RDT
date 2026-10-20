package io.github.rdt.runtime;

import io.github.rdt.runtime.PressureSnapshot;
import io.github.rdt.runtime.RuntimeComponent;
import io.github.rdt.support.RdtSupport;
import java.util.List;

/** Sanitized process metadata. Lease tokens never cross the management API. */
public record RuntimeInstance(String instanceId, String runId, String application,
                                  long startedAtMillis, long lastHeartbeatAtMillis,
                                  long leaseMillis, int queueConcurrency, int scheduleConcurrency,
                                  long generation, boolean draining, List<RuntimeComponent> components, PressureSnapshot pressure) {
    public RuntimeInstance {
        RdtSupport.require(pressure != null && components != null, "Current runtime pressure and components are required");
        components = List.copyOf(components);
    }
}
