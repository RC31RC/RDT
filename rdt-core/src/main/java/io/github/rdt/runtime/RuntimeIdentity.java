package io.github.rdt.runtime;

import io.github.rdt.support.RdtSupport;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** One process identity, shared by programmatic and declarative runtime components. */
public final class RuntimeIdentity {
    private final String instanceId;
    private final boolean stableInstanceId;
    private final String runId = UUID.randomUUID().toString();
    private volatile BooleanSupplier ownership = () -> false;
    private volatile String leaseToken = "";
    private volatile long generation;
    private volatile boolean draining;
    private volatile Supplier<PressureSnapshot> admission = PressureSnapshot::unrestricted;
    private final Map<String, Supplier<RuntimeComponent>> components = new ConcurrentHashMap<>();

    public RuntimeIdentity(String configuredId) {
        stableInstanceId = configuredId != null && !configuredId.isBlank();
        instanceId = stableInstanceId ? configuredId.strip() : "instance-" + UUID.randomUUID();
        RdtSupport.component(instanceId);
    }

    public void register(String key, Supplier<RuntimeComponent> component) { components.put(key, component); }
    public void unregister(String key) { components.remove(key); }
    public List<RuntimeComponent> components() {
        return components.values().stream().map(Supplier::get)
                .sorted(Comparator.comparing(RuntimeComponent::kind).thenComparing(RuntimeComponent::id))
                .toList();
    }

    public String instanceId() { return instanceId; }
    public boolean hasStableInstanceId() { return stableInstanceId; }
    public String runId() { return runId; }
    public String leaseToken() { return leaseToken; }
    public long generation() { return generation; }
    public boolean draining() { return draining; }
    public void draining(boolean value) { draining = value; }
    public boolean accepting() { return !draining && ownership.getAsBoolean(); }
    public void admission(Supplier<PressureSnapshot> value) { admission = Objects.requireNonNull(value); }
    public PressureSnapshot pressure() { return admission.get(); }
    public boolean acceptingNewWork() { return accepting() && !pressure().paused(); }

    public BooleanSupplier guard() {
        BooleanSupplier captured = ownership;
        return () -> captured == ownership && captured.getAsBoolean();
    }

    public void acquired(String token, long version, BooleanSupplier owner) {
        RdtSupport.require(token != null && !token.isBlank() && version > 0 && owner != null,
                "An acquired runtime requires its lease token, generation and ownership guard");
        leaseToken = token;
        generation = version;
        ownership = owner;
    }

    public void suspend() {
        ownership = () -> false;
    }
}
