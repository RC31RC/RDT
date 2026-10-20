package io.github.rdt.spring.lease;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService.Lease;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.spring.config.RdtInstanceProperties;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.spring.support.RdtAnnotationRegistrar;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.scheduling.support.TaskUtils;

/** A stable identity is recovered only after its previous consumers and resources are revoked. */
public class NodeIdentityLease implements SmartInitializingSingleton, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NodeIdentityLease.class);
    private final RedisOwnerLeaseService leases;
    private final RdtProperties rdt;
    private final RdtInstanceProperties properties;
    private final ObjectProvider<RdtAnnotationRegistrar> registrar;
    private final Supplier<List<NodeIdentityListener>> listeners;
    private final RuntimeIdentity runtime;
    private ScheduledExecutorService renewal;
    private volatile Lease lease;
    private volatile boolean healthy;
    private volatile boolean closed;
    private boolean waiting;

    public NodeIdentityLease(RedisOwnerLeaseService leases, RdtProperties rdt,
            RdtInstanceProperties properties, ObjectProvider<RdtAnnotationRegistrar> registrar,
            Supplier<List<NodeIdentityListener>> listeners, RuntimeIdentity runtime) {
        this.leases = leases;
        this.rdt = rdt;
        this.properties = properties;
        this.registrar = registrar;
        this.listeners = listeners;
        this.runtime = runtime;
        runtime.suspend();
    }

    private String instanceId() {
        return runtime.instanceId();
    }

    private void publishOwnership() {
        Lease captured = lease;
        runtime.acquired(captured.token(), captured.fencingVersion(), guard());
    }

    @Override
    public synchronized void afterSingletonsInstantiated() {
        lease = leases.acquire(name(), instanceId(), properties.lease())
                .orElseThrow(() -> new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "rdt.instance-id is already running: " + rdt.instanceId()));
        healthy = true;
        publishOwnership();
        registrar.ifAvailable(value -> value.setExecutionGuard(this::guard));
        renewal = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("rdt-identity-renewal").factory());
        // Spring logs failures at this scheduling boundary and keeps the next recovery attempt alive.
        renewal.scheduleWithFixedDelay(TaskUtils.decorateTaskWithErrorHandler(this::renew, null, true),
                properties.renewInterval().toMillis(), properties.renewInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void renew() {
        if (closed) return;
        try {
            if (healthy && leases.renew(lease, properties.lease())) return;
            if (healthy) log.warn("Node identity lost; revoking resources before recovery: {}", rdt.instanceId());
            healthy = false;
            suspend();
            if (lease != null) {
                leases.release(lease);
                lease = null;
            }
            var acquired = leases.acquire(name(), instanceId(), properties.lease());
            if (acquired.isEmpty()) {
                if (!waiting) log.warn("Node identity is held by another process; waiting: {}", rdt.instanceId());
                waiting = true;
                return;
            }
            lease = acquired.get();
            healthy = true;
            publishOwnership();
            registrar.ifAvailable(RdtAnnotationRegistrar::start);
            waiting = false;
            log.info("Node identity recovered; consumers and schedules restarted: {}", rdt.instanceId());
        } catch (RuntimeException exception) {
            // Public calls must see cleanup, Redis and registration failures; recovery retries later.
            healthy = false;
            try {
                suspend();
            } catch (RuntimeException cleanupFailure) {
                if (cleanupFailure != exception) exception.addSuppressed(cleanupFailure);
            }
            throw exception;
        }
    }

    private void suspend() {
        runtime.suspend();
        registrar.ifAvailable(RdtAnnotationRegistrar::stop);
        listeners.get().forEach(NodeIdentityListener::identityLost);
    }

    public boolean isHealthy() {
        return !closed && healthy && leases.isHeld(lease);
    }

    public void requireHealthy() {
        if (!isHealthy()) throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "Node identity lease unavailable; recovery is pending: " + rdt.instanceId());
    }

    public BooleanSupplier guard() {
        Lease acquired = lease;
        // A callback from a previous acquisition stays fenced after this node recovers.
        return () -> !closed && healthy && acquired != null
                && acquired.equals(lease) && leases.isHeld(acquired);
    }

    private String name() {
        return "node:" + instanceId();
    }

    @Override
    public synchronized void close() {
        closed = true;
        healthy = false;
        if (renewal != null) renewal.shutdownNow();
        suspend();
        if (lease != null) {
            leases.release(lease);
            lease = null;
        }
    }
}
