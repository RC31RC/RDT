package io.github.rdt.spring.lease;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeInstance;
import io.github.rdt.spring.config.RdtInstanceProperties;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.Environment;

/** Publishes sanitized process metadata while retaining application ownership of RedissonClient. */
public class RdtRuntimeRegistry implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(RdtRuntimeRegistry.class);
    private static final String RELEASE_IF_OWNER = "if redis.call('get', KEYS[1]) "
            + "and string.sub(redis.call('get', KEYS[1]), 1, string.len(ARGV[1])) == ARGV[1] "
            + "then return redis.call('del', KEYS[1]) else return 0 end";

    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final RdtProperties properties;
    private final RdtInstanceProperties instance;
    private final String application;
    private final String instanceId;
    private final String runId;
    private final RuntimeIdentity identity;
    private final long startedAtMillis = System.currentTimeMillis();
    private final RdtMetrics metrics;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong lastFailureLogNanos = new AtomicLong();
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("rdt-runtime-heartbeat").factory());

    public RdtRuntimeRegistry(RedissonClient client, KeySpace keys, JsonCodec json,
                              RdtProperties properties, RdtInstanceProperties instance,
                              Environment environment, RuntimeIdentity identity, RdtMetrics metrics) {
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.properties = properties;
        this.instance = instance;
        application = environment.getProperty("spring.application.name", "application");
        this.identity = identity;
        this.metrics=metrics;
        instanceId = identity.instanceId();
        runId = identity.runId();
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        publish();
        heartbeat.scheduleWithFixedDelay(this::heartbeatSafely, instance.renewInterval().toMillis(),
                instance.renewInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void heartbeatSafely() {
        if (!running.get()) {
            return;
        }
        try {
            publish();
        } catch (RuntimeException exception) {
            long now = System.nanoTime();
            long previous = lastFailureLogNanos.get();
            if (previous == 0 || now - previous >= TimeUnit.SECONDS.toNanos(30)) {
                lastFailureLogNanos.set(now);
                log.warn("RDT runtime heartbeat failed instanceId={} runId={}", instanceId, runId, exception);
            }
        }
    }

    private void publish() {
        if (identity.leaseToken().isBlank()) return;
        long now = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY,
                "local t=redis.call('TIME'); return t[1]*1000+math.floor(t[2]/1000)",RScript.ReturnType.LONG,List.of());
        String draining = client.<String>getBucket(keys.key("runtime-draining", instanceId), StringCodec.INSTANCE).get();
        identity.draining("true".equals(draining));
        RuntimeInstance view = new RuntimeInstance(instanceId, runId, application, startedAtMillis, now, instance.lease().toMillis(), properties.queue().maxConcurrentHandlers(), properties.schedule().maxConcurrentTasks(), identity.generation(), identity.draining(), identity.components(), identity.pressure());
        String value = identity.leaseToken() + "\n" + json.write(view);
        RBucket<String> target = bucket();
        RedisSupport.redisRun("publish RDT runtime metadata", () -> client.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                        + "return redis.call('psetex', KEYS[2], ARGV[2], ARGV[3]) end return nil",
                RScript.ReturnType.VALUE,
                List.of(keys.key("owner", "node:" + instanceId), target.getName()),
                identity.leaseToken(), instance.lease().toMillis(), value));
        client.<String>getScoredSortedSet(keys.key("runtime-index", "instances"), StringCodec.INSTANCE)
                .add(now + instance.lease().toMillis(), instanceId + ":" + runId);
    }

    private RBucket<String> bucket() {
        return client.getBucket(keys.key("runtime-instance", instanceId + ":" + runId), StringCodec.INSTANCE);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        heartbeat.shutdownNow();
        RedisSupport.redisRun("release RDT runtime metadata", () -> client.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE, RELEASE_IF_OWNER, RScript.ReturnType.LONG,
                List.of(keys.key("runtime-instance", instanceId + ":" + runId)), identity.leaseToken() + "\n"));
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}
