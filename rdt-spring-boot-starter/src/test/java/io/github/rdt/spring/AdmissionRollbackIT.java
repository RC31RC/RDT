package io.github.rdt.spring;

import io.github.rdt.support.RetryClassifier;
import io.github.rdt.lock.DefaultLockManager;

import io.github.rdt.redisson.lock.RedissonLockTemplate;

import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.redisson.schedule.JobInvocation;
import io.github.rdt.redisson.schedule.RedissonScheduledTaskTemplate;
import io.github.rdt.lock.*;
import io.github.rdt.runtime.*;
import io.github.rdt.schedule.*;
import io.github.rdt.support.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.rdt.spring.runtime.MicrometerRdtMetrics;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.redisson.Redisson;
import org.redisson.config.Config;
import org.redisson.client.codec.StringCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;

@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class AdmissionRollbackIT {
    @Test
    void capacityRejectionRollsBackLiveTokenAndReleasesReservation() {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis")).setConnectionMinimumIdleSize(1)
            .setConnectionPoolSize(4).setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(1);
        var client = Redisson.create(config);
        var keys = new KeySpace("rollback-it:" + UUID.randomUUID());
        var json = new JsonCodec();
        var options = new Binder(new MapConfigurationPropertySource(Map.of("rdt.schedule.poll-interval", "1h",
            "rdt.schedule.worker-lease", "4h", "rdt.schedule.max-concurrent-tasks", "1")))
            .bindOrCreate("rdt", RdtProperties.class);
        var identity = new RuntimeIdentity("worker");
        identity.acquired("token", 1, () -> true);
        try (var scheduler = new RedissonScheduledTaskTemplate(client, keys, json,
            new RedissonLockTemplate(client, keys), options.schedule().settings(),
            new io.github.rdt.spring.schedule.SpringCronParser(), identity, RetryClassifier.infrastructureFailures())) {
            scheduler.register("job", new ScheduleSpec.FixedRate(Duration.ofHours(2), Duration.ofHours(2)), () -> null);
            var meters = new SimpleMeterRegistry();
            scheduler.metrics(new MicrometerRdtMetrics(meters));
            Semaphore slots = (Semaphore)ReflectionTestUtils.getField(scheduler, "capacity");
            slots.acquireUninterruptibly();
            long now = System.currentTimeMillis();
            String token = "dispatch";
            var execution = new JobExecution("execution", "job", now, 0, "DISPATCHED", "worker", identity.runId(),
                token, now, now + 60000, "", 0);
            var state = client.<String, String>getMap(keys.key("schedule-state", "job"), StringCodec.INSTANCE);
            state.put("execution", json.write(execution));
            var reservations = client.<String, String>getMap(keys.key("job-reservations", "worker:" + identity.runId()),
                StringCodec.INSTANCE);
            reservations.put(token, Long.toString(now + 60000));
            ReflectionTestUtils.invokeMethod(scheduler, "executeInvocation",
                new JobInvocation("job", "execution", token));
            JobExecution ready = json.read(state.get("execution"), JobExecution.class);
            assertThat(ready.status()).isEqualTo("READY");
            assertThat(ready.retryAt()).isGreaterThan(now);
            assertThat(ready.detail()).contains("capacity");
            assertThat(reservations.containsKey(token)).isFalse();
            assertThat(meters.counter("rdt.schedule.capacity.rejected", "job", "job").count()).isEqualTo(1);
            assertThat(meters.counter("rdt.schedule.dispatch.rollback", "reason", "capacity").count()).isEqualTo(1);
            // A delayed old invocation cannot overwrite the new READY token.
            ReflectionTestUtils.invokeMethod(scheduler, "executeInvocation",
                new JobInvocation("job", "execution", token));
            assertThat(json.<JobExecution>read(state.get("execution"), JobExecution.class)).isEqualTo(ready);
            assertThat(meters.counter("rdt.schedule.capacity.rejected", "job", "job").count()).isEqualTo(1);
            assertThat(meters.counter("rdt.schedule.dispatch.rollback", "reason", "capacity").count()).isEqualTo(1);
            String nextToken = "third-rejection";
            var repeatedlyRejected = new JobExecution("execution", "job", now, 0, "DISPATCHED", "worker",
                identity.runId(), nextToken, now, now + 60000, "", 2);
            state.put("execution", json.write(repeatedlyRejected));
            reservations.put(nextToken, Long.toString(now + 60000));
            ReflectionTestUtils.invokeMethod(scheduler, "executeInvocation",
                new JobInvocation("job", "execution", nextToken));
            JobExecution delayed = json.read(state.get("execution"), JobExecution.class);
            assertThat(delayed.retryAt()).isGreaterThanOrEqualTo(now + Duration.ofHours(2).toMillis());
            assertThat(reservations.containsKey(nextToken)).isFalse();
            assertThat(meters.counter("rdt.schedule.capacity.rejected", "job", "job").count()).isEqualTo(2);
            assertThat(meters.counter("rdt.schedule.dispatch.rollback", "reason", "capacity").count()).isEqualTo(2);
            slots.release();
        } finally {
            client.shutdown();
        }
    }

    @Test
    void configuredDefaultWaitActuallyBoundsManagerAcquisition() throws Exception {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis")).setConnectionMinimumIdleSize(1)
            .setConnectionPoolSize(4).setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(1);
        var client = Redisson.create(config);
        var keys = new KeySpace("lock-policy-it:" + UUID.randomUUID());
        var template = new RedissonLockTemplate(client, keys);
        template.defaultOptions(new LockOptions(LockType.REENTRANT, Duration.ofMillis(100), Duration.ZERO));
        var manager = new DefaultLockManager(template);
        LockKey heldKey = LockKey.named(AdmissionRollbackIT.class, "held");
        try (var held = template.tryLock(heldKey.value()).orElseThrow();
            var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            long elapsed = executor.submit(() -> {
                long start = System.nanoTime();
                try (var scope = manager.beginScope()) {
                    assertThatThrownBy(() -> manager.lock(heldKey)).hasMessageContaining("deadline");
                }
                return Duration.ofNanos(System.nanoTime() - start).toMillis();
            }).get();
            assertThat(elapsed).isBetween(70L, 1500L);
        } finally {
            client.shutdown();
        }
    }
}
