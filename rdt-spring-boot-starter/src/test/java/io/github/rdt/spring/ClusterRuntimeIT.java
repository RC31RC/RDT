package io.github.rdt.spring;

import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.support.RetryClassifier;
import io.github.rdt.runtime.RdtMetrics;

import io.github.rdt.redisson.lease.RedisOwnerLeaseService;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.queue.RedisFailedMessageStore;
import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;
import io.github.rdt.runtime.AdaptiveAdmissionController;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.schedule.JobRoute;
import io.github.rdt.schedule.ScheduleOptions;
import io.github.rdt.schedule.ScheduleSpec;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.spring.config.RdtInstanceProperties;
import io.github.rdt.spring.lease.RdtRuntimeRegistry;
import io.github.rdt.redisson.schedule.RedissonScheduledTaskTemplate;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RScript;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.mock.env.MockEnvironment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@EnabledIfSystemProperty(named = "rdt.it.cluster", matches = ".+")
class ClusterRuntimeIT {
    private RedissonClient client() {
        var config = new Config().setThreads(2).setNettyThreads(2);
        config.useClusterServers().addNodeAddress(System.getProperty("rdt.it.cluster").split(","))
                .setReadMode(ReadMode.MASTER).setMasterConnectionMinimumIdleSize(1).setMasterConnectionPoolSize(8)
                .setSlaveConnectionMinimumIdleSize(1).setSlaveConnectionPoolSize(2)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(4);
        return Redisson.create(config);
    }

    private RuntimeIdentity identity(RedissonClient client, KeySpace keys, String id) {
        var identity = new RuntimeIdentity(id);
        var leases = new RedisOwnerLeaseService(client, keys, new RedissonLockTemplate(client, keys));
        var lease = leases.acquire("node:" + id, id, Duration.ofMinutes(2)).orElseThrow();
        identity.acquired(lease.token(), lease.fencingVersion(), () -> leases.isHeld(lease));
        return identity;
    }

    @Test
    void delayRetryDeadLetterAndRuntimeScriptsExecuteOnRealCluster() {
        var client = client();
        var keys = new KeySpace("cluster-it:" + UUID.randomUUID());
        var json = new JsonCodec();
        var locks = new RedissonLockTemplate(client, keys);
        var identity = identity(client, keys, "worker");
        var store = new RedisFailedMessageStore(client, keys, json);
        var queries = new RedissonManagementQueryService(client, keys, json);
        var calls = new AtomicInteger();
        var policy = new QueuePolicy(2, Duration.ofMillis(100), Duration.ofMillis(300), Duration.ofMillis(100),
                Duration.ofSeconds(2), true, Duration.ofMinutes(1), Duration.ZERO, 0, 1, Duration.ofSeconds(1), 2, 1, 2, 2, 262144, 10000, 67108864L, 16777216);
        try (var queue = new RedissonMessageQueueTemplate(client, keys, json, policy, locks, store, identity, RetryClassifier.infrastructureFailures())) {
            // Probe slot co-location without overwriting the live worker's identity lease.
            String first = keys.key("owner", "node:slot-probe");
            String second = keys.key("runtime-instance", "slot-probe:test-run");
            assertThat(client.getScript(StringCodec.INSTANCE).<Long>eval(RScript.Mode.READ_WRITE,
                    "redis.call('set', KEYS[1], 'ok'); redis.call('set', KEYS[2], 'ok'); return 1",
                    RScript.ReturnType.LONG, List.of(first, second))).isEqualTo(1L);
            assertThat(identity.acceptingNewWork()).isTrue();
            assertThatThrownBy(() -> client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                    "return 1", RScript.ReturnType.LONG, List.of("{slot-a}", "{slot-b}")))
                    .hasMessageContaining("CROSSSLOT");
            queue.subscribe(QueueSubscription.concurrent("delayed", QueueMode.STREAM, "g", 2), message -> {
                calls.incrementAndGet();
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "downstream unavailable");
            });
            queue.sendDelayed("delayed", "json payload", Duration.ofMillis(100));
            await().atMost(Duration.ofSeconds(6)).untilAsserted(() -> {
                assertThat(calls).hasValue(2);
                assertThat(queries.deadLetterStats(Duration.ofMinutes(5)).retainedCount()).isEqualTo(1);
            });
            var dead = queries.deadLetters(0, 10).items().getFirst();
            assertThat(new RedissonManagementCommandService(client, keys, locks, json)
                    .deleteDeadLetter(dead.destination(), dead.group(), dead.messageId(), "")).isTrue();
            assertThat(queries.deadLetterStats(Duration.ofMinutes(5)).retainedCount()).isZero();
            // Redisson MapCache timeout/index keys retain the original key hash tag.
            store.save(FailedMessage.stream("expiry", "g", Message.create("value", json), 2, "test", Instant.now().toString()),
                    Duration.ofMillis(100));
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(queries.deadLetterStats(Duration.ofMinutes(5)).retainedCount()).isZero());
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void clusterNativeWorkerRoutingRespectsPressureAndKeepsIdentityAlive() {
        var a = client();
        var b = client();
        var keys = new KeySpace("cluster-worker-it:" + UUID.randomUUID());
        var json = new JsonCodec();
        var first = identity(a, keys, "worker-a");
        var second = identity(b, keys, "worker-b");
        var pressure = new AdaptiveAdmissionController(.85, .65, .9, .7, Duration.ZERO);
        second.admission(pressure);
        pressure.refresh(.2, 1, System.currentTimeMillis());
        var properties = new Binder(new MapConfigurationPropertySource(Map.of("rdt.schedule.poll-interval", "100ms",
                "rdt.schedule.max-concurrent-tasks", "1", "rdt.schedule.worker-lease", "3s")))
                .bindOrCreate("rdt", RdtProperties.class);
        var timing = new RdtInstanceProperties(Duration.ofSeconds(10), Duration.ofSeconds(1));
        var ra = new RdtRuntimeRegistry(a, keys, json, properties, timing, new MockEnvironment(), first, RdtMetrics.NONE);
        var rb = new RdtRuntimeRegistry(b, keys, json, properties, timing, new MockEnvironment(), second, RdtMetrics.NONE);
        var calls = new AtomicInteger();
        try (var sa = new RedissonScheduledTaskTemplate(a, keys, json, new RedissonLockTemplate(a, keys), properties.schedule().settings(), new io.github.rdt.spring.schedule.SpringCronParser(), first, RetryClassifier.infrastructureFailures());
             var sb = new RedissonScheduledTaskTemplate(b, keys, json, new RedissonLockTemplate(b, keys), properties.schedule().settings(), new io.github.rdt.spring.schedule.SpringCronParser(), second, RetryClassifier.infrastructureFailures())) {
            var cadence = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO);
            var options = ScheduleOptions.builder().route(JobRoute.SPECIFIED).targetInstanceId("worker-b").build();
            sa.register("routed", cadence, options, () -> { throw new AssertionError("Wrong worker"); });
            sb.register("routed", cadence, options, () -> { calls.incrementAndGet(); return Map.of("result", "JSON DTO"); });
            ra.start();
            rb.start();
            var queries = new RedissonManagementQueryService(a, keys, json);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(queries.instances()).hasSize(2);
                assertThat(queries.executions("routed", 0, 10)).singleElement().satisfies(execution -> {
                    assertThat(execution.status()).isEqualTo("WAITING");
                    assertThat(execution.attempt()).isZero();
                });
                assertThat(second.guard().getAsBoolean()).isTrue();
            });
            pressure.refresh(.1, .1, System.currentTimeMillis());
            await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
                assertThat(calls).hasValue(1);
                assertThat(queries.taskResults("routed", 0, 10)).singleElement().satisfies(result -> {
                    assertThat(result.instanceId()).isEqualTo("worker-b");
                    assertThat(result.successful()).isTrue();
                });
            });
        } finally {
            ra.stop();
            rb.stop();
            a.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            b.shutdown();
            a.shutdown();
        }
    }
}
