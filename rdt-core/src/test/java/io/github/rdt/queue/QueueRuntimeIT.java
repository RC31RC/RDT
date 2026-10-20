package io.github.rdt.queue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.support.RetryClassifier;
import io.github.rdt.redisson.queue.RedisFailedMessageStore;

import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;

import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.runtime.AdaptiveAdmissionController;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService;

import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.schedule.TaskResult;
import org.redisson.client.codec.StringCodec;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import org.redisson.api.stream.StreamMessageId;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisNode;
import org.redisson.api.redisnode.RedisNodes;
import org.redisson.config.Config;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class QueueRuntimeIT {
    @ParameterizedTest
    @EnumSource(QueueMode.class)
    void applicationClassifierControlsEveryDeliveryMode(QueueMode mode) {
        var client = client();
        var keys = new KeySpace("retry-classifier-it:" + UUID.randomUUID());
        var failures = new CopyOnWriteArrayList<FailedMessage>();
        var systemCalls = new AtomicInteger();
        var businessCalls = new AtomicInteger();
        var unknownCalls = new AtomicInteger();
        RetryClassifier classifier = failure -> failure instanceof TestSystemFailure;
        try (var queue = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(),
                policy(Duration.ofMillis(50), 1), new RedissonLockTemplate(client, keys), failures::add,
                leasedIdentity(client, keys, "classified-worker"), classifier)) {
            queue.subscribe(QueueSubscription.concurrent("system", mode, "g", 1), message -> {
                systemCalls.incrementAndGet();
                throw new TestSystemFailure();
            });
            queue.subscribe(QueueSubscription.concurrent("business", mode, "g", 1), message -> {
                businessCalls.incrementAndGet();
                throw new TestBusinessFailure();
            });
            queue.subscribe(QueueSubscription.concurrent("unknown", mode, "g", 1), message -> {
                unknownCalls.incrementAndGet();
                throw new IllegalArgumentException("unknown failures are permanent by default");
            });
            queue.send("system", "work", mode);
            queue.send("business", "work", mode);
            queue.send("unknown", "work", mode);
            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                assertThat(failures).hasSize(3);
                assertThat(systemCalls).hasValue(3);
                assertThat(businessCalls).hasValue(1);
                assertThat(unknownCalls).hasValue(1);
            });
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(systemCalls).hasValue(3);
                assertThat(businessCalls).hasValue(1);
                assertThat(unknownCalls).hasValue(1);
            });
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    private static final class TestSystemFailure extends RuntimeException { }
    private static final class TestBusinessFailure extends RuntimeException { }

    private RedissonClient client() {
        Config config = new Config();
        config.setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(12)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(4);
        return Redisson.create(config);
    }
    private QueuePolicy policy(Duration poll, int batch) {
        return new QueuePolicy(3, Duration.ofMillis(100), Duration.ofSeconds(30), poll, Duration.ofSeconds(3), true, Duration.ofMinutes(5), Duration.ofHours(1), 10000, 1, Duration.ofSeconds(1), 8, 3, batch, 32, 262144, 10000, 67108864L, 16777216);
    }
    @Test
    void broadcastRetryReleasesHandlerSlotWhileWaiting() throws Exception {
        var client = client();
        var keys = new KeySpace("broadcast-retry-it:" + UUID.randomUUID());
        var policy = new QueuePolicy(3, Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMillis(100),
                Duration.ofSeconds(2), true, Duration.ofDays(1), Duration.ofHours(1), 10000,
                1, Duration.ofSeconds(2), 1, 1, 1, 4, 262144, 10000, 67108864L, 16777216);
        var calls = new AtomicInteger();
        var work = new AtomicInteger();
        var failed = new CountDownLatch(1);
        try (var queue = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(), policy,
                new RedissonLockTemplate(client, keys), failure -> { throw new AssertionError(failure); },
                leasedIdentity(client, keys, "broadcast-worker"), RetryClassifier.infrastructureFailures())) {
            queue.subscribe(QueueSubscription.concurrent("notice", QueueMode.BROADCAST, "online", 1), message -> {
                if (calls.incrementAndGet() == 1) {
                    failed.countDown();
                    throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "retry outside the handler slot");
                }
            });
            queue.subscribe(QueueSubscription.concurrent("work", QueueMode.STREAM, "workers", 1),
                    message -> work.incrementAndGet());
            queue.broadcast("notice", "event");
            assertThat(failed.await(3, TimeUnit.SECONDS)).isTrue();
            queue.send("work", "must not wait for broadcast backoff");
            await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(work).hasValue(1));
            assertThat(calls).hasValue(1);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(calls).hasValue(2));
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void cancelledBroadcastSubscriptionCannotRunItsScheduledRetry() throws Exception {
        var client = client();
        var keys = new KeySpace("broadcast-cancel-it:" + UUID.randomUUID());
        var calls = new AtomicInteger();
        var failed = new CountDownLatch(1);
        try (var queue = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(), policy(Duration.ofMillis(100), 1),
                new RedissonLockTemplate(client, keys), failure -> { throw new AssertionError(failure); },
                leasedIdentity(client, keys, "broadcast-worker"), RetryClassifier.infrastructureFailures())) {
            var subscription = queue.subscribe(QueueSubscription.concurrent("notice", QueueMode.BROADCAST, "online", 1), message -> {
                calls.incrementAndGet();
                failed.countDown();
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "unsubscribe before retry");
            });
            queue.broadcast("notice", "event");
            assertThat(failed.await(3, TimeUnit.SECONDS)).isTrue();
            subscription.close();
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(calls).hasValue(1));
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void broadcastDeadLettersKeepEachReceivingInstanceAndFailureBudget() {
        var client = client();
        var keys = new KeySpace("broadcast-dlq-it:" + UUID.randomUUID());
        var json = new JsonCodec();
        var locks = new RedissonLockTemplate(client, keys);
        var failures = new RedisFailedMessageStore(client, keys, json);
        var spec = QueueSubscription.concurrent("notice", QueueMode.BROADCAST, "online", 1);
        try (var first = new RedissonMessageQueueTemplate(client, keys, json, policy(Duration.ofMillis(100), 1),
                locks, failures, leasedIdentity(client, keys, "first"), RetryClassifier.infrastructureFailures());
             var second = new RedissonMessageQueueTemplate(client, keys, json, policy(Duration.ofMillis(100), 1),
                     locks, failures, leasedIdentity(client, keys, "second"), RetryClassifier.infrastructureFailures())) {
            MessageHandler fail = message -> { throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "always fails"); };
            first.subscribe(spec, fail);
            second.subscribe(spec, fail);
            first.broadcast("notice", "same event");
            var dlq = client.<String, String>getMapCache(keys.key("dead-letters", "notice"), StringCodec.INSTANCE);
            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(dlq.size()).isEqualTo(2));
            var records = dlq.readAllValues().stream().map(value -> json.<FailedMessage>read(value, FailedMessage.class)).toList();
            assertThat(records).extracting(FailedMessage::instanceId).containsExactlyInAnyOrder("first", "second");
            assertThat(records).allSatisfy(failure -> {
                assertThat(failure.mode()).isEqualTo(QueueMode.BROADCAST);
                assertThat(failure.failureCount()).isEqualTo(3);
            });
            assertThat(records).extracting(failure -> failure.message().id()).containsOnly(records.getFirst().message().id());
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void delayedMessageSurvivesProducerShutdownAndIdleReadersDoNotConsumeBroadcastBudget() {
        var client = client();
        KeySpace keys = new KeySpace("rdt-delay-it:" + UUID.randomUUID());
        JsonCodec json = new JsonCodec();
        var locks = new RedissonLockTemplate(client, keys);
        try {
            String id;
            try (var producer = new RedissonMessageQueueTemplate(client, keys, json, policy(Duration.ofSeconds(2), 4), locks, failure -> { }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
                id = producer.sendDelayed("later", "payload", Duration.ofMillis(200));
            }
            List<String> received = new CopyOnWriteArrayList<>();
            AtomicInteger broadcast = new AtomicInteger();
            try (var consumer = new RedissonMessageQueueTemplate(client, keys, json, policy(Duration.ofSeconds(2), 4), locks, failure -> { }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
                for (int i = 0; i < 12; i++) consumer.subscribe(new QueueSubscription("idle-" + i, QueueMode.STREAM, "g", 4, "", QueueOptions.defaults(), 0, "idle-" + i + ":" + "g"), message -> { });
                consumer.subscribe(new QueueSubscription("notice", QueueMode.BROADCAST, "online", 1, "", QueueOptions.defaults(), 0, "notice" + ":" + "online"), message -> broadcast.incrementAndGet());
                consumer.broadcast("notice", "notification");
                consumer.subscribe(new QueueSubscription("later", QueueMode.STREAM, "g", 1, "", QueueOptions.defaults(), 0, "later" + ":" + "g"), message -> received.add(message.id()));
                List<Message> envelopes = new CopyOnWriteArrayList<>();
                consumer.subscribe(new QueueSubscription("envelopes", QueueMode.STREAM, "g", 1, "", QueueOptions.defaults(), 0, "envelopes" + ":" + "g"), envelopes::add);
                consumer.sendMessage("envelopes", new Message("explicit-id", json.write("body"), Instant.now().toString(),
                        1, "business-key", null, Map.of("trace", "local-test")));
                await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
                    assertThat(received).containsExactly(id);
                    assertThat(envelopes).singleElement().satisfies(envelope -> {
                        assertThat(envelope.key()).isEqualTo("business-key");
                        assertThat(envelope.headers()).containsEntry("trace", "local-test");
                    });
                    assertThat(broadcast).hasValue(1);
                    assertThat(consumer.broadcastDrops()).isZero();
                });
            }
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void normalRetryDoesNotWaitForCrashVisibilityTimeout() {
        var client = client();
        var keys = new KeySpace("rdt-normal-retry-it:" + UUID.randomUUID());
        var policy = policy(Duration.ofMillis(100), 4);
        AtomicInteger calls = new AtomicInteger();
        try (var queue = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(), policy, new RedissonLockTemplate(client, keys), failure -> { throw new AssertionError(failure); }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
            queue.subscribe(new QueueSubscription("work", QueueMode.STREAM, "g", 1, "", QueueOptions.defaults(), 0, "work" + ":" + "g"), message -> {
                if (calls.incrementAndGet() == 1) throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "retry in 100ms, not 30s");
            });
            queue.send("work", "retry");
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(calls).hasValue(2);
                assertThat(client.getStream(keys.key("stream", "work")).getPendingInfo("g").getTotal()).isZero();
            });
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void expiredMessageLockCannotAcknowledgeSuccessorWork() throws Exception {
        var client = client();
        var keys = new KeySpace("rdt-message-fence-it:" + UUID.randomUUID());
        var policy = new QueuePolicy(3, Duration.ofMillis(100), Duration.ofMillis(300), Duration.ofMillis(100), Duration.ofSeconds(3), false, Duration.ofMinutes(5), Duration.ZERO, 0, 1, Duration.ofSeconds(1), 4, 2, 2, 32, 262144, 10000, 67108864L, 16777216);
        var firstStarted = new CountDownLatch(1);
        var secondStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var releaseSecond = new CountDownLatch(1);
        try (var first = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(), policy, new RedissonLockTemplate(client, keys), failure -> { }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures());
             var second = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(), policy, new RedissonLockTemplate(client, keys), failure -> { }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
            first.subscribe(new QueueSubscription("work", QueueMode.STREAM, "g", 1, "", QueueOptions.defaults(), 0, "work" + ":" + "g"), message -> {
                firstStarted.countDown();
                releaseFirst.await();
            });
            first.send("work", "fenced");
            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            var stream = client.getStream(keys.key("stream", "work"), StringCodec.INSTANCE);
            var id = stream.pendingRange("g", StreamMessageId.MIN, StreamMessageId.MAX, 1).keySet().iterator().next();
            String lock = keys.key("lock", "message:" + RdtSupport.component("work") + ":" + RdtSupport.component("g") + ":" + id);
            client.getBucket(lock).delete();
            second.subscribe(new QueueSubscription("work", QueueMode.STREAM, "g", 1, "", QueueOptions.defaults(), 0, "work" + ":" + "g"), message -> {
                secondStarted.countDown();
                releaseSecond.await();
            });
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
            releaseFirst.countDown();
            await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(stream.getPendingInfo("g").getTotal()).isEqualTo(1));
            releaseSecond.countDown();
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(stream.getPendingInfo("g").getTotal()).isZero());
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void managementPagesBoundResourceAndHistoryReads() {
        var client = client();
        var keys = new KeySpace("rdt-pages-it:" + UUID.randomUUID());
        var json = new JsonCodec();
        try {
            var index = new ManagementIndex(client, keys);
            var queries = new RedissonManagementQueryService(client, keys, json);
            for (int i = 0; i < 12; i++) {
                String id = "job-" + String.format("%02d", i);
                client.<String, String>getMap(keys.key("schedule-state", id), StringCodec.INSTANCE)
                        .putAll(Map.of("definition", "test", "next", "1"));
                index.register("tasks", id);
            }
            var page = queries.tasks(5, 5);
            assertThat(page.total()).isEqualTo(12);
            assertThat(page.items()).extracting(task -> task.taskId())
                    .containsExactly("job-05", "job-06", "job-07", "job-08", "job-09");
            for (int i = 0; i < 20; i++) {
                String id = "attempt-" + i;
                var result = new TaskResult("job-00", "now", "now", "now", "worker", true, "ok", "test-run", "test-execution", 1, (true) ? "SUCCEEDED" : "FAILED");
                client.<String, String>getMap(keys.key("schedule-results", "job-00"), StringCodec.INSTANCE)
                        .fastPut(id, json.write(result));
                index.record("attempts:job-00", id, System.currentTimeMillis(), Duration.ofMinutes(1));
            }
            assertThat(queries.taskResults("job-00", 0, 3)).hasSize(3);
            var store = new RedisFailedMessageStore(client, keys, json);
            for (int i = 0; i < 9; i++) store.save(FailedMessage.stream("work", "g",
                    Message.create("payload", json), 3, "failed", Instant.now().toString()));
            assertThat(queries.deadLetters(0, 4).items()).hasSize(4);
            assertThat(queries.deadLetters(4, 4).total()).isEqualTo(9);
            assertThat(queries.overview().taskCount()).isEqualTo(12);
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "rdt.it.benchmark", matches = "true")
    void recordsIdleAndBusyResourceMeasurements() throws Exception {
        var client = client();
        JsonCodec json = new JsonCodec();
        List<Map<String, Object>> measurements = new ArrayList<>();
        var node = client.getRedisNodes(RedisNodes.SINGLE).getInstance();
        try {
            for (int batch : List.of(1, 4, 8)) {
                for (Duration poll : batch == 1 ? List.of(Duration.ofMillis(50), Duration.ofSeconds(2)) : List.of(Duration.ofSeconds(2))) {
                    KeySpace keys = new KeySpace("rdt-bench:" + UUID.randomUUID());
                    AtomicInteger active = new AtomicInteger();
                    AtomicInteger maximum = new AtomicInteger();
                    AtomicInteger completed = new AtomicInteger();
                    List<Long> latencies = new CopyOnWriteArrayList<>();
                    try (var queue = new RedissonMessageQueueTemplate(client, keys, json, policy(poll, batch), new RedissonLockTemplate(client, keys), failure -> { throw new AssertionError(failure); }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
                        for (int topic = 0; topic < 3; topic++) queue.subscribe(new QueueSubscription("load-" + topic, QueueMode.STREAM, "g", 4, "", QueueOptions.defaults(), 0, "load-" + topic + ":" + "g"), message -> {
                            maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                            try {
                                latencies.add(System.currentTimeMillis() - Instant.parse(message.createdAt()).toEpochMilli());
                                Thread.sleep(2);
                                completed.incrementAndGet();
                            } finally { active.decrementAndGet(); }
                        });
                        Thread.sleep(500);
                        var before = node.info(RedisNode.InfoSection.ALL);
                        long jvmBefore = ProcessHandle.current().info().totalCpuDuration().orElse(Duration.ZERO).toNanos();
                        long idleStarted = System.nanoTime();
                        Thread.sleep(6000);
                        var idle = node.info(RedisNode.InfoSection.ALL);
                        long idleCpu = ProcessHandle.current().info().totalCpuDuration().orElse(Duration.ZERO).toNanos() - jvmBefore;
                        double idleSeconds = (System.nanoTime() - idleStarted) / 1e9;
                        long busyStarted = System.nanoTime();
                        String payload = "x".repeat(1024);
                        for (int message = 0; message < 600; message++) queue.send("load-" + (message % 3), payload);
                        await().atMost(Duration.ofSeconds(30)).until(() -> completed.get() == 600);
                        double seconds = (System.nanoTime() - busyStarted) / 1e9;
                        var busy = node.info(RedisNode.InfoSection.ALL);
                        List<Long> sorted = latencies.stream().sorted().toList();
                        assertThat(maximum.get()).isLessThanOrEqualTo(8);
                        var row = new java.util.LinkedHashMap<String, Object>();
                        row.put("pollMillis", poll.toMillis()); row.put("batchSize", batch);
                        row.put("idleCommandsPerSecond", delta(idle, before, "total_commands_processed") / idleSeconds);
                        row.put("idleBytesOutPerSecond", delta(idle, before, "total_net_output_bytes") / idleSeconds);
                        row.put("idleJvmCpuMillis", idleCpu / 1000000);
                        row.put("busySeconds", seconds); row.put("messagesPerSecond", 600 / seconds);
                        row.put("latencyP50Millis", sorted.get(300)); row.put("latencyP95Millis", sorted.get(570));
                        row.put("busyCommands", delta(busy, idle, "total_commands_processed"));
                        row.put("busyBytesOut", delta(busy, idle, "total_net_output_bytes"));
                        row.put("redisCpuSeconds", delta(busy, idle, "used_cpu_sys") + delta(busy, idle, "used_cpu_user"));
                        row.put("redisClients", busy.get("connected_clients")); row.put("maxActiveHandlers", maximum.get());
                        row.put("jvmHeapUsed", java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                        measurements.add(row);
                    } finally { client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*"); }
                }
            }
            Files.writeString(Path.of("target/queue-benchmark.json"), json.write(Map.of(
                    "date", Instant.now().toString(), "jdk", System.getProperty("java.version"),
                    "os", System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                    "processors", Runtime.getRuntime().availableProcessors(), "redis", node.info(RedisNode.InfoSection.SERVER),
                    "persistence", node.info(RedisNode.InfoSection.PERSISTENCE), "measurements", measurements)));
        } finally { client.shutdown(); }
    }
    private double delta(Map<String, String> after, Map<String, String> before, String key) {
        return Double.parseDouble(after.get(key)) - Double.parseDouble(before.get(key));
    }

    @Test
    void fullLocalBufferStopsFetchingAndShutdownLeavesPrefetchRecoverable() throws Exception {
        var client = client();
        var keys = new KeySpace("rdt-buffer-it:" + UUID.randomUUID());
        var json = new JsonCodec();
        var policy = new QueuePolicy(3, Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofSeconds(2), true, Duration.ofMinutes(1), Duration.ZERO, 0, 1, Duration.ofSeconds(1), 1, 1, 1, 2, 262144, 10000, 67108864L, 16777216);
        var started = new CountDownLatch(1);
        var blocked = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try {
            try (var queue = new RedissonMessageQueueTemplate(client, keys, json, policy,
                    new RedissonLockTemplate(client, keys), failure -> { }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
                queue.subscribe(QueueSubscription.concurrent("slow", QueueMode.STREAM, "g", 8), message -> {
                    started.countDown();
                    blocked.await();
                });
                for (int i = 0; i < 8; i++) queue.send("slow", i);
                assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
                var stream = client.<String, String>getStream(keys.key("stream", "slow"), StringCodec.INSTANCE);
                await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(queue.bufferedMessages()).isEqualTo(2));
                await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                        assertThat(stream.getPendingInfo("g").getTotal()).isEqualTo(3));
            }
            try (var recovery = new RedissonMessageQueueTemplate(client, keys, json, policy,
                    new RedissonLockTemplate(client, keys), failure -> { }, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures())) {
                recovery.subscribe(QueueSubscription.concurrent("slow", QueueMode.STREAM, "g", 1), message -> calls.incrementAndGet());
                await().atMost(Duration.ofSeconds(6)).untilAsserted(() -> assertThat(calls).hasValue(8));
            }
        } finally {
            blocked.countDown();
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void pressurePausesRedisFetchAndResumesWithoutRevokingOwnership() {
        var client = client();
        var keys = new KeySpace("rdt-pressure-it:" + UUID.randomUUID());
        var identity = leasedIdentity(client, keys, "pressure-worker");
        var pressure = new AdaptiveAdmissionController(.85, .65, .9, .7, Duration.ZERO);
        identity.admission(pressure);
        pressure.refresh(.1, 1, System.currentTimeMillis());
        var calls = new AtomicInteger();
        try (var queue = new RedissonMessageQueueTemplate(client, keys, new JsonCodec(), policy(Duration.ofMillis(100), 1),
                new RedissonLockTemplate(client, keys), failure -> { }, identity, RetryClassifier.infrastructureFailures())) {
            queue.subscribe(QueueSubscription.concurrent("work", QueueMode.STREAM, "g", 1), message -> calls.incrementAndGet());
            queue.send("work", "deferred");
            await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(calls).hasValue(0);
                assertThat(client.getStream(keys.key("stream", "work"), StringCodec.INSTANCE).getPendingInfo("g").getTotal()).isZero();
                assertThat(identity.guard().getAsBoolean()).isTrue();
            });
            pressure.refresh(.1, .1, System.currentTimeMillis());
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(calls).hasValue(1));
        } finally {
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    private RuntimeIdentity leasedIdentity(RedissonClient client, KeySpace keys, String id) {
        var identity = new RuntimeIdentity(id);
        var leases = new RedisOwnerLeaseService(client, keys, new RedissonLockTemplate(client, keys));
        var lease = leases.acquire("node:" + identity.instanceId(), identity.instanceId(), Duration.ofMinutes(5)).orElseThrow();
        identity.acquired(lease.token(), lease.fencingVersion(), () -> leases.isHeld(lease));
        return identity;
    }
}
