package io.github.rdt.redisson.queue;

import io.github.rdt.exception.RdtQueueException;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryClassifier;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RScript;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class QueueCapacityAndCleanupIT {
    private final KeySpace keys = new KeySpace("capacity-cleanup:" + UUID.randomUUID());
    private final JsonCodec json = new JsonCodec();
    private RedissonClient client;

    static QueuePolicy policy(int count, long bytes) {
        return new QueuePolicy(3, Duration.ofMillis(10), Duration.ofSeconds(30), Duration.ofMillis(100),
                Duration.ofSeconds(2), true, Duration.ofMinutes(1), Duration.ofDays(7), 10000,
                1, Duration.ofSeconds(1), 4, 2, 8, 8, 4096, count, bytes, 32768);
    }

    @BeforeEach
    void connect() {
        var config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(8);
        client = Redisson.create(config);
    }

    @AfterEach
    void close() {
        client.getKeys().deleteByPattern("rdt:" + RdtSupport.component(keys.namespace()) + ":*");
        client.shutdown();
    }

    private RuntimeIdentity identity() {
        var identity = new RuntimeIdentity("test-worker");
        identity.acquired("test-token", 1, () -> true);
        return identity;
    }

    private RStream<String, String> stream(String destination) {
        return client.getStream(keys.key("stream", destination), StringCodec.INSTANCE);
    }

    @Test
    void oneSweepReclaims1024AckedMessagesAndSkipsOldestPendingPrecisely() {
        var target = stream("events");
        target.createGroup(StreamCreateGroupArgs.name("g").id(StreamMessageId.ALL).makeStream());
        for (int i = 0; i < 1024; i++) target.add(StreamAddArgs.entry("message", "payload"));
        var all = target.readGroup("g", "crashed", StreamReadGroupArgs.neverDelivered().count(1024));
        target.ack("g", all.keySet().toArray(StreamMessageId[]::new));
        var retention = new StreamRetention(policy(2048, 1048576), client, keys);
        retention.markAcknowledged(policy(2048, 1048576), target);
        retention.sweepAcknowledged();
        assertThat(target.size()).isZero();

        for (int i = 0; i < 512; i++) target.add(StreamAddArgs.entry("message", "payload"));
        var ids = new ArrayList<>(target.readGroup("g", "crashed", StreamReadGroupArgs.neverDelivered().count(512)).keySet());
        target.ack("g", ids.subList(1, ids.size()).toArray(StreamMessageId[]::new));
        retention.markAcknowledged(policy(2048, 1048576), target);
        retention.sweepAcknowledged();
        assertThat(target.size()).isEqualTo(1);
        assertThat(target.range(StreamMessageId.MIN, StreamMessageId.MAX)).containsOnlyKeys(ids.getFirst());
    }

    @Test
    void cleanupWithoutGroupsPreservesMessagesAndWaitsForSubscriptionAck() {
        var target = stream("events");
        for (int i = 0; i < 3; i++) target.add(StreamAddArgs.entry("message", "payload"));
        var retention = new StreamRetention(policy(10, 8192), client, keys);

        retention.markAcknowledged(policy(10, 8192), target);
        retention.sweepAcknowledged();
        assertThat(target.size()).isEqualTo(3);

        target.createGroup(StreamCreateGroupArgs.name("g").id(StreamMessageId.ALL));
        var delivered = target.readGroup("g", "worker", StreamReadGroupArgs.neverDelivered().count(3));
        target.ack("g", delivered.keySet().toArray(StreamMessageId[]::new));
        retention.markAcknowledged(policy(10, 8192), target);
        retention.sweepAcknowledged();

        assertThat(target.size()).isZero();
    }

    @Test
    void delayedAndStreamMessagesShareAtomicCapacityAndAckReturnsCredits() {
        var policy = policy(2, 8192);
        var admission = new QueueAdmission(client, keys, policy);
        String encoded = json.write(Message.create("work", json));
        admission.accept("events", encoded, null);
        admission.accept("events", encoded + " ", System.currentTimeMillis() + 60000);
        assertThatThrownBy(() -> admission.accept("events", encoded, null)).isInstanceOf(RdtQueueException.class);
        var target = stream("events");
        target.createGroup(StreamCreateGroupArgs.name("g").id(StreamMessageId.ALL));
        var id = target.readGroup("g", "worker", StreamReadGroupArgs.neverDelivered().count(1)).keySet().iterator().next();
        var retention = new StreamRetention(policy, client, keys);
        retention.acknowledge(policy, target, "g", id.toString(), List.of(keys.key("message-retries", "events:g")));
        retention.sweepAcknowledged();
        assertThat(client.getAtomicLong(keys.key("queue-capacity", "events")).get())
                .isEqualTo((encoded + " ").getBytes(StandardCharsets.UTF_8).length);
        admission.accept("events", encoded, null);
        assertThat(target.size()).isEqualTo(1);
    }

    @Test
    void multiGroupAckDefersPendingChecksToOneBoundedSweep() {
        var policy = policy(10, 8192);
        var admission = new QueueAdmission(client, keys, policy);
        String encoded = json.write(Message.create("work", json));
        admission.accept("events", encoded, null);
        var target = stream("events");
        target.createGroup(StreamCreateGroupArgs.name("a").id(StreamMessageId.ALL));
        target.createGroup(StreamCreateGroupArgs.name("b").id(StreamMessageId.ALL));
        var id = target.readGroup("a", "worker-a", StreamReadGroupArgs.neverDelivered().count(1))
                .keySet().iterator().next();
        target.readGroup("b", "worker-b", StreamReadGroupArgs.neverDelivered().count(1));
        var retention = new StreamRetention(policy, client, keys);

        retention.acknowledge(policy, target, "a", id.toString(), List.of(keys.key("message-retries", "events:a")));
        assertThat(target.size()).isEqualTo(1);
        assertThat(client.getAtomicLong(keys.key("queue-capacity", "events")).get())
                .isEqualTo(encoded.getBytes(StandardCharsets.UTF_8).length);
        retention.acknowledge(policy, target, "b", id.toString(), List.of(keys.key("message-retries", "events:b")));
        retention.sweepAcknowledged();

        assertThat(target.size()).isZero();
        assertThat(client.getAtomicLong(keys.key("queue-capacity", "events")).get()).isZero();
    }

    @Test
    void capacityBytesReconcileAfterOutOfBandPartialDeletion() {
        var admission = new QueueAdmission(client, keys, policy(10, 8192));
        String first = "stream-envelope";
        String removed = "delayed-envelope";
        String next = "next-envelope";
        admission.accept("events", first, null);
        admission.accept("events", removed, System.currentTimeMillis() + 60000);
        client.<String>getScoredSortedSet(keys.key("message-delay", "events"), StringCodec.INSTANCE)
                .remove(removed);

        admission.accept("events", next, null);

        assertThat(client.getAtomicLong(keys.key("queue-capacity", "events")).get())
                .isEqualTo(first.length() + next.length());
    }

    @Test
    void byteLimitAndMessageSizeRejectWithoutPartialWrites() {
        var admission = new QueueAdmission(client, keys, policy(100, 4096));
        admission.accept("events", "x".repeat(3000), null);
        assertThatThrownBy(() -> admission.accept("events", "y".repeat(2000), null)).isInstanceOf(RdtQueueException.class);
        assertThatThrownBy(() -> admission.accept("events", "z".repeat(4097), null)).isInstanceOf(RdtQueueException.class);
        assertThat(stream("events").size()).isEqualTo(1);
        assertThat(client.getAtomicLong(keys.key("queue-capacity", "events")).get()).isEqualTo(3000);
    }

    @Test
    void concurrentProducersCannotOversubscribe() {
        var admission = new QueueAdmission(client, keys, policy(32, 8192));
        var accepted = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 128; i++) executor.submit(() -> {
                try { admission.accept("events", "message", null); accepted.incrementAndGet(); }
                catch (RdtQueueException full) { /* Expected admission rejection. */ }
            });
        }
        assertThat(accepted).hasValue(32);
        assertThat(stream("events").size()).isEqualTo(32);
    }

    @Test
    void successfulMessagesLeaveNoPerMessageDiagnosticsAndRecoveryDrainsContinuously() {
        var target = stream("events");
        target.createGroup(StreamCreateGroupArgs.name("g").id(StreamMessageId.ALL).makeStream());
        for (int i = 0; i < 64; i++) target.add(StreamAddArgs.entry("message", json.write(Message.create("work", json))));
        var pending = target.readGroup("g", "crashed", StreamReadGroupArgs.neverDelivered().count(64));
        // Old Pending is immediately eligible with the default 30s visibility; no wall-clock sleep.
        client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                for i = 1, #ARGV do
                  redis.call('XCLAIM', KEYS[1], 'g', 'crashed', 0, ARGV[i], 'IDLE', 60000, 'JUSTID')
                end
                return 1
                """, RScript.ReturnType.LONG, List.of(target.getName()),
                pending.keySet().stream().map(StreamMessageId::toString).toArray());
        var handled = new AtomicInteger();
        try (var queue = new RedissonMessageQueueTemplate(client, keys, json, policy(1000, 1048576),
                new RedissonLockTemplate(client, keys), failure -> { throw new AssertionError(failure); },
                identity(), RetryClassifier.infrastructureFailures())) {
            queue.subscribe(QueueSubscription.concurrent("events", QueueMode.STREAM, "g", 1), message -> handled.incrementAndGet());
            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                assertThat(handled).hasValue(64);
                assertThat(target.size()).isZero();
            });
            for (String feature : List.of("message-delivery", "message-failures", "message-retry-at", "message-first-failure"))
                assertThat(client.getKeys().getKeysByPattern(keys.pattern(feature))).isEmpty();
        }
    }

    @Test
    void orderedConsumerDoesNotSleepAfterSuccessfulMessages() {
        var handled = new AtomicInteger();
        try (var queue = new RedissonMessageQueueTemplate(client, keys, json, policy(1000, 1048576),
                new RedissonLockTemplate(client, keys), failure -> { throw new AssertionError(failure); },
                identity(), RetryClassifier.infrastructureFailures())) {
            for (int i = 0; i < 128; i++) queue.sendOrdered("ordered", "same-key", i, 1);
            queue.subscribe(QueueSubscription.ordered("ordered", "g", 1, 1), message -> handled.incrementAndGet());
            // The former mandatory 50ms per success alone required at least 6.4 seconds.
            await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> assertThat(handled).hasValue(128));
        }
    }

    @Test
    void delayedTransferDrainsMoreThan64InOneTurn() {
        var policy = policy(1000, 1048576);
        var streams = new QueueStreams(client, keys);
        var retention = new StreamRetention(policy, client, keys);
        var producer = new QueueProducer(client, keys, json, identity(), retention, streams, new AtomicBoolean(), policy);
        for (int i = 0; i < 256; i++) producer.sendDelayed("events", i, Duration.ofMillis(1));
        await().atMost(Duration.ofSeconds(1)).until(() -> client.<String>getScoredSortedSet(keys.key("message-delay", "events"), StringCodec.INSTANCE).lastScore() < System.currentTimeMillis());
        try {
            producer.transferDelayed();
            assertThat(stream("events").size()).isEqualTo(256);
        } finally {
            producer.close();
        }
    }

    @Test
    void delayedTransferLeaseAllowsOneScannerAndFastFailover() {
        var first = new DelayTransferCoordinator(client, keys, new RuntimeIdentity("first"));
        var second = new DelayTransferCoordinator(client, keys, new RuntimeIdentity("second"));
        try {
            assertThat(first.isLeader()).isTrue();
            assertThat(second.isLeader()).isFalse();
            first.close();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(second.isLeader()).isTrue());
        } finally {
            first.close();
            second.close();
        }
    }
}
