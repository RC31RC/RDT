package io.github.rdt.redisson.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisNodes;
import org.redisson.api.redisnode.RedisNode.InfoSection;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.UUID;

@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class StreamMaintenanceIT {
    private QueuePolicy policy() {
        return new QueuePolicy(
                1,
                Duration.ZERO,
                Duration.ofSeconds(1),
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                true,
                Duration.ofMinutes(5),
                Duration.ZERO,
                0,
                1,
                Duration.ofSeconds(1),
                2,
                2,
                2,
                2, 262144, 10000, 67108864L, 16777216);
    }

    @Test
    void cleanupWaitsForOfflineGroupsAndPendingThenCleansIdleTail() {
        var config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer()
                .setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1)
                .setConnectionPoolSize(4);
        var client = Redisson.create(config);
        var keys = new KeySpace("maintenance-it:" + UUID.randomUUID());
        try {
            var stream =
                    client.<String, String>getStream(
                            keys.key("stream", "events"), StringCodec.INSTANCE);
            stream.createGroup(
                    StreamCreateGroupArgs.name("online").id(StreamMessageId.ALL).makeStream());
            stream.createGroup(StreamCreateGroupArgs.name("offline").id(StreamMessageId.ALL));
            var id = stream.add(StreamAddArgs.entries(Map.of("payload", "one")));
            stream.readGroup("online", "node", StreamReadGroupArgs.neverDelivered().count(1));
            stream.ack("online", id);
            var retention = new StreamRetention(policy(), client, keys);
            retention.markAcknowledged(policy(), stream);
            retention.sweepAcknowledged();
            assertThat(stream.size()).isEqualTo(1);
            stream.readGroup("offline", "late-node", StreamReadGroupArgs.neverDelivered().count(1));
            // Still pending in one group: a delivered cursor alone must never authorize deletion.
            await().pollDelay(Duration.ofMillis(1100))
                    .atMost(Duration.ofSeconds(3))
                    .untilAsserted(
                            () -> {
                                retention.sweepAcknowledged();
                                assertThat(stream.size()).isEqualTo(1);
                            });
            stream.ack("offline", id);
            retention.markAcknowledged(policy(), stream);
            await().atMost(Duration.ofSeconds(3))
                    .untilAsserted(
                            () -> {
                                retention.sweepAcknowledged();
                                assertThat(stream.size()).isZero();
                            });
        } finally {
            client.getKeys()
                    .deleteByPattern("rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void cleanupNeverRoundsAdjacentLargeSequenceIdsIntoTheDeliveredBoundary() {
        var config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer()
                .setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1)
                .setConnectionPoolSize(4);
        var client = Redisson.create(config);
        var keys = new KeySpace("maintenance-precision-it:" + UUID.randomUUID());
        try {
            var stream =
                    client.<String, String>getStream(
                            keys.key("stream", "events"), StringCodec.INSTANCE);
            stream.createGroup(
                    StreamCreateGroupArgs.name("workers").id(StreamMessageId.ALL).makeStream());
            var delivered = new StreamMessageId(1, 9007199254740992L);
            var unread = new StreamMessageId(1, 9007199254740993L);
            stream.add(delivered, StreamAddArgs.entry("payload", "delivered"));
            stream.add(unread, StreamAddArgs.entry("payload", "unread"));
            stream.readGroup("workers", "node", StreamReadGroupArgs.neverDelivered().count(1));
            stream.ack("workers", delivered);
            var retention = new StreamRetention(policy(), client, keys);

            retention.markAcknowledged(policy(), stream);
            retention.sweepAcknowledged();

            assertThat(stream.range(StreamMessageId.MIN, StreamMessageId.MAX))
                    .containsOnlyKeys(unread);
        } finally {
            client.getKeys()
                    .deleteByPattern("rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void onlyOneRecoveryScannerLeadsAndCloseAllowsImmediateTakeover() {
        var config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer()
                .setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1)
                .setConnectionPoolSize(4);
        var client = Redisson.create(config);
        var keys = new KeySpace("recovery-it:" + UUID.randomUUID());
        var locks = new RedissonLockTemplate(client, keys);
        var spec = QueueSubscription.concurrent("events", QueueMode.STREAM, "workers", 1);
        try (var first =
                        new PendingRecoveryCoordinator(
                                client, keys, locks, spec, new RuntimeIdentity("a"), policy());
                var second =
                        new PendingRecoveryCoordinator(
                                client, keys, locks, spec, new RuntimeIdentity("b"), policy())) {
            assertThat(first.isLeader()).isTrue();
            assertThat(second.isLeader()).isFalse();
            first.close();
            await().atMost(Duration.ofSeconds(3))
                    .untilAsserted(() -> assertThat(second.isLeader()).isTrue());
        } finally {
            client.getKeys()
                    .deleteByPattern("rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }

    @Test
    void acknowledgementsNeverQueryGroupsAndSweepsGroupsOncePerBoundedBatch() {
        var config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(4);
        var client = Redisson.create(config);
        var keys = new KeySpace("maintenance-cost-it:" + UUID.randomUUID());
        String name = keys.key("stream", "events");
        try {
            var stream = client.<String, String>getStream(name, StringCodec.INSTANCE);
            stream.createGroup(StreamCreateGroupArgs.name("online").id(StreamMessageId.ALL).makeStream());
            stream.createGroup(StreamCreateGroupArgs.name("offline").id(StreamMessageId.ALL));
            for (int i = 0; i < 600; i++) stream.add(StreamAddArgs.entry("payload", "message"));
            for (String group : List.of("online", "offline")) {
                var delivered = stream.readGroup(group, "node", StreamReadGroupArgs.neverDelivered().count(600));
                assertThat(delivered).hasSize(600);
                stream.ack(group, delivered.keySet().toArray(StreamMessageId[]::new));
            }
            var retention = new StreamRetention(policy(), client, keys);
            var before = stats(client);

            for (int i = 0; i < 600; i++) retention.markAcknowledged(policy(), stream);

            var afterMarks = stats(client);
            assertThat(calls(afterMarks, "xinfo|groups")).isEqualTo(calls(before, "xinfo|groups"));
            assertThat(calls(afterMarks, "xpending")).isEqualTo(calls(before, "xpending"));
            assertThat(stream.size()).isEqualTo(600);

            retention.sweepAcknowledged();

            var afterSweep = stats(client);
            assertThat(calls(afterSweep, "xinfo|groups") - calls(afterMarks, "xinfo|groups")).isEqualTo(10);
            assertThat(calls(afterSweep, "xpending") - calls(afterMarks, "xpending")).isEqualTo(20);
            assertThat(stream.size()).isZero();
        } finally {
            client.getKeys().unlink(name, keys.key("stream-maintenance", name));
            client.shutdown();
        }
    }

    private Map<String, String> stats(RedissonClient client) {
        return client.getRedisNodes(RedisNodes.SINGLE).getInstance().info(InfoSection.COMMANDSTATS);
    }

    private long calls(Map<String, String> stats, String command) {
        return Long.parseLong(stats.getOrDefault("cmdstat_" + command, "calls=0").substring(6).split(",", 2)[0]);
    }
}
