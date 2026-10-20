package io.github.rdt.console.archive;

import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.Message;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfSystemProperty(named = "rdt.it.cluster", matches = ".+")
class RedisArchiveClusterIT {
    @Test
    void taggedMapCacheExpiryScriptsAndDiscoveryWorkOnThreeMasterCluster() {
        RedissonClient client = client();
        KeySpace first = new KeySpace("archive-cluster-it:" + UUID.randomUUID());
        KeySpace second = new KeySpace(first.namespace() + ":second");
        JsonCodec json = new JsonCodec();
        Instant now = Instant.now();
        try {
            for (KeySpace keys : List.of(first, second)) {
                var failure = FailedMessage.stream("topic", "group", Message.create(Map.of("id", "test"), json),
                        3, "downstream", now.toString());
                var cache = client.<String, String>getMapCache(keys.key("dead-letters", "topic"), StringCodec.INSTANCE);
                cache.fastPut("live", json.write(failure), 1, TimeUnit.DAYS, 1, TimeUnit.DAYS);
                cache.fastPut("expired", json.write(failure), 1, TimeUnit.DAYS);
                client.getScoredSortedSet("redisson__timeout__set:" + keys.key("dead-letters", "topic"), StringCodec.INSTANCE)
                        .add(1, "expired");
                var execution = new JobExecution("failed", "job", now.toEpochMilli(), 1, "FAILED",
                        "worker", "run", "token", now.toEpochMilli(), 0, "failure", 0);
                client.<String, String>getMap(keys.key("schedule-executions", "job"), StringCodec.INSTANCE)
                        .fastPut("failed", json.write(execution));
                client.<String>getScoredSortedSet(keys.key("schedule-execution-expiry", "job"), StringCodec.INSTANCE)
                        .add(System.currentTimeMillis() + Duration.ofDays(1).toMillis(), "failed");
                var stuck = new JobExecution("stuck", "job", now.toEpochMilli(), 1, "DISPATCHED",
                        "worker", "run", "token", now.toEpochMilli() - 600001, 0, "stuck", 0);
                client.<String, String>getMap(keys.key("schedule-state", "job"), StringCodec.INSTANCE)
                        .put("execution", json.write(stuck));
            }
            var records = new ArrayList<Map<String, Object>>();
            long count = new RedisArchiveReader(client, json, List.of(first.namespace(), second.namespace()), Duration.ofMinutes(10))
                    .read(now, batch -> batch.forEach(value -> records.add(json.read(value, Map.class))));
            assertThat(count).isEqualTo(6);
            assertThat(records.stream().map(record -> record.get("kind")))
                    .containsExactlyInAnyOrder("DEAD_LETTER", "FAILED_EXECUTION", "STALLED_EXECUTION",
                            "DEAD_LETTER", "FAILED_EXECUTION", "STALLED_EXECUTION");
            assertThat(records.stream().map(record -> record.get("namespace")))
                    .containsOnly(first.namespace(), second.namespace());
            assertThat(new RedisArchiveReader(client, json, List.of(first.namespace()), Duration.ofMinutes(10))
                    .read(now, batch -> { })).isEqualTo(3);
        } finally {
            try {
                for (KeySpace keys : List.of(first, second)) {
                    client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
                }
            } finally { client.shutdown(); }
        }
    }

    private RedissonClient client() {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        config.useClusterServers().addNodeAddress(System.getProperty("rdt.it.cluster").split(","))
                .setReadMode(ReadMode.MASTER).setMasterConnectionMinimumIdleSize(1).setMasterConnectionPoolSize(4)
                .setSlaveConnectionMinimumIdleSize(1).setSlaveConnectionPoolSize(2)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(1);
        return Redisson.create(config);
    }
}
