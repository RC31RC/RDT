package io.github.rdt.spring;

import io.github.rdt.support.RetryClassifier;
import io.github.rdt.runtime.RuntimeIdentity;

import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** These assertions use two independent JVMs; two Redisson objects do not count as failover validation. */
@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class RuntimeProcessIT {
    @Test
    void routesByCapabilityOrdersRetriesAndRecoversAfterProcessDeath() throws Exception {
        KeySpace keys = new KeySpace("rdt-process-it:" + UUID.randomUUID());
        String address = System.getProperty("rdt.it.redis");
        Config config = new Config();
        config.setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(address).setConnectionMinimumIdleSize(1).setConnectionPoolSize(8);
        var client = Redisson.create(config);
        var processes = new ArrayList<Process>();
        try {
            for (String instance : List.of("worker-a", "worker-b")) {
                Path log = Files.createTempFile("rdt-" + instance, ".log");
                processes.add(new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Xmx192m", "-cp", System.getProperty("java.class.path"), RuntimeWorkerProcess.class.getName(),
                        address, keys.namespace(), instance).redirectErrorStream(true).redirectOutput(log.toFile()).start());
            }
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(client.getBucket(keys.key("test-ready", "worker-a"), StringCodec.INSTANCE).get()).isNotNull();
                assertThat(client.getBucket(keys.key("test-ready", "worker-b"), StringCodec.INSTANCE).get()).isNotNull();
                assertThat(processes).allMatch(Process::isAlive);
            });
            JsonCodec json = new JsonCodec();
            var locks = new RedissonLockTemplate(client, keys);
            var commands = new RedissonManagementCommandService(client, keys, locks, new JsonCodec());
            var queries = new RedissonManagementQueryService(client, keys, json);
            await().atMost(Duration.ofSeconds(6)).untilAsserted(() -> assertThat(queries.instances()).hasSize(2));
            Path duplicateLog = Files.createTempFile("rdt-duplicate-identity", ".log");
            Process duplicate = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx192m", "-cp", System.getProperty("java.class.path"), RuntimeWorkerProcess.class.getName(),
                    address, keys.namespace(), "worker-a").redirectErrorStream(true).redirectOutput(duplicateLog.toFile()).start();
            processes.add(duplicate);
            assertThat(duplicate.waitFor(15, TimeUnit.SECONDS)).isTrue();
            assertThat(duplicate.exitValue()).isNotZero();
            assertThat(Files.readString(duplicateLog)).contains("already running");
            assertThat(queries.instances()).hasSize(2);
            assertThat(json.write(queries.instances())).doesNotContain("leaseToken");
            assertThat(commands.triggerTask("routed-job")).isTrue();
            await().atMost(Duration.ofSeconds(12)).untilAsserted(() ->
                    assertThat(client.getList(keys.key("test-results", "routed"), StringCodec.INSTANCE).readAll())
                            .singleElement().asString().startsWith("worker-b:"));
            var policy = new QueuePolicy(3, Duration.ofMillis(600), Duration.ofMillis(500), Duration.ofMillis(250), Duration.ofSeconds(2), false, Duration.ofMinutes(5), Duration.ZERO, 0, 1, Duration.ofSeconds(3), 16, 4, 8, 32, 262144, 10000, 67108864L, 16777216);
            try (var producer = new RedissonMessageQueueTemplate(client, keys, json, policy, locks, failure -> { }, new RuntimeIdentity(null), RetryClassifier.infrastructureFailures())) {
                var sentIds = new HashSet<String>();
                for (int i = 0; i < 24; i++) sentIds.add(producer.send("competing", "message-" + i));
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(client.getList(keys.key("test-results", "competing"), StringCodec.INSTANCE).readAll())
                                .containsExactlyInAnyOrderElementsOf(sentIds));
                producer.sendOrdered("ordered", "A", "A1", 2);
                producer.sendOrdered("ordered", "A", "A2", 2);
                producer.sendOrdered("ordered", "B-key", "B1", 2);
                producer.reliableBroadcast("reliable-broadcast", "hello");
                await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
                    var events = client.getList(keys.key("test-results", "ordered"), StringCodec.INSTANCE).readAll();
                    assertThat(events).contains("A1-failed", "A1", "A2", "B1");
                    assertThat(events.indexOf("A2")).isGreaterThan(events.indexOf("A1"));
                    assertThat(events.indexOf("B1")).isLessThan(events.indexOf("A2"));
                    assertThat(client.getList(keys.key("test-results", "broadcast"), StringCodec.INSTANCE).readAll())
                            .containsExactlyInAnyOrder("worker-a", "worker-b");
                });
            }
            String pendingId;
            try (var producer = new RedissonMessageQueueTemplate(client, keys, json, policy, locks, failure -> { }, new RuntimeIdentity(null), RetryClassifier.infrastructureFailures())) {
                pendingId = producer.send("crash-pending", "recover-me");
            }
            await().atMost(Duration.ofSeconds(12)).until(() ->
                    client.getAtomicLong(keys.key("test-count", "pending-crash")).get() == 1);
            String queueOwner = client.<String>getBucket(keys.key("test-owner", "pending-crash"), StringCodec.INSTANCE).get();
            int queueOwnerIndex = queueOwner.equals("worker-a") ? 0 : 1;
            Process queueKilled = processes.get(queueOwnerIndex);
            queueKilled.destroyForcibly();
            assertThat(queueKilled.waitFor(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(client.getList(keys.key("test-results", "pending-crash"), StringCodec.INSTANCE).readAll())
                        .containsExactly(pendingId);
                assertThat(client.getAtomicLong(keys.key("test-count", "pending-crash")).get()).isEqualTo(2);
            });
            await().atMost(Duration.ofSeconds(10)).until(() -> !client.getBucket(keys.key("owner", "node:" + queueOwner)).isExists());
            processes.set(queueOwnerIndex, startWorker(address, keys, queueOwner));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(queries.instances()).hasSize(2);
                assertThat(processes.get(queueOwnerIndex).isAlive()).isTrue();
            });
            assertThat(commands.triggerTask("crash-job")).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> client.getAtomicLong(keys.key("test-count", "job-crash")).get() == 1);
            String owner = client.<String>getBucket(keys.key("test-owner", "job-crash"), StringCodec.INSTANCE).get();
            Process killed = processes.get(owner.equals("worker-a") ? 0 : 1);
            killed.destroyForcibly();
            assertThat(killed.waitFor(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var state = client.<String, String>getMap(keys.key("schedule-state", "crash-job"), StringCodec.INSTANCE);
                JobExecution result = json.read(state.get("execution"), JobExecution.class);
                assertThat(result.status()).isEqualTo("SUCCEEDED");
                assertThat(result.instanceId()).isNotEqualTo(owner);
                assertThat(result.attempt()).isEqualTo(2);
            });
            assertThat(client.getAtomicLong(keys.key("test-count", "job-crash")).get()).isEqualTo(2);
        } finally {
            for (Process process : processes) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            client.shutdown();
        }
    }
    private Process startWorker(String address, KeySpace keys, String instance) throws Exception {
        Path log = Files.createTempFile("rdt-restarted-" + instance, ".log");
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx192m", "-cp", System.getProperty("java.class.path"), RuntimeWorkerProcess.class.getName(),
                address, keys.namespace(), instance).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

}
