package io.github.rdt.redisson.schedule;

import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.ScheduleDefinitionChangePolicy;
import io.github.rdt.schedule.ScheduleFailureMode;
import io.github.rdt.schedule.ScheduleMisfirePolicy;
import io.github.rdt.schedule.SchedulePolicy;
import io.github.rdt.schedule.ScheduleSpec;
import io.github.rdt.schedule.SchedulerSettings;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryClassifier;
import io.github.rdt.support.RetryPolicy;
import java.time.Duration;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class ScheduleResourceBoundsIT {
    private final KeySpace keys = new KeySpace("schedule-bounds:" + UUID.randomUUID());
    private final JsonCodec json = new JsonCodec();
    private RedissonClient client;

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

    @Test
    void abandonedMailboxIsBoundedAndExpiresWithoutAWorker() {
        var mailbox = new JobMailbox(client, keys, json);
        mailbox.send("gone", "old-run", new JobInvocation("job", "one", "token"), Duration.ofMillis(200), 1);
        assertThatThrownBy(() -> mailbox.send("gone", "old-run", new JobInvocation("job", "two", "token"), Duration.ofMillis(200), 1))
                .isInstanceOf(RuntimeException.class);
        String key = keys.key("job-worker", "gone:old-run");
        assertThat(client.getList(key, StringCodec.INSTANCE).remainTimeToLive()).isPositive();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(client.getKeys().countExists(key)).isZero());
    }

    @Test
    void blockingMailboxReadWakesAsSoonAsAnInvocationArrives() throws Exception {
        var mailbox = new JobMailbox(client, keys, json);
        var invocation = new JobInvocation("job", "execution", "token");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var waiting = executor.submit(() -> mailbox.receive("worker", "run", 2));
            Thread.sleep(100);
            mailbox.send("worker", "run", invocation, Duration.ofSeconds(5), 2);
            assertThat(waiting.get(2, TimeUnit.SECONDS)).containsExactly(json.write(invocation));
        }
    }

    @Test
    void localDueIndexReadsAtMostOneBoundedCandidatePage() {
        var due = new ScheduleDueIndex(client, keys, json);
        var candidates = new ArrayList<String>();
        for (int i = 0; i < ScheduleDueIndex.LOCAL_SCAN_LIMIT + 44; i++) {
            String id = "task-" + i;
            candidates.add(id);
            due.dueIndex().add(Double.MAX_VALUE, id);
        }

        var first = due.findDue(candidates.subList(0, ScheduleDueIndex.LOCAL_SCAN_LIMIT), 1, 128);
        var second = due.findDue(candidates.subList(ScheduleDueIndex.LOCAL_SCAN_LIMIT, candidates.size()), 1, 128);

        assertThat(first.scanned()).isEqualTo(ScheduleDueIndex.LOCAL_SCAN_LIMIT);
        assertThat(first.ids()).isEmpty();
        assertThat(second.scanned()).isEqualTo(44);
        assertThat(second.ids()).isEmpty();
    }

    @Test
    void historiesBodiesAndAllIndexesAreCappedAndExpireAfterLastWrite() {
        var repository = new JobExecutionRepository(client, keys, json, Duration.ofMillis(800), 4);
        for (int i = 0; i < 10; i++) {
            repository.archive(new JobExecution("e-" + i, "job", i, 1, "SUCCEEDED", "node", "run", "token", System.currentTimeMillis()+i, 0, "", 0));
            repository.save("job", "r-" + i, Duration.ofMillis(800), new TaskResult("job", "now", "now", "now", "node", false, "failure", "run", "e-"+i, 1, "FAILED"));
        }
        assertThat(client.getMap(keys.key("schedule-executions", "job")).size()).isEqualTo(4);
        assertThat(client.getMap(keys.key("schedule-results", "job")).size()).isEqualTo(4);
        var index = new ManagementIndex(client, keys);
        assertThat(index.resources("attempts:job").size()).isEqualTo(4);
        assertThat(index.resources("failures:job").size()).isEqualTo(4);
        assertThat(repository.results("job", 0, 10)).hasSize(4);
        assertThat(new RedissonManagementQueryService(client, keys, json).executions("job", 0, 10)).hasSize(4);
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(client.getKeys().getKeysByPattern("rdt:" + RdtSupport.component(keys.namespace()) + ":*")).isEmpty());
    }

    @Test
    void queryingExpiredResultsAlsoReclaimsBodiesWhileNewerResultsRemainLive() {
        var repository = new JobExecutionRepository(client, keys, json, Duration.ofMinutes(1), 4);
        var result = new TaskResult("job", "now", "now", "now", "node", false, "failure", "run", "execution", 1, "FAILED");
        repository.save("job", "expired", Duration.ofMinutes(1), result);
        repository.save("job", "live", Duration.ofMinutes(1), result);
        var index = new ManagementIndex(client, keys);
        index.expiries("attempts:job").add(1, "expired");
        assertThat(repository.results("job", 0, 10)).hasSize(1);
        assertThat(client.<String, String>getMap(keys.key("schedule-results", "job"), StringCodec.INSTANCE).keySet())
                .containsExactly("live");
        assertThat(index.resources("failures:job").contains("expired")).isFalse();
        assertThat(index.expiries("failures:job").contains("expired")).isFalse();
    }

    @Test
    void managementIndexExpiresWithoutAnySubsequentWriterOrQuery() {
        var index = new ManagementIndex(client, keys);
        index.record("dead-letters", "id", 1, Duration.ofMillis(200));
        assertThat(index.resources("dead-letters").remainTimeToLive()).isPositive();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(index.resources("dead-letters").isExists()).isFalse();
            assertThat(index.expiries("dead-letters").isExists()).isFalse();
        });
    }

    @Test
    void unregisteredHistoricalTasksNeverConsumeActiveTaskScanBudget() {
        var due = client.<String>getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE);
        for (int i = 0; i < 1024; i++) due.add(0, "orphan-" + i);
        var policy = new SchedulePolicy(new RetryPolicy(1, Duration.ZERO, 1, Duration.ofSeconds(1)), Duration.ofSeconds(1),
                ScheduleDefinitionChangePolicy.FAIL, ScheduleMisfirePolicy.FIRE_ONCE, Duration.ofMinutes(1), ScheduleFailureMode.CONTINUE);
        var settings = new SchedulerSettings(Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(1), 1, 1, Duration.ofSeconds(3), policy, 4);
        var identity = new RuntimeIdentity("worker");
        identity.acquired("token", 1, () -> true);
        var calls = new AtomicInteger();
        try (var scheduler = new RedissonScheduledTaskTemplate(client, keys, json,
                new RedissonLockTemplate(client, keys), settings,
                (expression, zone) -> { throw new AssertionError("No cron"); }, identity, RetryClassifier.infrastructureFailures())) {
            scheduler.register("active", new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO), calls::incrementAndGet);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(calls).hasValue(1));
        }
    }

    @Test
    void globalRetirementRemovesSharedIndexesAndPreventsResurrection() {
        var lock = new RedissonLockTemplate(client, keys);
        var policy = new SchedulePolicy(new RetryPolicy(1, Duration.ZERO, 1, Duration.ofSeconds(1)), Duration.ofSeconds(1),
                ScheduleDefinitionChangePolicy.FAIL, ScheduleMisfirePolicy.FIRE_ONCE, Duration.ofMinutes(1), ScheduleFailureMode.CONTINUE);
        var settings = new SchedulerSettings(Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(1), 1, 2, Duration.ofSeconds(3), policy, 4);
        var identity = new RuntimeIdentity("worker");
        identity.acquired("token", 1, () -> true);
        try (var scheduler = new RedissonScheduledTaskTemplate(client, keys, json, lock, settings,
                (expression, zone) -> { throw new AssertionError("No cron in this test"); }, identity, RetryClassifier.infrastructureFailures())) {
            var spec = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ofHours(1));
            var registration = scheduler.register("job", spec, () -> 1);
            registration.close();
            assertThat(client.<String>getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE).contains("job")).isTrue();
            var commands = new RedissonManagementCommandService(client, keys, lock, json);
            assertThat(commands.retireTask("job")).isTrue();
            assertThat(new ManagementIndex(client, keys).resources("tasks").contains("job")).isFalse();
            assertThat(client.<String>getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE).contains("job")).isFalse();
            assertThat(commands.triggerTask("job")).isFalse();
            assertThat(new RedissonManagementQueryService(client, keys, json).task("job")).isEmpty();
            assertThatThrownBy(() -> scheduler.register("job", spec, () -> 1)).isInstanceOf(RuntimeException.class);
            assertThat(client.<String, String>getMap(keys.key("schedule-state", "job"), StringCodec.INSTANCE).readAllMap())
                    .containsOnlyKeys("retired");
        }
    }
}
