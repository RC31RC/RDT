package io.github.rdt.spring;

import io.github.rdt.spring.schedule.SpringCronParser;
import io.github.rdt.support.RetryPolicy;
import io.github.rdt.schedule.SchedulePolicy;
import io.github.rdt.schedule.SchedulerSettings;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.support.RetryClassifier;
import io.github.rdt.exception.RdtException;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.queue.Subscription;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.runtime.AdaptiveAdmissionController;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService;
import io.github.rdt.schedule.JobRoute;
import java.util.concurrent.CountDownLatch;
import io.github.rdt.schedule.ScheduleDefinitionChangePolicy;
import io.github.rdt.schedule.ScheduleMisfirePolicy;
import io.github.rdt.schedule.ScheduleOptions;
import io.github.rdt.schedule.ScheduleFailureMode;
import io.github.rdt.schedule.ScheduleSpec;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.redisson.schedule.RedissonScheduledTaskTemplate;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.client.codec.StringCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** 独立客户端在指定 Redis 上竞争同一份持久化触发状态。 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class ClusterScheduleIT {
    private final KeySpace keys = new KeySpace("rdt-schedule-it:" + UUID.randomUUID());
    private RedissonClient a;
    private RedissonClient b;

    @BeforeAll
    void connect() {
        a = client();
        b = client();
    }

    private RedissonClient client() {
        Config config = new Config();
        config.setThreads(2).setNettyThreads(2).setLockWatchdogTimeout(3000);
        config.setPassword(System.getenv("REDIS_PASSWORD"));
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(8)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(4);
        return Redisson.create(config);
    }

    private RedissonScheduledTaskTemplate scheduler(RedissonClient client) {
        return scheduler(client, ScheduleDefinitionChangePolicy.FAIL);
    }

    private RedissonScheduledTaskTemplate scheduler(RedissonClient client,
                                                    ScheduleDefinitionChangePolicy policy) {
        return new RedissonScheduledTaskTemplate(client, keys, new JsonCodec(), new RedissonLockTemplate(client, keys), new RdtProperties.Schedule(true, Duration.ofMillis(250), Duration.ofMinutes(5), Duration.ofSeconds(10), policy, 2, 3, Duration.ZERO, 1, Duration.ofHours(1), ScheduleMisfirePolicy.FIRE_ONCE, Duration.ofMinutes(1), ZoneId.of("UTC"), ScheduleFailureMode.CONTINUE, 128, Duration.ofSeconds(30), 1000).settings(), new io.github.rdt.spring.schedule.SpringCronParser(), leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures());
    }

    @Test
    void drainingKeepsRunningAttemptLeaseAndAllowsCompletion() throws Exception {
        var identity = leasedIdentity(a, keys, "draining-worker");
        var properties = new RdtProperties.Schedule(true, Duration.ofMillis(100), Duration.ofMinutes(5), Duration.ofSeconds(4), ScheduleDefinitionChangePolicy.FAIL, 1, 3, Duration.ZERO, 1, Duration.ofHours(1), ScheduleMisfirePolicy.FIRE_ONCE, Duration.ofMinutes(1), ZoneId.of("UTC"), ScheduleFailureMode.CONTINUE, 128, Duration.ofMillis(900), 1000);
        var started = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        String id = "draining-job";
        Callable<Integer> task = () -> {
            calls.incrementAndGet();
            started.countDown();
            Thread.sleep(2200);
            return 42;
        };
        try (var first = new RedissonScheduledTaskTemplate(a, keys, new JsonCodec(), new RedissonLockTemplate(a, keys), properties.settings(), new io.github.rdt.spring.schedule.SpringCronParser(), identity, RetryClassifier.infrastructureFailures());
             var second = new RedissonScheduledTaskTemplate(b, keys, new JsonCodec(), new RedissonLockTemplate(b, keys), properties.settings(), new io.github.rdt.spring.schedule.SpringCronParser(), leasedIdentity(b, keys, "standby-worker"), RetryClassifier.infrastructureFailures())) {
            var cadence = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO);
            first.register(id, cadence, task);
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            identity.draining(true);
            second.register(id, cadence, task);
            await().atMost(Duration.ofSeconds(6)).untilAsserted(() ->
                    assertThat(first.results(id, 0, 200)).singleElement()
                            .satisfies(result -> assertThat(result.successful()).isTrue()));
            assertThat(calls).hasValue(1);
            assertThat(first.results(id, 0, 200)).allSatisfy(result -> assertThat(result.attempt()).isEqualTo(1));
        }
    }

    @Test
    void runningAttemptRenewsWhilePressureStopsNewJobs() throws Exception {
        var identity = leasedIdentity(a, keys, "pressure-inflight");
        var pressure = new AdaptiveAdmissionController(.85, .65, .9, .7, Duration.ZERO);
        identity.admission(pressure);
        var properties = new RdtProperties.Schedule(true, Duration.ofMillis(100), Duration.ofMinutes(5), Duration.ofSeconds(4),
                ScheduleDefinitionChangePolicy.FAIL, 1, 3, Duration.ZERO, 1, Duration.ofHours(1), ScheduleMisfirePolicy.FIRE_ONCE,
                Duration.ofMinutes(1), ZoneId.of("UTC"), ScheduleFailureMode.CONTINUE, 128, Duration.ofMillis(900), 1000);
        var started = new CountDownLatch(1);
        var deferred = new AtomicInteger();
        try (var scheduler = new RedissonScheduledTaskTemplate(a, keys, new JsonCodec(), new RedissonLockTemplate(a, keys), properties.settings(), new io.github.rdt.spring.schedule.SpringCronParser(), identity, RetryClassifier.infrastructureFailures())) {
            var cadence = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO);
            scheduler.register("pressure-running", cadence, () -> { started.countDown(); Thread.sleep(2200); return "done"; });
            assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
            pressure.refresh(1, .1, System.currentTimeMillis());
            scheduler.register("pressure-deferred", cadence, deferred::incrementAndGet);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(scheduler.results("pressure-running", 0, 10)).singleElement()
                            .satisfies(result -> assertThat(result.successful()).isTrue()));
            assertThat(deferred).hasValue(0);
            assertThat(identity.guard().getAsBoolean()).isTrue();
            pressure.refresh(.1, .1, System.currentTimeMillis());
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(deferred).hasValue(1));
        }
    }

    @Test
    void unavailableSpecifiedWorkerWaitsWithoutCountingBusinessAttempt() {
        String id = "unavailable-worker-job";
        try (var scheduler = scheduler(a)) {
            scheduler.register(id, new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO),
                    ScheduleOptions.builder().route(JobRoute.SPECIFIED).targetInstanceId("not-online").build(),
                    () -> { throw new AssertionError("Must not run on a different worker"); });
            var query = new RedissonManagementQueryService(a, keys, new JsonCodec());
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(query.executions(id, 0, 10)).singleElement().satisfies(execution -> {
                        assertThat(execution.status()).isEqualTo("WAITING");
                        assertThat(execution.attempt()).isZero();
                    }));
            assertThat(scheduler.results(id, 0, 200)).isEmpty();
        }
    }

    @AfterAll
    void cleanup() {
        if (b != null) {
            b.shutdown();
        }
        if (a != null) {
            try {
                a.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            } finally {
                a.shutdown();
            }
        }
    }

    @Test
    void taskNameIsSharedAndCanChangeWithoutResettingSchedule() {
        String taskId = "display-name-v1";
        var queries = new RedissonManagementQueryService(b, keys, new JsonCodec());
        var spec = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ofHours(1));
        try (var first = scheduler(a); var second = scheduler(b)) {
            Subscription registration = first.register(taskId, "Warframe世界状态轮询", spec, () -> null);
            long next = queries.task(taskId).orElseThrow().nextAtMillis();
            second.register(taskId, spec, () -> null);
            assertThat(queries.task(taskId).orElseThrow().displayName()).isEqualTo("Warframe世界状态轮询");
            registration.close();
            first.register(taskId, "Warframe世界数据轮询", spec, () -> null);
            assertThat(queries.task(taskId).orElseThrow().displayName()).isEqualTo("Warframe世界数据轮询");
            assertThat(queries.task(taskId).orElseThrow().nextAtMillis()).isEqualTo(next);
        }
    }

    @Test
    void incompleteScheduleStateIsRepairedWhenTaskIsRegisteredAgain() {
        String taskId = "repair-incomplete-state-v1";
        ScheduleSpec.FixedDelay spec = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ofHours(1));
        RMap<String, String> state = a.getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
        try (var first = scheduler(a); var second = scheduler(b)) {
            first.register(taskId, "状态修复测试", spec, () -> null);
            assertThat(state.get("definition")).isNotBlank();
            assertThat(state.get("next")).isNotBlank();

            state.fastRemove("next");
            second.register(taskId, "状态修复测试", spec, () -> null);
            assertThat(state.get("next")).matches("\\d+");

            String invalidTaskId = taskId + "-invalid";
            first.register(invalidTaskId, "状态修复测试", spec, () -> null);
            RMap<String, String> invalidState = a.getMap(
                    keys.key("schedule-state", invalidTaskId), StringCodec.INSTANCE);
            invalidState.fastPut("next", "not-a-number");
            try (var third = scheduler(b)) {
                third.register(invalidTaskId, "状态修复测试", spec, () -> null);
            }
            assertThat(invalidState.get("next")).matches("\\d+");
        }
    }

    @Test
    void resultExpiresWithoutRemovingTaskDefinitionOrDisplayName() {
        String taskId = "short-result-ttl-v1";
        var json = new JsonCodec();
        var queries = new RedissonManagementQueryService(b, keys, json);
        try (var scheduler = new RedissonScheduledTaskTemplate(a, keys, json, new RedissonLockTemplate(a, keys), new RdtProperties.Schedule(true, Duration.ofMillis(100), Duration.ofSeconds(2), Duration.ofSeconds(3), ScheduleDefinitionChangePolicy.FAIL, 1, 3, Duration.ZERO, 1, Duration.ofHours(1), ScheduleMisfirePolicy.FIRE_ONCE, Duration.ofMinutes(1), ZoneId.of("UTC"), ScheduleFailureMode.CONTINUE, 128, Duration.ofSeconds(30), 1000).settings(), new io.github.rdt.spring.schedule.SpringCronParser(), leasedIdentity(a, keys, null), RetryClassifier.infrastructureFailures())) {
            scheduler.register(taskId, "记录保留测试", new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO),
                    () -> "完成");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(queries.taskResults(taskId, 0, 200)).singleElement()
                            .satisfies(result -> assertThat(result.successful()).isTrue()));
            await().atMost(Duration.ofSeconds(6)).untilAsserted(() ->
                    assertThat(queries.taskResults(taskId, 0, 200)).isEmpty());
            assertThat(queries.task(taskId).orElseThrow().displayName()).isEqualTo("记录保留测试");
        }
    }

    @Test
    void eachFixedRateOccurrenceRunsOnceAndNeverOverlapsAcrossClients() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        Callable<Integer> task = () -> {
            maximum.accumulateAndGet(inside.incrementAndGet(), Math::max);
            try {
                Thread.sleep(Duration.ofMillis(500));
                return calls.incrementAndGet();
            } finally {
                inside.decrementAndGet();
            }
        };
        ScheduleSpec.FixedRate spec = new ScheduleSpec.FixedRate(Duration.ofSeconds(2), Duration.ofSeconds(1));
        RedissonScheduledTaskTemplate first = scheduler(a);
        try (first; RedissonScheduledTaskTemplate second = scheduler(b)) {
            first.register("rate:v1", spec, task);
            second.register("rate:v1", spec, task);
            await().atMost(Duration.ofSeconds(25)).until(() -> calls.get() >= 3);
        }
        List<TaskResult> results = first.results("rate:v1", 0, 200);
        assertThat(maximum).hasValue(1);
        assertThat(results).hasSize(calls.get());
        assertThat(results).allMatch(result -> result.successful() && result.completedAt() != null);
        assertThat(results.stream().map(result -> result.scheduledAt()).distinct().count()).isEqualTo(calls.get());
    }

    @Test
    void fixedDelayUsesClusterCompletionAndContinuesAfterFailure() {
        AtomicInteger calls = new AtomicInteger();
        Callable<Integer> task = () -> {
            if (calls.incrementAndGet() == 1) {
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "expected test failure");
            }
            Thread.sleep(Duration.ofMillis(300));
            return calls.get();
        };
        ScheduleSpec.FixedDelay spec = new ScheduleSpec.FixedDelay(Duration.ofSeconds(1), Duration.ofSeconds(1));
        RedissonScheduledTaskTemplate first = scheduler(a);
        try (first; RedissonScheduledTaskTemplate second = scheduler(b)) {
            first.register("delay:v1", spec, task);
            second.register("delay:v1", spec, task);
            await().atMost(Duration.ofSeconds(25)).until(() -> calls.get() >= 3);
        }
        List<TaskResult> results = first.results("delay:v1", 0, 200).stream()
                .sorted(Comparator.comparing(result -> result.scheduledAt())).toList();
        assertThat(results.getFirst().successful()).isFalse();
        assertThat(results.getLast().successful()).isTrue();
        for (int i = 1; i < results.size(); i++) {
            assertThat(Duration.between(Instant.parse(results.get(i - 1).completedAt()),
                    Instant.parse(results.get(i).startedAt()))).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        }
    }

    @Test
    void failedCronUsesSharedRetryDelayAndSuccessReturnsToCronCadence() {
        String taskId = "cron-failure-retry-v1";
        AtomicInteger calls = new AtomicInteger();
        ScheduleSpec.Cron spec = new ScheduleSpec.Cron("0 0 4 * * *", ZoneId.of("Asia/Shanghai"));
        RMap<String, String> state = a.getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
        try (RedissonScheduledTaskTemplate first = scheduler(a);
             RedissonScheduledTaskTemplate second = scheduler(b)) {
            ScheduleOptions retries = ScheduleOptions.builder().failureMode(ScheduleFailureMode.RETRY)
                    .retryDelay(Duration.ofSeconds(1)).build();
            first.register(taskId, "失败重试测试", spec, retries, () -> {
                if (calls.incrementAndGet() == 1) {
                    throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "expected test failure");
                }
                return calls.get();
            });
            second.register(taskId, "失败重试测试", spec, retries, () -> {
                if (calls.incrementAndGet() == 1) {
                    throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "expected test failure");
                }
                return calls.get();
            });
            assertThat(new RedissonManagementCommandService(a, keys, new RedissonLockTemplate(a, keys), new JsonCodec())
                    .triggerTask(taskId)).isTrue();

            await().atMost(Duration.ofSeconds(15)).until(() -> calls.get() >= 2);

            List<TaskResult> results = first.results(taskId, 0, 200).stream()
                    .sorted(Comparator.comparing(TaskResult::startedAt)).toList();
            assertThat(results).hasSize(2);
            assertThat(results.getFirst().successful()).isFalse();
            assertThat(results.getLast().successful()).isTrue();
            assertThat(Duration.between(Instant.parse(results.getFirst().completedAt()),
                    Instant.parse(results.getLast().startedAt()))).isGreaterThanOrEqualTo(Duration.ofMillis(900));
            assertThat(Long.parseLong(state.get("next")))
                    .isGreaterThan(System.currentTimeMillis() + Duration.ofHours(1).toMillis());
        }
    }

    @Test
    void schedulerUsesTheApplicationClassifierRatherThanExceptionClassNamesOrCauses() {
        var settings = new SchedulerSettings(Duration.ofMillis(100), Duration.ofMinutes(5),
                Duration.ofSeconds(2), 2, 32, Duration.ofSeconds(10),
                new SchedulePolicy(new RetryPolicy(3, Duration.ofMillis(100), 2, Duration.ofSeconds(1)),
                        Duration.ofMinutes(5), ScheduleDefinitionChangePolicy.FAIL,
                        ScheduleMisfirePolicy.FIRE_ONCE, Duration.ofMinutes(1), ScheduleFailureMode.RETRY), 1000);
        var calls = new AtomicInteger();
        var businessCalls = new AtomicInteger();
        RetryClassifier classifier = failure -> failure instanceof ApplicationSystemFailure;
        try (var scheduler = new RedissonScheduledTaskTemplate(a, keys, new JsonCodec(),
                new RedissonLockTemplate(a, keys), settings, new SpringCronParser(),
                leasedIdentity(a, keys, "classifier-scheduler"), classifier)) {
            var cadence = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO);
            scheduler.register("classified-system", cadence, () -> {
                calls.incrementAndGet();
                throw new ApplicationSystemFailure();
            });
            scheduler.register("classified-business", cadence, () -> {
                businessCalls.incrementAndGet();
                throw new IllegalArgumentException(new ApplicationSystemFailure());
            });
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(scheduler.results("classified-system", 0, 10)).hasSize(3);
                assertThat(scheduler.results("classified-business", 0, 10)).hasSize(1);
                assertThat(calls).hasValue(3);
                assertThat(businessCalls).hasValue(1);
            });
        }
    }

    private static final class ApplicationSystemFailure extends RuntimeException { }

    @Test
    void permanentAndUnknownFailuresDoNotRetryEvenWhenTaskEnablesRetry() {
        var businessCalls = new AtomicInteger();
        var unknownCalls = new AtomicInteger();
        var options = ScheduleOptions.builder().failureMode(ScheduleFailureMode.RETRY)
                .maxAttempts(5).retryDelay(Duration.ofMillis(100)).build();
        var cadence = new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO);
        try (var scheduler = scheduler(a)) {
            scheduler.register("permanent-failure", cadence, options, () -> {
                businessCalls.incrementAndGet();
                throw new IllegalArgumentException("permanent business rejection");
            });
            scheduler.register("unknown-failure", cadence, options, () -> {
                unknownCalls.incrementAndGet();
                throw new IllegalStateException("unclassified failure");
            });
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(scheduler.results("permanent-failure", 0, 10)).hasSize(1);
                assertThat(scheduler.results("unknown-failure", 0, 10)).hasSize(1);
            });
            await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(businessCalls).hasValue(1);
                assertThat(unknownCalls).hasValue(1);
            });
        }
    }

    @Test
    void taskOptionsBoundRetryAttemptsAndBackoff() {
        String taskId = "task-options-retry-v1";
        AtomicInteger calls = new AtomicInteger();
        ScheduleOptions policy = ScheduleOptions.builder()
                .failureMode(ScheduleFailureMode.RETRY)
                .maxAttempts(2)
                .retryDelay(Duration.ofMillis(100))
                .retryMultiplier(2)
                .maxRetryDelay(Duration.ofMillis(200))
                .build();
        try (RedissonScheduledTaskTemplate scheduler = scheduler(a)) {
            scheduler.register(taskId, new ScheduleSpec.FixedDelay(Duration.ofHours(1), Duration.ZERO), policy, () -> {
                calls.incrementAndGet();
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "expected task failure");
            });
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(calls).hasValue(2);
                assertThat(scheduler.results(taskId, 0, 200)).hasSize(2).allSatisfy(result -> {
                    assertThat(result.successful()).isFalse();
                    assertThat(result.status()).isEqualTo("FAILED");
                });
            });
        }
    }

    @Test
    void continueModeKeepsCronTimelineEvenWithRetryParameters() {
        String taskId = "continue-cron";
        var options = ScheduleOptions.builder().failureMode(ScheduleFailureMode.CONTINUE)
                .maxAttempts(5).retryDelay(Duration.ofMillis(10)).build();
        try (var scheduler = scheduler(a)) {
            scheduler.register(taskId, new ScheduleSpec.Cron("*/2 * * * * *", ZoneId.of("UTC")), options, () -> {
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "expected failure on every cron trigger");
            });
            await().atMost(Duration.ofSeconds(9)).untilAsserted(() ->
                    assertThat(scheduler.results(taskId, 0, 200)).hasSizeGreaterThanOrEqualTo(3)
                            .allMatch(result -> result.completedAt() != null && !result.successful()));
            var times = scheduler.results(taskId, 0, 200).stream()
                    .map(result -> Instant.parse(result.scheduledAt()).toEpochMilli()).sorted().toList();
            assertThat(times).allMatch(time -> time % 2000 == 0);
            for (int i = 1; i < times.size(); i++) {
                assertThat(times.get(i) - times.get(i - 1)).isGreaterThanOrEqualTo(2000);
            }
            assertThat(a.<String, String>getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE)
                    .containsKey("retryAt")).isFalse();
        }
    }

    @Test
    void retryStateSurvivesNodeSwitchWithoutOverwritingRegularWatermark() {
        String taskId = "retry-failover";
        AtomicInteger calls = new AtomicInteger();
        var options = ScheduleOptions.builder().failureMode(ScheduleFailureMode.RETRY)
                .maxAttempts(3).retryDelay(Duration.ofMillis(700)).retryMultiplier(2)
                .maxRetryDelay(Duration.ofSeconds(2)).build();
        var cadence = new ScheduleSpec.FixedRate(Duration.ofHours(1), Duration.ZERO);
        Callable<Object> failure = () -> {
            calls.incrementAndGet();
            throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "expected bounded failure");
        };
        RMap<String, String> state = a.getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
        try (var first = scheduler(a); var second = scheduler(b)) {
            var registration = first.register(taskId, cadence, options, failure);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                String raw = state.get("execution");
                assertThat(raw).isNotBlank();
                JobExecution execution = new JsonCodec().read(raw, JobExecution.class);
                assertThat(execution.status()).isEqualTo("READY");
                assertThat(execution.attempt()).isEqualTo(1);
                assertThat(first.results(taskId, 0, 200)).hasSize(1);
            });
            long regularNext = Long.parseLong(state.get("next"));
            assertThat(regularNext).isGreaterThan(System.currentTimeMillis() + Duration.ofMinutes(50).toMillis());
            assertThat(new JsonCodec().<JobExecution>read(state.get("execution"), JobExecution.class).attempt()).isEqualTo(1);
            registration.close();
            second.register(taskId, cadence, options, failure);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(calls).hasValue(3);
                assertThat(new JsonCodec().<JobExecution>read(state.get("execution"), JobExecution.class).status()).isEqualTo("FAILED");
                assertThat(second.results(taskId, 0, 200)).hasSize(3)
                        .allMatch(result -> result.completedAt() != null && !result.successful());
            });
            assertThat(Long.parseLong(state.get("next"))).isEqualTo(regularNext);
            var history = second.results(taskId, 0, 200).stream()
                    .sorted(Comparator.comparing(TaskResult::startedAt)).toList();
            assertThat(Duration.between(Instant.parse(history.get(0).completedAt()),
                    Instant.parse(history.get(1).startedAt()))).isGreaterThanOrEqualTo(Duration.ofMillis(700));
            assertThat(Duration.between(Instant.parse(history.get(1).completedAt()),
                    Instant.parse(history.get(2).startedAt()))).isGreaterThanOrEqualTo(Duration.ofMillis(1400));
            await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(calls).hasValue(3));
        }
    }

    @Test
    void cronCancellationAndConflictingDefinitionsAreExplicit() {
        AtomicInteger calls = new AtomicInteger();
        ScheduleSpec.Cron spec = new ScheduleSpec.Cron("*/2 * * * * *", ZoneId.of("UTC"));
        try (RedissonScheduledTaskTemplate first = scheduler(a);
             RedissonScheduledTaskTemplate second = scheduler(b)) {
            Subscription registration = first.register("cron:v1", spec, calls::incrementAndGet);
            assertThatThrownBy(() -> second.register("cron:v1",
                    new ScheduleSpec.Cron("*/3 * * * * *", ZoneId.of("UTC")), calls::incrementAndGet))
                    .isInstanceOf(RdtException.class);
            await().atMost(Duration.ofSeconds(12)).until(() -> calls.get() >= 1);
            registration.close();
        }
        int stopped = calls.get();
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                .untilAsserted(() -> assertThat(calls).hasValue(stopped));
    }

    @Test
    void updatePolicyKeepsTaskIdAndRefreshesDefinitionAndNextTime() {
        String taskId = "schedule-definition-update-v1";
        var queries = new RedissonManagementQueryService(b, keys, new JsonCodec());
        ScheduleSpec.FixedDelay original = new ScheduleSpec.FixedDelay(
                Duration.ofHours(1), Duration.ofHours(1));
        ScheduleSpec.FixedDelay updated = new ScheduleSpec.FixedDelay(
                Duration.ofHours(2), Duration.ofMinutes(10));
        try (RedissonScheduledTaskTemplate first = scheduler(a, ScheduleDefinitionChangePolicy.UPDATE);
             RedissonScheduledTaskTemplate second = scheduler(b, ScheduleDefinitionChangePolicy.UPDATE)) {
            first.register(taskId, "调度定义更新测试", original, () -> null);
            long oldNext = queries.task(taskId).orElseThrow().nextAtMillis();

            second.register(taskId, "调度定义更新测试", updated, () -> null);

            var view = queries.task(taskId).orElseThrow();
            assertThat(view.taskId()).isEqualTo(taskId);
            assertThat(view.definition()).isEqualTo(updated.toString());
            assertThat(view.nextAtMillis()).isNotEqualTo(oldNext);
            assertThat(view.nextAtMillis()).isGreaterThan(System.currentTimeMillis());
            await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(queries.task(taskId).orElseThrow().nextAtMillis())
                            .isEqualTo(view.nextAtMillis()));
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
