package io.github.rdt;

import io.github.rdt.exception.RdtConfigurationException;

import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.support.RetryClassifier;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService;

import java.util.stream.LongStream;
import io.github.rdt.redisson.atomic.RedisUtil;
import io.github.rdt.cache.CacheEntry;
import io.github.rdt.redisson.state.RedissonExpiringMapStore;
import io.github.rdt.redisson.state.RedissonCounterStore;
import io.github.rdt.cache.CachePolicy;
import io.github.rdt.redisson.cache.RedissonCacheService;
import io.github.rdt.exception.RdtException;
import io.github.rdt.id.IdMode;
import io.github.rdt.id.IdPolicy;
import io.github.rdt.redisson.id.RedissonIdGenerator;
import io.github.rdt.idempotency.IdempotencyHandle;
import io.github.rdt.idempotency.EventCoalescer.Policy;
import io.github.rdt.redisson.idempotency.RedisEventCoalescer;
import io.github.rdt.redisson.idempotency.RedissonIdempotencyTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.lock.DefaultLockManager;
import io.github.rdt.lock.LockScope;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.management.ManagementOverview;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageHandler;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueueOptions;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;
import io.github.rdt.redisson.queue.RedisFailedMessageStore;
import io.github.rdt.redisson.ratelimit.RedissonRateLimiterTemplate;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RStream;
import org.redisson.api.RMapCache;
import org.redisson.api.RMap;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.state.RedissonSharedStateStore;
import io.github.rdt.redisson.state.RedissonPersistentMapStore;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** 在显式指定的 Redis 上使用独立命名空间执行双客户端集成测试。 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class DistributedPrimitivesIT {
    @Test
    void correlationPreservesRepeatedCommandsAndMatchesAcrossClients() throws Exception {
        var first = new RedisEventCoalescer(a, keys);
        var second = new RedisEventCoalescer(b, keys);
        var policy = new Policy(Duration.ofMillis(250), 8, 4);
        String one = first.observe("same-command", "onebot", "message-1", policy);
        String two = first.observe("same-command", "onebot", "message-2", policy);
        assertThat(two).isNotEqualTo(one);
        assertThat(second.observe("same-command", "qq", "qq-1", policy)).isEqualTo(one);
        assertThat(second.observe("same-command", "qq", "qq-2", policy)).isEqualTo(two);
        assertThat(second.observe("same-command", "qq", "qq-1", policy)).isEqualTo(one);
        Thread.sleep(300);
        assertThat(first.observe("same-command", "onebot", "message-3", policy)).isNotEqualTo(one);
    }

    @Test
    void correlationIsAtomicAcrossConcurrentClientsAndEnforcesCapacity() throws Exception {
        var first = new RedisEventCoalescer(a, keys);
        var second = new RedisEventCoalescer(b, keys);
        var policy = new Policy(Duration.ofSeconds(5), 2, 4);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var start = new CountDownLatch(1);
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                int source = i;
                results.add(executor.submit(() -> {
                    start.await();
                    return (source % 2 == 0 ? first : second).observe(
                            "concurrent-command", "source-" + source, "event-" + source, policy);
                }));
            }
            start.countDown();
            Set<String> rounds = new HashSet<>();
            for (var result : results) rounds.add(result.get(5, TimeUnit.SECONDS));
            assertThat(rounds).hasSize(1);
        }
        first.observe("capacity-command", "source", "one", policy);
        first.observe("capacity-command", "source", "two", policy);
        assertThatThrownBy(() -> second.observe("capacity-command", "source", "three", policy))
                .isInstanceOf(RdtException.class);
    }

    @Test
    void confirmedCacheClearingScansRegionsButPreservesStateAndOtherNamespaces() {
        var keys = new KeySpace("cache-clearing-it:" + UUID.randomUUID());
        var locksA = new RedissonLockTemplate(a, keys);
        var locksB = new RedissonLockTemplate(b, keys);
        var cachePolicy = new CachePolicy(Duration.ofMinutes(5), Duration.ofSeconds(1), 0,
                true, Duration.ofMinutes(1), 32);
        var foreignKeys = new KeySpace(keys.namespace() + "-other");
        var states = new RedissonSharedStateStore(a, keys, json);
        var commands = new RedissonManagementCommandService(a, keys, locksA, json);
        try (var root = new RedissonCacheService(a, keys, json, cachePolicy, locksA);
             var remote = new RedissonCacheService(b, keys, json, cachePolicy, locksB);
             var foreign = new RedissonCacheService(a, foreignKeys, json, cachePolicy,
                     new RedissonLockTemplate(a, foreignKeys))) {
            var region = root.region("prices", cachePolicy);
            var remoteRegion = remote.region("prices", cachePolicy);
            root.put("root", "root-value", Duration.ofMinutes(5));
            region.put("region", "region-value", Duration.ofMinutes(5));
            foreign.put("foreign", "preserved", Duration.ofMinutes(5));
            states.put("facts", "preserved", Duration.ofMinutes(5));
            assertThat(remote.get("root", String.class)).isInstanceOf(CacheEntry.Hit.class);
            assertThat(remoteRegion.get("region", String.class)).isInstanceOf(CacheEntry.Hit.class);
            assertThatThrownBy(() -> commands.clearCache(false)).isInstanceOf(RdtConfigurationException.class);
            assertThat(root.get("root", String.class)).isInstanceOf(CacheEntry.Hit.class);
            assertThat(commands.clearCache(true)).isEqualTo(2);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(remote.get("root", String.class)).isInstanceOf(CacheEntry.Miss.class);
                assertThat(remoteRegion.get("region", String.class)).isInstanceOf(CacheEntry.Miss.class);
            });
            assertThat(states.get("facts", String.class)).contains("preserved");
            assertThat(foreign.get("foreign", String.class)).isInstanceOf(CacheEntry.Hit.class);
        } finally {
            a.getKeys().deleteByPattern("rdt:" + RdtSupport.component(foreignKeys.namespace()) + ":*");
            a.getKeys().deleteByPattern("rdt:" + RdtSupport.component(keys.namespace()) + ":*");
        }
    }

    private final KeySpace keys = new KeySpace("rdt-it:" + UUID.randomUUID());
    private final JsonCodec json = new JsonCodec();
    private RedissonClient a;
    private RedissonClient b;
    private RedissonLockTemplate locksA;
    private RedissonLockTemplate locksB;

    @BeforeAll
    void connect() {
        a = client();
        b = client();
        locksA = new RedissonLockTemplate(a, keys);
        locksB = new RedissonLockTemplate(b, keys);
    }

    private RedissonClient client() {
        Config config = new Config();
        config.setNettyThreads(2).setThreads(2).setLockWatchdogTimeout(1200);
        config.setPassword(System.getenv("REDIS_PASSWORD"));
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(8)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(4);
        return Redisson.create(config);
    }

    @AfterAll
    void disconnect() {
        try {
            if (b != null) {
                b.shutdown();
            }
            if (a != null) {
                a.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*");
            }
        } finally {
            if (a != null) {
                a.shutdown();
            }
        }
    }

    private RedissonCacheService cache(RedissonClient client, boolean local) {
        return new RedissonCacheService(client, keys, json,
                new CachePolicy(Duration.ofSeconds(20), Duration.ofSeconds(2), 0, local, Duration.ofMillis(300), 100),
                new RedissonLockTemplate(client, keys));
    }

    @Test
    void cacheMissNullTtlAndBatch() throws Exception {
        try (RedissonCacheService cache = cache(a, false)) {
            assertThat(cache.get("absent", String.class)).isInstanceOf(CacheEntry.Miss.class);
            AtomicInteger loads = new AtomicInteger();
            for (int i = 0; i < 2; i++) {
                assertThat(cache.<String>getOrLoad("null", String.class, null, true,
                        () -> { loads.incrementAndGet(); return null; })).isNull();
            }
            assertThat(loads).hasValue(1);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(cache.get("null", String.class)).isInstanceOf(CacheEntry.Miss.class));
            cache.putAll(Map.of("batch1", 1, "batch2", 2), Duration.ofSeconds(5));
            assertThat(cache.<Integer>getAll(List.of("batch1", "batch2", "missing"), Integer.class))
                    .containsEntry("batch1", new CacheEntry.Hit<>(1))
                    .containsEntry("batch2", new CacheEntry.Hit<>(2))
                    .containsEntry("missing", new CacheEntry.Miss<>());
        }
    }

    @Test
    void localCacheIsInvalidatedAcrossClientsAndNeverOutlivesRedisTtl() {
        try (RedissonCacheService first = cache(a, true); RedissonCacheService second = cache(b, false)) {
            second.put("local", "old", Duration.ofSeconds(3));
            assertThat(first.get("local", String.class)).isEqualTo(new CacheEntry.Hit<>("old"));
            second.put("local", "new", Duration.ofSeconds(3));
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(first.get("local", String.class)).isEqualTo(new CacheEntry.Hit<>("new")));
            first.put("short", "value", Duration.ofMillis(80));
            first.get("short", String.class);
            await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
                    assertThat(first.get("short", String.class)).isInstanceOf(CacheEntry.Miss.class));
        }
    }

    @Test
    void mapVariantsAndAtomicCacheCounter() {
        var fields = new RedissonExpiringMapStore(a, keys, new JsonCodec());
        var maps = new RedissonPersistentMapStore(a, keys, new JsonCodec());
        var counters = new RedissonCounterStore(a, keys);
        fields.put("expiring", "field", 7, Duration.ofSeconds(2));
        assertThat(fields.<Integer>get("expiring", "field", Integer.class)).contains(7);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(fields.get("expiring", "field", Integer.class)).isEmpty());
        maps.putAll("persistent", Map.of("a", 1, "b", 2));
        assertThat(maps.<Integer>get("persistent", Integer.class)).containsEntry("b", 2);
        assertThat(counters.increment("counter", 2)).isEqualTo(2);
        assertThat(counters.increment("counter", 3)).isEqualTo(5);
    }

    @Test
    void clusterSingleFlightLoadsOnlyOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (RedissonCacheService first = cache(a, false); RedissonCacheService second = cache(b, false);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                RedissonCacheService selected = i % 2 == 0 ? first : second;
                futures.add(executor.submit(() -> selected.getOrLoad("single-flight", String.class, null, true, () -> {
                    calls.incrementAndGet();
                    Thread.sleep(Duration.ofMillis(50));
                    return "loaded";
                })));
            }
            for (Future<String> result : futures) {
                assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("loaded");
            }
            assertThat(calls).hasValue(1);
        }
    }

    @Test
    void mutualExclusionAcrossClients() throws Exception {
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> tasks = new ArrayList<>();
            for (int i = 0; i < 24; i++) {
                RedissonLockTemplate locks = i % 2 == 0 ? locksA : locksB;
                tasks.add(executor.submit(() -> locks.execute("mutex",
                        new LockOptions(LockType.REENTRANT, Duration.ofSeconds(30), Duration.ZERO), () -> {
                    maximum.accumulateAndGet(inside.incrementAndGet(), Math::max);
                    try {
                        Thread.sleep(Duration.ofMillis(10));
                        return 1;
                    } finally {
                        inside.decrementAndGet();
                    }
                })));
            }
            for (Future<Integer> task : tasks) {
                task.get(40, TimeUnit.SECONDS);
            }
        }
        assertThat(maximum).hasValue(1);
    }

    @Test
    void watchdogReentrancyAndExceptionSafeRelease() throws Exception {
        LockOptions immediate = new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO);
        try (LockHandle first = locksA.tryLock("renew", immediate).orElseThrow()) {
            try (LockHandle second = locksA.tryLock("renew", immediate).orElseThrow()) {
                assertThat(second.isHeld()).isTrue();
            }
            Thread.sleep(Duration.ofMillis(1800));
            assertThat(first.isHeld()).isTrue();
            assertThat(locksB.tryLock("renew", immediate)).isEmpty();
        }
        assertThatThrownBy(() -> locksA.execute("exception", immediate, () -> {
            throw new IllegalStateException("business");
        })).isInstanceOf(IllegalStateException.class);
        try (LockHandle handle = locksB.tryLock("exception", immediate).orElseThrow()) {
            assertThat(handle.isHeld()).isTrue();
        }
    }

    @Test
    void fixedLeaseExpiryIsVisibleAndNeverUnlocksAnotherOwner() throws Exception {
        LockHandle expired = locksA.tryLock("lease", new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ofMillis(80))).orElseThrow();
        Thread.sleep(Duration.ofMillis(150));
        try (LockHandle successor = locksB.tryLock("lease", LockOptions.defaults()).orElseThrow()) {
            assertThatThrownBy(expired::close).isInstanceOf(RdtException.class);
            assertThat(successor.isHeld()).isTrue();
        }
    }

    @Test
    void scopedManagerKeepsParentLockAcrossNestedScopeAndRenews() throws Exception {
        DefaultLockManager manager = new DefaultLockManager(locksA);
        LockOptions immediate = new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO);
        try (LockScope outer = manager.beginScope()) {
            manager.lock("scoped", immediate);
            manager.lock("scoped", immediate);
            try (LockScope inner = manager.beginScope()) {
                manager.lock("scoped", immediate);
                assertThat(inner.holds("scoped")).isTrue();
            }
            Thread.sleep(Duration.ofMillis(1800));
            assertThat(outer.holds("scoped")).isTrue();
            assertThat(locksB.tryLock("scoped", immediate)).isEmpty();
        }
        assertThat(manager.currentScope()).isNull();
        try (LockHandle successor = locksB.tryLock("scoped", immediate).orElseThrow()) {
            assertThat(successor.isHeld()).isTrue();
        }
    }

    @Test
    void readWriteAndFairLocksUseExpectedOwnership() {
        try (LockHandle readA = locksA.tryLock("rw", new LockOptions(LockType.READ, Duration.ZERO, Duration.ZERO)).orElseThrow();
             LockHandle readB = locksB.tryLock("rw", new LockOptions(LockType.READ, Duration.ZERO, Duration.ZERO)).orElseThrow()) {
            assertThat(readA.isHeld() && readB.isHeld()).isTrue();
            assertThat(locksB.tryLock("rw", new LockOptions(LockType.WRITE, Duration.ZERO, Duration.ZERO))).isEmpty();
        }
        try (LockHandle fair = locksA.tryLock("fair", new LockOptions(LockType.FAIR, Duration.ZERO, Duration.ZERO)).orElseThrow()) {
            assertThat(fair.isHeld()).isTrue();
            assertThat(locksB.tryLock("fair", new LockOptions(LockType.FAIR, Duration.ZERO, Duration.ZERO))).isEmpty();
        }
    }

    @Test
    void semaphoreReleaseRestoresCapacity() {
        try (LockHandle permit = locksA.trySemaphore("semaphore", 1, Duration.ZERO).orElseThrow()) {
            assertThat(permit.isHeld()).isTrue();
            assertThat(locksB.trySemaphore("semaphore", 1, Duration.ZERO)).isEmpty();
        }
        try (LockHandle permit = locksB.trySemaphore("semaphore", 1, Duration.ZERO).orElseThrow()) {
            assertThat(permit.isHeld()).isTrue();
        }
    }

    @Test
    void idsAreUniqueAndAtomicAllocationsHaveNoConcurrentGaps() throws Exception {
        IdPolicy policy = new IdPolicy(IdMode.SEGMENT, 1, 20, ZoneId.of("UTC"));
        RedissonIdGenerator first = new RedissonIdGenerator(a, keys, policy);
        RedissonIdGenerator second = new RedissonIdGenerator(b, keys, policy);
        for (IdMode mode : IdMode.values()) {
            Set<Long> ids = ConcurrentHashMap.newKeySet();
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    RedissonIdGenerator selected = i % 2 == 0 ? first : second;
                    futures.add(executor.submit(() -> ids.add(selected.next("ids-" + mode, mode))));
                }
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            }
            assertThat(ids).hasSize(100);
            if (mode == IdMode.ATOMIC) {
                assertThat(ids).containsAll(LongStream.rangeClosed(1, 100).boxed().toList());
            }
        }
        assertThat(first.nextFormatted("format", "ORD", "yyyyMMdd", 6)).matches("ORD\\d{14}");
    }

    @Test
    void qpsAndConcurrentCapacityAreSharedWithoutReset() {
        RedissonRateLimiterTemplate first = new RedissonRateLimiterTemplate(a, keys, Duration.ofMinutes(1));
        RedissonRateLimiterTemplate second = new RedissonRateLimiterTemplate(b, keys, Duration.ofMinutes(1));
        assertThat(first.tryAcquire("rate", 2, Duration.ofSeconds(10), Duration.ZERO)).isTrue();
        assertThat(second.tryAcquire("rate", 2, Duration.ofSeconds(10), Duration.ZERO)).isTrue();
        assertThat(first.tryAcquire("rate", 2, Duration.ofSeconds(10), Duration.ZERO)).isFalse();
        assertThatThrownBy(() -> second.tryAcquire("rate", 3, Duration.ofSeconds(10), Duration.ZERO)).isInstanceOf(RdtException.class);
        try (LockHandle permit = first.tryConcurrency("concurrent", 1, Duration.ZERO, Duration.ofSeconds(3)).orElseThrow()) {
            assertThat(permit.isHeld()).isTrue();
            assertThat(second.tryConcurrency("concurrent", 1, Duration.ZERO, Duration.ofSeconds(3))).isEmpty();
        }
        try (LockHandle permit = second.tryConcurrency("concurrent", 1, Duration.ZERO, Duration.ofSeconds(3)).orElseThrow()) {
            assertThat(permit.isHeld()).isTrue();
        }
    }

    @Test
    void atomicStructuresWorkAcrossClients() {
        RedisUtil first = new RedisUtil(a, keys);
        RedisUtil second = new RedisUtil(b, keys);
        assertThat(first.addLong("long", 1)).isEqualTo(1);
        assertThat(second.compareAndSet("long", 1, 7)).isTrue();
        assertThat(first.addDouble("double", 0.5)).isEqualTo(0.5);
        second.setBit("bits", 42, true);
        assertThat(first.bitCount("bits")).isEqualTo(1);
        first.addDistinct("users", "a");
        second.addDistinct("users", "a");
        second.addDistinct("users", "b");
        assertThat(first.distinctCount("users")).isEqualTo(2);
        first.initializeBloom("bloom", 1000, 0.01);
        first.bloomAdd("bloom", "present");
        assertThat(second.bloomMightContain("bloom", "present")).isTrue();
    }

    @Test
    void idempotencyLeaseAndCompletionAreSharedAcrossClients() throws Exception {
        RedissonIdempotencyTemplate first = new RedissonIdempotencyTemplate(a, keys);
        RedissonIdempotencyTemplate second = new RedissonIdempotencyTemplate(b, keys);
        try (IdempotencyHandle handle = first.tryStart("order-1", Duration.ofSeconds(2), Duration.ofSeconds(1)).orElseThrow()) {
            assertThat(second.tryStart("order-1", Duration.ofSeconds(2), Duration.ofSeconds(1))).isEmpty();
            assertThat(handle.complete()).isTrue();
            assertThat(first.isCompleted("order-1")).isTrue();
            assertThat(second.tryStart("order-1", Duration.ofSeconds(2), Duration.ofSeconds(1))).isEmpty();
        }
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(first.isCompleted("order-1")).isFalse());
    }

    private RedissonMessageQueueTemplate messages(RedissonClient client, List<FailedMessage> failures) {
        return new RedissonMessageQueueTemplate(client, keys, json, new QueuePolicy(3, Duration.ofMillis(30), Duration.ofMillis(120), Duration.ofMillis(50), Duration.ofSeconds(3), false, Duration.ZERO, Duration.ZERO, 0, 1, Duration.ofMinutes(1), 16, 4, 8, 32, 262144, 10000, 67108864L, 16777216), new RedissonLockTemplate(client, keys), failures::add, leasedIdentity(client, keys, null), RetryClassifier.infrastructureFailures());
    }

    @Test
    void reliableBroadcastDisplayNamePreservesIndependentGroups() {
        String destination = "display-broadcast-v1";
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        Set<String> firstReceived = ConcurrentHashMap.newKeySet();
        Set<String> secondReceived = ConcurrentHashMap.newKeySet();
        var policy = new QueuePolicy(3, Duration.ofMillis(30), Duration.ofSeconds(2), Duration.ofMillis(50), Duration.ofSeconds(3), false, Duration.ZERO, Duration.ZERO, 0, 1, Duration.ofMinutes(1), 16, 4, 8, 32, 262144, 10000, 67108864L, 16777216);
        try (var first = new RedissonMessageQueueTemplate(a, keys, json, policy, locksA, failures::add, leasedIdentity(a, keys, "node-a"), RetryClassifier.infrastructureFailures());
             var second = new RedissonMessageQueueTemplate(b, keys, json, policy, locksB, failures::add, leasedIdentity(b, keys, "node-b"), RetryClassifier.infrastructureFailures())) {
            var subscription = new QueueSubscription(destination, QueueMode.RELIABLE_BROADCAST, "local-cache-v1", 1, "Warframe缓存更新广播", QueueOptions.defaults(), 0, destination + ":" + "local-cache-v1");
            first.subscribe(subscription, message -> firstReceived.add(message.id()));
            second.subscribe(subscription, message -> secondReceived.add(message.id()));
            String id = first.reliableBroadcast(destination, "updated");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(firstReceived).contains(id);
                assertThat(secondReceived).contains(id);
            });
            var queries = new RedissonManagementQueryService(b, keys, json);
            assertThat(queries.stream(destination).orElseThrow().displayName()).isEqualTo("Warframe缓存更新广播");
            assertThat(queries.stream(destination).orElseThrow().groups()).extracting(group -> group.name())
                    .containsExactlyInAnyOrder("local-cache-v1:" + "node-a",
                            "local-cache-v1:" + "node-b");
            assertThat(failures).isEmpty();
        }
    }

    @Test
    void queueNameIsSharedWithoutChangingDeliveryOrExpiringMessages() {
        String destination = "display-name-v1";
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        Set<String> processed = ConcurrentHashMap.newKeySet();
        var queries = new RedissonManagementQueryService(b, keys, json);
        try (var first = messages(a, failures); var second = messages(b, failures)) {
            first.subscribe(new QueueSubscription(destination, QueueMode.STREAM, "workers", 1, "Warframe世界状态刷新", QueueOptions.defaults(), 0, destination + ":" + "workers"),
                    message -> processed.add(message.id()));
            second.subscribe(new QueueSubscription(destination, QueueMode.STREAM, "workers", 1, "", QueueOptions.defaults(), 0, destination + ":" + "workers"),
                    message -> processed.add(message.id()));
            String messageId = first.send(destination, "latest");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(processed).contains(messageId));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(queries.stream(destination).orElseThrow().groups()).singleElement()
                            .satisfies(group -> assertThat(group.pending()).isZero()));
            var view = queries.stream(destination).orElseThrow();
            assertThat(view.displayName()).isEqualTo("Warframe世界状态刷新");
            assertThat(view.destination()).isEqualTo(destination);
            assertThat(view.length()).isEqualTo(1);
            assertThat(queries.streamMessages(destination, 10)).hasSize(1);
            assertThat(a.getStream(keys.key("stream", destination)).remainTimeToLive()).isEqualTo(-1);

            Message failed = Message.create("failed", json);
            new RedisFailedMessageStore(a, keys, json).save(FailedMessage.stream(destination, "workers", failed,
                    3, "test", "2026-09-23T00:00:00Z"));
            assertThat(queries.deadLetters(0, 200).items().stream().filter(entry -> entry.messageId().equals(failed.id())))
                    .singleElement().satisfies(entry -> assertThat(entry.displayName()).isEqualTo("Warframe世界状态刷新"));
            assertThat(a.getMap(keys.key("dead-letters", destination)).remainTimeToLive()).isEqualTo(-1);

            second.subscribe(new QueueSubscription(destination, QueueMode.STREAM, "other-workers", 1, "Warframe刷新请求", QueueOptions.defaults(), 0, destination + ":" + "other-workers"),
                    message -> { });
            assertThat(queries.stream(destination).orElseThrow().displayName()).isEqualTo("Warframe刷新请求");
            first.send("unnamed-display-v1", "unnamed");
            assertThat(queries.stream("unnamed-display-v1").orElseThrow().displayName()).isEqualTo("unnamed-display-v1");
            assertThat(new RedissonManagementQueryService(b, new KeySpace(keys.namespace() + ":other"), json)
                    .stream(destination)).isEmpty();
        }
    }

    @Test
    void streamRetriesAndDeadLettersOnlyAfterBudgetExhaustion() {
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        AtomicInteger invocations = new AtomicInteger();
        try (RedissonMessageQueueTemplate transport = messages(a, failures)) {
            transport.subscribe(new QueueSubscription("retry", QueueMode.STREAM, "workers", 1, "", QueueOptions.defaults(), 0, "retry" + ":" + "workers"), message -> {
                invocations.incrementAndGet();
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "transient downstream failure");
            });
            String id = transport.send("retry", "payload");
            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(failures).hasSize(1));
            assertThat(invocations).hasValue(3);
            assertThat(failures.getFirst().message().id()).isEqualTo(id);
            assertThat(a.getStream(keys.key("stream", "retry"), StringCodec.INSTANCE).getPendingInfo("workers").getTotal()).isZero();
        }
    }

    @Test
    void subscriptionsUseIndependentRetryBudgets() {
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        AtomicInteger shortAttempts = new AtomicInteger();
        AtomicInteger longAttempts = new AtomicInteger();
        QueueOptions shortPolicy = QueueOptions.builder()
                .maxAttempts(2)
                .retryDelay(Duration.ofMillis(20))
                .maxRetryDelay(Duration.ofMillis(40))
                .build();
        QueueOptions longPolicy = QueueOptions.builder()
                .maxAttempts(4)
                .retryDelay(Duration.ofMillis(20))
                .retryMultiplier(2)
                .maxRetryDelay(Duration.ofMillis(100))
                .build();
        try (RedissonMessageQueueTemplate transport = messages(a, failures)) {
            transport.subscribe(new QueueSubscription("short-retry", QueueMode.STREAM, "workers", 1, "", shortPolicy, 0, "short-retry" + ":" + "workers"), message -> {
                shortAttempts.incrementAndGet();
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "short failure budget");
            });
            transport.subscribe(new QueueSubscription("long-retry", QueueMode.STREAM, "workers", 1, "", longPolicy, 0, "long-retry" + ":" + "workers"), message -> {
                longAttempts.incrementAndGet();
                throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "long failure budget");
            });
            transport.send("short-retry", "short");
            transport.send("long-retry", "long");

            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(failures).hasSize(2));
            assertThat(shortAttempts).hasValue(2);
            assertThat(longAttempts).hasValue(4);
        }
    }

    @Test
    void streamRecoversADeadConsumersPendingMessage() {
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        Set<String> processed = ConcurrentHashMap.newKeySet();
        try (RedissonMessageQueueTemplate producer = messages(a, failures);
             RedissonMessageQueueTemplate recovery = messages(b, failures)) {
            RStream<String, String> stream = a.getStream(keys.key("stream", "recover"), StringCodec.INSTANCE);
            stream.createGroup(StreamCreateGroupArgs.name("workers").id(StreamMessageId.ALL).makeStream());
            String id = producer.send("recover", "durable");
            assertThat(stream.readGroup("workers", "dead-consumer", StreamReadGroupArgs.neverDelivered().count(1))).hasSize(1);
            recovery.subscribe(new QueueSubscription("recover", QueueMode.STREAM, "workers", 1, "", QueueOptions.defaults(), 0, "recover" + ":" + "workers"), message -> processed.add(message.id()));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(processed).contains(id));
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(stream.getPendingInfo("workers").getTotal()).isZero());
            assertThat(failures).isEmpty();
        }
    }

    @Test
    void managementOverviewCountsBatchedResultsAndIgnoresExpiredAndOrphanResults() {
        KeySpace overviewKeys = new KeySpace("rdt-it:overview:" + UUID.randomUUID());
        try {
            RedissonManagementQueryService queries = new RedissonManagementQueryService(a, overviewKeys, json);
            assertThat(queries.overview()).isEqualTo(
                    new ManagementOverview(overviewKeys.namespace(), 0, 0, 0, 0, 0, 0, 0, false));
            RStream<String, String> stream = a.getStream(overviewKeys.key("stream", "work"), StringCodec.INSTANCE);
            stream.createGroup(StreamCreateGroupArgs.name("workers").id(StreamMessageId.ALL).makeStream());
            stream.add(StreamAddArgs.entry("message", "payload"));
            stream.readGroup("workers", "worker", StreamReadGroupArgs.neverDelivered().count(1));
            a.<String, String>getMap(overviewKeys.key("schedule-state", "task"), StringCodec.INSTANCE)
                    .putAll(Map.of("definition", "PT1H", "next", "1000"));
            RMap<String, String> results = a.getMap(
                    overviewKeys.key("schedule-results", "task"), StringCodec.INSTANCE);
            Map<String, String> history = new HashMap<>();
            for (int i = 0; i < 220; i++) {
                history.put(Integer.toString(i), json.write(new TaskResult("task", Integer.toString(i), "1", "2", "worker", i % 2 == 0, "detail", "test-run", "test-execution", 1, (i % 2 == 0) ? "SUCCEEDED" : "FAILED")));
            }
            results.putAll(history);
            var index = new ManagementIndex(a, overviewKeys);
            index.register("streams", "work");
            index.register("tasks", "task");
            history.keySet().forEach(id -> {
                index.record("attempts:task", id, Long.parseLong(id), Duration.ZERO);
                if (Integer.parseInt(id) % 2 != 0) index.record("failures:task", id, Long.parseLong(id), Duration.ZERO);
            });
            String failure = json.write(new TaskResult("task", "0", "1", "2", "worker", false, "expired", "test-run", "test-execution", 1, (false) ? "SUCCEEDED" : "FAILED"));
            results.fastPut("expired", failure);
            results.fastRemove("expired");
            a.<String, String>getMap(overviewKeys.key("schedule-results", "orphan"), StringCodec.INSTANCE)
                    .fastPut("orphan", failure);
            // Overview must count entries without trying to decode dead-letter payloads.
            a.<String, String>getMapCache(overviewKeys.key("dead-letters", "work"), StringCodec.INSTANCE)
                    .putAll(Map.of("one", "payload", "two", "payload"));
            index.record("dead-letters", "one", 1, Duration.ZERO);
            index.record("dead-letters", "two", 2, Duration.ZERO);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(results.containsKey("expired")).isFalse());
            assertThat(queries.overview()).isEqualTo(
                    new ManagementOverview(overviewKeys.namespace(), 1, 1, 1, 1, 1, 110, 2, false));
            assertThat(queries.tasks(0, 200).items()).singleElement().satisfies(task -> assertThat(task.taskId()).isEqualTo("task"));
            assertThat(queries.taskResults("task", 0, 200)).hasSize(200);
            assertThat(queries.taskResults("task", 200, 200)).hasSize(20);
        } finally {
            a.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(overviewKeys.namespace()) + ":*");
        }
    }

    @Test
    void managementCommandsOperateOnOneNamespaceAndIndividualResources() {
        RedissonManagementCommandService commands = new RedissonManagementCommandService(a, keys, locksA, new JsonCodec());
        RedissonManagementQueryService queries = new RedissonManagementQueryService(a, keys, json);

        String taskId = "manual-trigger";
        long originalNext = System.currentTimeMillis() + Duration.ofHours(1).toMillis();
        a.<String, String>getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE).putAll(Map.of(
                "definition", "PT1H", "next", Long.toString(originalNext)));
        assertThat(commands.triggerTask(taskId)).isTrue();
        var state = a.<String, String>getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
        JobExecution requested = json.read(state.get("execution"), JobExecution.class);
        assertThat(requested.status()).isEqualTo("READY");
        assertThat(requested.attempt()).isZero();
        assertThat(requested.scheduledAt()).isLessThan(originalNext);
        assertThat(state.get("next")).isEqualTo(Long.toString(originalNext));
        assertThat(commands.triggerTask(taskId)).isFalse();

        var retryState = a.<String, String>getMap(keys.key("schedule-state", "manual-retry"), StringCodec.INSTANCE);
        long retryAt = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        var waiting = new JobExecution("retry-execution", "manual-retry", 1000, 1, "RETRY_WAIT",
                "worker", "run", "token", retryAt - 1000, retryAt, "retry waiting", 0);
        retryState.putAll(Map.of("definition", "PT1H", "next", Long.toString(originalNext),
                "execution", json.write(waiting)));
        assertThat(commands.triggerTask("manual-retry")).isFalse();
        assertThat(json.<JobExecution>read(retryState.get("execution"), JobExecution.class)).isEqualTo(waiting);

        RStream<String, String> stream = a.getStream(keys.key("stream", "administration"), StringCodec.INSTANCE);
        stream.createGroup(StreamCreateGroupArgs.name("operators").id(StreamMessageId.ALL).makeStream());
        StreamMessageId messageId = stream.add(org.redisson.api.stream.StreamAddArgs.entry("message", "payload"));
        stream.readGroup("operators", "operator-1", StreamReadGroupArgs.neverDelivered().count(1));
        assertThat(queries.streamMessages("administration", 10)).extracting(message -> message.id())
                .contains(messageId.toString());
        assertThat(commands.deleteStreamMessage("administration", messageId.toString())).isTrue();
        assertThat(stream.range(StreamMessageId.MIN, StreamMessageId.MAX)).isEmpty();
        assertThat(stream.getPendingInfo("operators").getTotal()).isZero();

        String destination = "failed-work";
        String group = "operators";
        String failedId = "1-0";
        String field = RdtSupport.component(group) + ":" + RdtSupport.component(failedId);
        a.<String, String>getMap(keys.key("dead-letters", destination), StringCodec.INSTANCE)
                .fastPut(field, "failure");
        assertThat(commands.deleteDeadLetter(destination, group, failedId, "")).isTrue();
        assertThat(a.<String, String>getMap(keys.key("dead-letters", destination), StringCodec.INSTANCE).containsKey(field))
                .isFalse();
    }

    @Test
    void twoConsumersDoNotOverlapSlowStreamProcessing() {
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        try (RedissonMessageQueueTemplate first = messages(a, failures);
             RedissonMessageQueueTemplate second = messages(b, failures)) {
            MessageHandler handler = message -> {
                invocations.incrementAndGet();
                max.accumulateAndGet(inside.incrementAndGet(), Math::max);
                try {
                    Thread.sleep(Duration.ofMillis(450));
                } finally {
                    inside.decrementAndGet();
                }
                done.countDown();
            };
            first.subscribe(new QueueSubscription("slow", QueueMode.STREAM, "workers", 1, "", QueueOptions.defaults(), 0, "slow" + ":" + "workers"), handler);
            second.subscribe(new QueueSubscription("slow", QueueMode.STREAM, "workers", 1, "", QueueOptions.defaults(), 0, "slow" + ":" + "workers"), handler);
            first.send("slow", "longer-than-visibility");
            await().atMost(Duration.ofSeconds(5)).until(() -> done.getCount() == 0);
            await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(invocations).hasValue(1));
            assertThat(max).hasValue(1);
        }
    }

    @Test
    void streamDelayedAndBroadcastTransportsAreUsable() {
        List<FailedMessage> failures = new CopyOnWriteArrayList<>();
        Set<String> received = ConcurrentHashMap.newKeySet();
        AtomicInteger broadcasts = new AtomicInteger();
        try (RedissonMessageQueueTemplate first = messages(a, failures);
             RedissonMessageQueueTemplate second = messages(b, failures)) {
            first.subscribe(new QueueSubscription("ordinary", QueueMode.STREAM, "default", 1, "", QueueOptions.defaults(), 0, "ordinary" + ":" + "default"), m -> received.add(m.id()));
            second.subscribe(new QueueSubscription("delayed", QueueMode.STREAM, "default", 1, "", QueueOptions.defaults(), 0, "delayed" + ":" + "default"), m -> received.add(m.id()));
            first.subscribe(new QueueSubscription("broadcast", QueueMode.BROADCAST, "default", 1, "", QueueOptions.defaults(), 0, "broadcast" + ":" + "default"), m -> broadcasts.incrementAndGet());
            second.subscribe(new QueueSubscription("broadcast", QueueMode.BROADCAST, "default", 1, "", QueueOptions.defaults(), 0, "broadcast" + ":" + "default"), m -> broadcasts.incrementAndGet());
            String ordinary = first.send("ordinary", "one", QueueMode.STREAM);
            String delayed = first.sendDelayed("delayed", "two", Duration.ofMillis(150));
            first.broadcast("broadcast", "three");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(received).contains(ordinary, delayed);
                assertThat(broadcasts).hasValue(2);
            });
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
