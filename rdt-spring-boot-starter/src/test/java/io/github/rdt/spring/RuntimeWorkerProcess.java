package io.github.rdt.spring;

import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.support.RetryClassifier;
import io.github.rdt.queue.QueueOptions;

import io.github.rdt.redisson.lease.RedisOwnerLeaseService;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.schedule.JobRoute;
import io.github.rdt.schedule.ScheduleOptions;
import io.github.rdt.schedule.ScheduleSpec;
import io.github.rdt.spring.config.RdtInstanceProperties;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.spring.lease.NodeIdentityLease;
import io.github.rdt.spring.lease.RdtRuntimeRegistry;
import io.github.rdt.redisson.schedule.RedissonScheduledTaskTemplate;
import io.github.rdt.spring.support.RdtAnnotationRegistrar;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.mock.env.MockEnvironment;

/** A separate JVM, including native worker transport, identity lease and process-kill fault windows. */
public final class RuntimeWorkerProcess {
    private RuntimeWorkerProcess() { }
    public static void main(String[] args) throws Exception {
        String address = args[0];
        KeySpace keys = new KeySpace(args[1]);
        String instance = args[2];
        Config config = new Config();
        config.setThreads(2).setNettyThreads(2).setLockWatchdogTimeout(1200);
        config.useSingleServer().setAddress(address).setConnectionPoolSize(10).setConnectionMinimumIdleSize(1)
                .setSubscriptionConnectionPoolSize(4).setSubscriptionConnectionMinimumIdleSize(1);
        RedissonClient client = Redisson.create(config);
        JsonCodec json = new JsonCodec();
        var locks = new RedissonLockTemplate(client, keys);
        var identity = new RuntimeIdentity(instance);
        RdtProperties properties = new Binder(new MapConfigurationPropertySource(Map.of(
                "rdt.namespace", keys.namespace(), "rdt.instance-id", instance,
                "rdt.schedule.poll-interval", "200ms", "rdt.schedule.worker-lease", "4s",
                "rdt.schedule.max-concurrent-tasks", "2", "rdt.queue.max-concurrent-handlers", "4")))
                .bindOrCreate("rdt", RdtProperties.class);
        var timing = new RdtInstanceProperties(Duration.ofSeconds(6), Duration.ofSeconds(1));
        var beans = new DefaultListableBeanFactory();
        try (var lease = new NodeIdentityLease(new RedisOwnerLeaseService(client, keys, locks), properties, timing,
                beans.getBeanProvider(RdtAnnotationRegistrar.class), List::of, identity);
             var scheduler = new RedissonScheduledTaskTemplate(client, keys, json, locks, properties.schedule().settings(), new io.github.rdt.spring.schedule.SpringCronParser(), identity, RetryClassifier.infrastructureFailures());
             var queue = new RedissonMessageQueueTemplate(client, keys, json,
                     new QueuePolicy(3, Duration.ofMillis(600), Duration.ofMillis(500), Duration.ofMillis(250), Duration.ofSeconds(2), false, Duration.ofMinutes(5), Duration.ZERO, 0, 1, Duration.ofSeconds(3), 4, 2, 8, 32, 262144, 10000, 67108864L, 16777216), locks, failure -> { }, identity, RetryClassifier.infrastructureFailures())) {
            var cadence = new ScheduleSpec.FixedRate(Duration.ofHours(1), Duration.ofHours(1));
            scheduler.register("routed-job", cadence, ScheduleOptions.builder().route(JobRoute.SPECIFIED)
                    .targetInstanceId("worker-b").build(), () -> {
                client.getList(keys.key("test-results", "routed"), StringCodec.INSTANCE).add(instance + ":" + identity.runId());
                return instance;
            });
            scheduler.register("crash-job", cadence, () -> {
                long attempt = client.getAtomicLong(keys.key("test-count", "job-crash")).incrementAndGet();
                client.<String>getBucket(keys.key("test-owner", "job-crash"), StringCodec.INSTANCE).set(instance);
                if (attempt == 1) Thread.sleep(30000);
                return instance;
            });
            queue.subscribe(new QueueSubscription("crash-pending", QueueMode.STREAM, "workers", 2, "", QueueOptions.defaults(), 0, "crash-pending" + ":" + "workers"), message -> {
                long attempt = client.getAtomicLong(keys.key("test-count", "pending-crash")).incrementAndGet();
                client.<String>getBucket(keys.key("test-owner", "pending-crash"), StringCodec.INSTANCE).set(instance);
                if (attempt == 1) Thread.sleep(30000);
                client.getList(keys.key("test-results", "pending-crash"), StringCodec.INSTANCE).add(message.id());
            });
            queue.subscribe(new QueueSubscription("competing", QueueMode.STREAM, "workers", 2, "", QueueOptions.defaults(), 0, "competing" + ":" + "workers"), message ->
                    client.getList(keys.key("test-results", "competing"), StringCodec.INSTANCE).add(message.id()));
            queue.subscribe(QueueSubscription.ordered("ordered", "workers", 2, 2), message -> {
                String body = message.body(json, String.class);
                if (body.equals("A1") && client.getAtomicLong(keys.key("test-count", "A1")).incrementAndGet() == 1) {
                    client.getList(keys.key("test-results", "ordered"), StringCodec.INSTANCE).add("A1-failed");
                    throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "retry head");
                }
                client.getList(keys.key("test-results", "ordered"), StringCodec.INSTANCE).add(body);
            });
            queue.subscribe(new QueueSubscription("reliable-broadcast", QueueMode.RELIABLE_BROADCAST, "replicas", 1, "", QueueOptions.defaults(), 0, "reliable-broadcast" + ":" + "replicas"), message ->
                    client.getList(keys.key("test-results", "broadcast"), StringCodec.INSTANCE).add(instance));
            lease.afterSingletonsInstantiated();
            var registry = new RdtRuntimeRegistry(client, keys, json, properties, timing,
                    new MockEnvironment().withProperty("spring.application.name", "rdt-process-it"), identity, RdtMetrics.NONE);
            registry.start();
            client.<String>getBucket(keys.key("test-ready", instance), StringCodec.INSTANCE).set(identity.runId());
            new CountDownLatch(1).await();
            registry.stop();
        } finally { client.shutdown(); }
    }
}
