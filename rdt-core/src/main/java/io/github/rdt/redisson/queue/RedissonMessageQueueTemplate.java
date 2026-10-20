package io.github.rdt.redisson.queue;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.queue.FailedMessageStore;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageHandler;
import io.github.rdt.queue.MessageQueueTemplate;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.queue.Subscription;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeComponent;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryClassifier;

import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 基于虚拟线程的消费者，支持持久化 Stream 重试状态和待确认列表恢复。 */
public final class RedissonMessageQueueTemplate implements MessageQueueTemplate, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RedissonMessageQueueTemplate.class);
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final QueuePolicy policy;
    private final DistributedLockTemplate locks;
    private final FailedMessageStore failures;
    private final String instanceId;
    private final RuntimeIdentity identity;
    private final RetryClassifier retryClassifier;
    private final StreamRetention retention;
    private final StreamRetryIndex retryIndex;
    private final MessageFailureRecorder failureRecorder;
    private final StreamMessageLifecycle lifecycle;
    private volatile RdtMetrics metrics = RdtMetrics.NONE;

    public void metrics(RdtMetrics value) {
        metrics = value;
        retryIndex.metrics(value);
        failureRecorder.metrics(value);
        lifecycle.metrics(value);
    }

    private final AtomicInteger volatileWaiting = new AtomicInteger();
    private final AtomicLong broadcastDrops = new AtomicLong();
    private final ScheduledExecutorService transfer =
            Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon().name("rdt-queue-maintenance").factory());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final ExecutionBudget budget;
    private final QueueStreams streams;
    private final QueueProducer producer;

    public RedissonMessageQueueTemplate(
            RedissonClient client,
            KeySpace keys,
            JsonCodec json,
            QueuePolicy policy,
            DistributedLockTemplate locks,
            FailedMessageStore failures,
            RuntimeIdentity identity,
            RetryClassifier retryClassifier) {
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.policy = policy;
        this.locks = locks;
        this.failures = failures;
        this.identity = identity;
        this.retryClassifier = Objects.requireNonNull(retryClassifier);
        this.instanceId = identity.instanceId();
        retention = new StreamRetention(policy, client, keys);
        retryIndex = new StreamRetryIndex(client, keys);
        failureRecorder = new MessageFailureRecorder(client, keys, failures, instanceId);
        lifecycle =
                new StreamMessageLifecycle(
                        client,
                        keys,
                        json,
                        locks,
                        identity,
                        retryIndex,
                        retention,
                        failureRecorder,
                        retryClassifier);
        budget = new ExecutionBudget(policy);
        streams = new QueueStreams(client, keys);
        producer = new QueueProducer(client, keys, json, identity, retention, streams, closed);
        transfer.scheduleWithFixedDelay(
                () -> {
                    try {
                        producer.transferDelayed();
                        retention.sweepAcknowledged();
                    } catch (RuntimeException error) {
                        if (!closed.get()) log.warn("Queue maintenance failed", error);
                    }
                },
                1,
                1,
                TimeUnit.SECONDS);
    }

    @Override
    public String send(String destination, Object payload, QueueMode mode) {
        return producer.send(destination, payload, mode);
    }

    @Override
    public String sendMessage(String destination, Message message, QueueMode mode) {
        return producer.sendMessage(destination, message, mode);
    }

    @Override
    public String sendDelayed(String destination, Object payload, Duration delay) {
        return producer.sendDelayed(destination, payload, delay);
    }

    @Override
    public String sendOrdered(String topic, String orderingKey, Object payload, int partitions) {
        return producer.sendOrdered(topic, orderingKey, payload, partitions);
    }

    @Override
    public Subscription subscribe(QueueSubscription spec, MessageHandler handler) {
        RdtSupport.require(
                !closed.get() && handler != null, "Transport is closed or handler is missing");
        // 编程式订阅与注解注册一样，在启动工作线程前完成策略校验。
        policy.resolve(spec.options());
        RdtSupport.require(
                spec.mode() != QueueMode.RELIABLE_BROADCAST || identity.hasStableInstanceId(),
                "Reliable broadcast requires an explicitly configured stable instance ID");
        // 共享目的地名称供独立管理端读取；不是实例注册或心跳，也不随订阅关闭删除。
        if (!spec.displayName().isBlank()) {
            RedisSupport.redisRun(
                    "save queue display name",
                    () ->
                            client.<String, String>getMap(
                                            keys.key("queue-metadata", spec.destination()),
                                            StringCodec.INSTANCE)
                                    .fastPut("displayName", spec.displayName()));
        }
        if (spec.mode() == QueueMode.BROADCAST) {
            return track(
                    new BroadcastSubscriptionRuntime(
                            spec,
                            handler,
                            client,
                            keys,
                            json,
                            identity,
                            policy,
                            budget,
                            transfer,
                            failureRecorder,
                            retryClassifier,
                            closed,
                            volatileWaiting,
                            this::recordBroadcastDrop,
                            () -> metrics));
        }
        final QueueSubscription effective =
                spec.mode() == QueueMode.RELIABLE_BROADCAST
                        ? new QueueSubscription(
                                spec.destination(),
                                QueueMode.RELIABLE_BROADCAST,
                                spec.effectiveGroup(instanceId),
                                spec.concurrency(),
                                spec.displayName(),
                                spec.options(),
                                spec.orderedPartitions(),
                                spec.listenerId())
                        : spec;
        if (effective.orderedPartitions() > 0) {
            streams.registerPartitions(effective.destination(), effective.orderedPartitions());
            for (int partition = 0; partition < effective.orderedPartitions(); partition++) {
                streams.createGroup(
                        new QueueSubscription(
                                streams.partitionTopic(effective.destination(), partition),
                                QueueMode.STREAM,
                                effective.group(),
                                1,
                                effective.displayName(),
                                effective.options(),
                                0,
                                effective.listenerId() + ":" + partition));
            }
        } else if (effective.mode() != QueueMode.BROADCAST) {
            streams.createGroup(effective);
        }
        SubscriptionRuntime runtime =
                effective.orderedPartitions() > 0
                        ? new OrderedPartitionSubscriptionRuntime(
                                effective, handler, identity, policy, locks, streams, budget,
                                lifecycle)
                        : new StreamBatchSubscriptionRuntime(
                                effective,
                                handler,
                                identity,
                                policy,
                                retryIndex,
                                lifecycle,
                                streams,
                                budget,
                                new PendingRecoveryCoordinator(
                                        client, keys, locks, effective, identity, policy),
                                () -> metrics);
        String componentId = "listener:" + spec.listenerId();
        identity.register(
                componentId,
                () ->
                        new RuntimeComponent(
                                "listener",
                                spec.listenerId(),
                                spec.destination(),
                                effective.group(),
                                spec.mode()
                                        + ":"
                                        + (effective.orderedPartitions() == 0
                                                ? "CONCURRENT"
                                                : "ORDERED:" + effective.orderedPartitions())
                                        + ":"
                                        + policy.resolve(spec.options()),
                                spec.concurrency(),
                                lifecycle.active(spec.listenerId()),
                                budget.bufferedMessages(),
                                policy.localBufferCapacity()));
        Subscription subscription =
                () -> {
                    identity.unregister(componentId);
                    runtime.close();
                };
        return track(subscription);
    }

    private Subscription track(Subscription subscription) {
        subscriptions.add(subscription);
        return () -> {
            subscription.close();
            subscriptions.remove(subscription);
        };
    }

    public int bufferedMessages() {
        return budget.bufferedMessages();
    }

    public int bufferCapacity() {
        return policy.localBufferCapacity();
    }

    public long broadcastDrops() {
        return broadcastDrops.get();
    }

    private void recordBroadcastDrop(QueueSubscription spec) {
        metrics.increment("rdt.queue.broadcast.dropped", "destination", spec.destination());
        long dropped = broadcastDrops.incrementAndGet();
        if ((dropped & (dropped - 1)) == 0) {
            log.warn(
                    "Online broadcast capacity exceeded destination={} totalDropped={}",
                    spec.destination(),
                    dropped);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        transfer.shutdownNow();
        try {
            subscriptions.forEach(Subscription::close);
        } finally {
            budget.close();
        }
    }
}
