package io.github.rdt.redisson.queue;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.queue.MessageHandler;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.runtime.RuntimeIdentity;

import org.redisson.api.RStream;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

final class OrderedPartitionSubscriptionRuntime extends SubscriptionRuntime {
    private static final Logger log =
            LoggerFactory.getLogger(OrderedPartitionSubscriptionRuntime.class);
    private final RuntimeIdentity identity;
    private final QueuePolicy policy;
    private final DistributedLockTemplate locks;
    private final QueueStreams streams;
    private final ExecutionBudget budget;
    private final StreamMessageLifecycle lifecycle;

    OrderedPartitionSubscriptionRuntime(
            QueueSubscription spec,
            MessageHandler handler,
            RuntimeIdentity identity,
            QueuePolicy policy,
            DistributedLockTemplate locks,
            QueueStreams streams,
            ExecutionBudget budget,
            StreamMessageLifecycle lifecycle) {
        this.identity = identity;
        this.policy = policy;
        this.locks = locks;
        this.streams = streams;
        this.budget = budget;
        this.lifecycle = lifecycle;
        for (int i = 0; i < spec.concurrency(); i++) {
            int start = i;
            tasks.add(budget.submit(() -> consumeOrdered(spec, handler, active, start)));
        }
    }

    private QueueSubscription partitionSpec(QueueSubscription spec, int partition) {
        return new QueueSubscription(
                streams.partitionTopic(spec.destination(), partition),
                QueueMode.STREAM,
                spec.group(),
                1,
                spec.displayName(),
                spec.options(),
                0,
                streams.partitionTopic(spec.destination(), partition) + ":" + spec.group());
    }

    private void consumeOrdered(
            QueueSubscription spec, MessageHandler handler, AtomicBoolean active, int start) {
        String consumer = identity.instanceId() + ":" + identity.runId() + ":p" + start;
        QueuePolicy effective = policy.resolve(spec.options());
        int partition = start % spec.orderedPartitions();
        while (active.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (!identity.acceptingNewWork()) {
                    Thread.sleep(effective.pollTimeout());
                    continue;
                }
                budget.reserve();
                try {
                    QueueSubscription channel = partitionSpec(spec, partition);
                    var owner =
                            locks.tryLock(
                                    "ordered:" + channel.destination() + ":" + channel.group(),
                                    new LockOptions(
                                            LockType.REENTRANT, Duration.ZERO, Duration.ZERO));
                    if (owner.isPresent()) {
                        try (LockHandle handle = owner.get()) {
                            RStream<String, String> target = streams.stream(channel.destination());
                            var pending =
                                    target.pendingRange(
                                            channel.group(),
                                            StreamMessageId.MIN,
                                            StreamMessageId.MAX,
                                            1);
                            if (!pending.isEmpty()) {
                                // An unresolved head blocks the whole partition, including its
                                // retry backoff.
                                var recovered =
                                        target.autoClaim(
                                                channel.group(),
                                                consumer,
                                                0,
                                                TimeUnit.MILLISECONDS,
                                                StreamMessageId.ALL,
                                                1);
                                for (var entry : recovered.getMessages().entrySet()) {
                                    processOrdered(
                                            channel,
                                            effective,
                                            target,
                                            entry.getKey(),
                                            entry.getValue(),
                                            handler,
                                            handle::isHeld);
                                }
                            } else {
                                var fresh =
                                        budget.read(
                                                () ->
                                                        target.readGroup(
                                                                channel.group(),
                                                                consumer,
                                                                StreamReadGroupArgs.neverDelivered()
                                                                        .count(1)
                                                                        .timeout(
                                                                                effective
                                                                                        .pollTimeout())));
                                for (var entry : fresh.entrySet()) {
                                    processOrdered(
                                            channel,
                                            effective,
                                            target,
                                            entry.getKey(),
                                            entry.getValue(),
                                            handler,
                                            handle::isHeld);
                                }
                            }
                        }
                    }
                } finally {
                    budget.release(1);
                }
                partition = (partition + 1) % spec.orderedPartitions();
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception error) {
                if (active.get())
                    log.warn(
                            "Ordered consumer iteration failed topic={}",
                            spec.destination(),
                            error);
                pause(
                        effective.retryDelay().isZero()
                                ? Duration.ofSeconds(1)
                                : effective.retryDelay());
            }
        }
    }

    private void processOrdered(
            QueueSubscription spec,
            QueuePolicy effective,
            RStream<String, String> target,
            StreamMessageId id,
            Map<String, String> data,
            MessageHandler handler,
            BooleanSupplier owner)
            throws InterruptedException {
        budget.acquireHandler();
        try {
            lifecycle.processStream(spec, effective, target, id, data, handler, owner);
        } finally {
            budget.releaseHandler();
        }
    }
}
