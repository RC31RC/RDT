package io.github.rdt.redisson.queue;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageHandler;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeComponent;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RetryClassifier;

import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class BroadcastSubscriptionRuntime extends SubscriptionRuntime {
    private static final Logger log = LoggerFactory.getLogger(BroadcastSubscriptionRuntime.class);
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final RuntimeIdentity identity;
    private final QueuePolicy policy;
    private final ExecutionBudget budget;
    private final ScheduledExecutorService transfer;
    private final MessageFailureRecorder failureRecorder;
    private final RetryClassifier retryClassifier;
    private final AtomicBoolean closed;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicInteger volatileWaiting;
    private final Consumer<QueueSubscription> drop;
    private final Supplier<RdtMetrics> metrics;
    private final RTopic topic;
    private final int listener;
    private final String componentId;
    private boolean removed;

    BroadcastSubscriptionRuntime(
            QueueSubscription spec,
            MessageHandler handler,
            RedissonClient client,
            KeySpace keys,
            JsonCodec json,
            RuntimeIdentity identity,
            QueuePolicy policy,
            ExecutionBudget budget,
            ScheduledExecutorService transfer,
            MessageFailureRecorder failureRecorder,
            RetryClassifier retryClassifier,
            AtomicBoolean closed,
            AtomicInteger volatileWaiting,
            Consumer<QueueSubscription> drop,
            Supplier<RdtMetrics> metrics) {
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.identity = identity;
        this.policy = policy;
        this.budget = budget;
        this.transfer = transfer;
        this.failureRecorder = failureRecorder;
        this.retryClassifier = retryClassifier;
        this.closed = closed;
        this.volatileWaiting = volatileWaiting;
        this.drop = drop;
        this.metrics = metrics;
        topic = client.getTopic(keys.key("topic", spec.destination()), StringCodec.INSTANCE);
        Semaphore listenerCapacity = new Semaphore(spec.concurrency());
        listener =
                RedisSupport.redis(
                        "subscribe broadcast",
                        () ->
                                topic.addListener(
                                        String.class,
                                        (channel, raw) -> {
                                            if (stopped.get()
                                                    || !identity.accepting()
                                                    || closed.get()) return;
                                            if (!identity.acceptingNewWork()) {
                                                drop.accept(spec);
                                                return;
                                            }
                                            if (raw.getBytes(StandardCharsets.UTF_8).length > policy.maxMessageBytes()) {
                                                drop.accept(spec);
                                                return;
                                            }
                                            if (!listenerCapacity.tryAcquire()) {
                                                drop.accept(spec);
                                                return;
                                            }
                                            if (!budget.tryAcquireHandler()) {
                                                listenerCapacity.release();
                                                drop.accept(spec);
                                                return;
                                            }
                                            if (!budget.tryReserve()) {
                                                budget.releaseHandler();
                                                listenerCapacity.release();
                                                drop.accept(spec);
                                                return;
                                            }
                                            try {
                                                budget.submit(
                                                        () -> {
                                                            try {
                                                                processVolatile(
                                                                        spec,
                                                                        policy.resolve(
                                                                                spec.options()),
                                                                        json.read(
                                                                                raw, Message.class),
                                                                        handler,
                                                                        listenerCapacity,
                                                                        stopped,
                                                                        0);
                                                            } catch (Exception e) {
                                                                log.error(
                                                                        "Broadcast delivery failed"
                                                                            + " destination={}",
                                                                        spec.destination(),
                                                                        e);
                                                            } finally {
                                                                budget.release(1);
                                                                budget.releaseHandler();
                                                                listenerCapacity.release();
                                                            }
                                                        });
                                            } catch (RuntimeException e) {
                                                budget.release(1);
                                                budget.releaseHandler();
                                                listenerCapacity.release();
                                                log.warn("Broadcast rejected during shutdown", e);
                                            }
                                        }));
        componentId = "listener:" + spec.listenerId();
        identity.register(
                componentId,
                () ->
                        new RuntimeComponent(
                                "listener",
                                spec.listenerId(),
                                spec.destination(),
                                spec.group(),
                                "BROADCAST:" + policy.resolve(spec.options()),
                                spec.concurrency(),
                                spec.concurrency() - listenerCapacity.availablePermits(),
                                0,
                                0));
    }

    @Override
    public synchronized void close() {
        if (removed) return;
        stopped.set(true);
        RedisSupport.redisRun("unsubscribe broadcast", () -> topic.removeListener(listener));
        identity.unregister(componentId);
        removed = true;
    }

    private void processVolatile(
            QueueSubscription spec,
            QueuePolicy effectivePolicy,
            Message message,
            MessageHandler handler,
            Semaphore listenerSlots,
            AtomicBoolean stopped,
            int failuresSoFar)
            throws Exception {
        try {
            handler.handle(message);
            if (failuresSoFar > 0) failureRecorder.clearFirstFailure(spec, message);
        } catch (Exception error) {
            if (error instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw error;
            }
            metrics.get()
                    .increment("rdt.queue.broadcast.failure", "destination", spec.destination());
            int failed = failuresSoFar + 1;
            failureRecorder.recordFirstFailure(spec, message, effectivePolicy);
            if (!retryClassifier.isRetryable(error) || !effectivePolicy.retry().canRetry(failed)) {
                failureRecorder.saveFailure(
                        failureRecorder.failure(spec, message, failed, error), effectivePolicy);
                failureRecorder.clearFirstFailure(spec, message);
                return;
            }
            if (volatileWaiting.incrementAndGet() > policy.localBufferCapacity()) {
                volatileWaiting.decrementAndGet();
                failureRecorder.saveFailure(
                        failureRecorder.failure(
                                spec,
                                message,
                                failed,
                                new RdtConfigurationException(
                                        "Broadcast retry buffer full", error)),
                        effectivePolicy);
                return;
            }
            if (!budget.tryReserve()) {
                volatileWaiting.decrementAndGet();
                drop.accept(spec);
                return;
            }
            try {
                transfer.schedule(
                    () -> {
                        volatileWaiting.decrementAndGet();
                        // Capture this subscription, not a registry lookup that can resolve a
                        // replacement listener.
                        if (closed.get() || stopped.get() || !identity.accepting()) { budget.release(1); return; }
                        if (!identity.acceptingNewWork() || !listenerSlots.tryAcquire()) {
                            budget.release(1);
                            drop.accept(spec);
                            return;
                        }
                        if (!budget.tryAcquireHandler()) {
                            budget.release(1);
                            listenerSlots.release();
                            drop.accept(spec);
                            return;
                        }
                        try {
                            budget.submit(
                                    () -> {
                                        try {
                                            if (!stopped.get())
                                                processVolatile(
                                                        spec,
                                                        effectivePolicy,
                                                        message,
                                                        handler,
                                                        listenerSlots,
                                                        stopped,
                                                        failed);
                                        } catch (Exception failure) {
                                            log.error(
                                                    "Broadcast retry failed destination={}",
                                                    spec.destination(),
                                                    failure);
                                        } finally {
                                            budget.release(1);
                                            budget.releaseHandler();
                                            listenerSlots.release();
                                        }
                                    });
                        } catch (RuntimeException submission) {
                            budget.release(1);
                            budget.releaseHandler();
                            listenerSlots.release();
                            throw submission;
                        }
                    },
                    effectivePolicy.retry().delayAfter(failed).toMillis(),
                    TimeUnit.MILLISECONDS);
            } catch (RuntimeException rejected) {
                budget.release(1);
                volatileWaiting.decrementAndGet();
                throw rejected;
            }
        }
    }
}
