package io.github.rdt.redisson.queue;

import io.github.rdt.queue.MessageHandler;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeIdentity;

import org.redisson.api.RStream;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

final class StreamBatchSubscriptionRuntime extends SubscriptionRuntime {
    private static final Logger log = LoggerFactory.getLogger(StreamBatchSubscriptionRuntime.class);
    private final RuntimeIdentity identity;
    private final QueuePolicy policy;
    private final StreamRetryIndex retryIndex;
    private final StreamMessageLifecycle lifecycle;
    private final QueueStreams streams;
    private final ExecutionBudget budget;
    private final PendingRecoveryCoordinator recovery;
    private final Supplier<RdtMetrics> metrics;

    StreamBatchSubscriptionRuntime(
            QueueSubscription spec,
            MessageHandler handler,
            RuntimeIdentity identity,
            QueuePolicy policy,
            StreamRetryIndex retryIndex,
            StreamMessageLifecycle lifecycle,
            QueueStreams streams,
            ExecutionBudget budget,
            PendingRecoveryCoordinator recovery,
            Supplier<RdtMetrics> metrics) {
        this.identity = identity;
        this.policy = policy;
        this.retryIndex = retryIndex;
        this.lifecycle = lifecycle;
        this.streams = streams;
        this.budget = budget;
        this.recovery = recovery;
        this.metrics = metrics;
        tasks.add(budget.submit(() -> consumeBatched(spec, handler, active)));
    }

    private void consumeBatched(
            QueueSubscription spec, MessageHandler handler, AtomicBoolean active) {
        String consumer = identity.instanceId() + ":" + identity.runId() + ":" + UUID.randomUUID();
        QueuePolicy effective = policy.resolve(spec.options());
        Semaphore listenerSlots = new Semaphore(spec.concurrency(), true);
        String cursor = "-";
        long nextRecovery = 0;
        RStream<String, String> target = streams.stream(spec.destination());
        try {
            while (active.get() && !Thread.currentThread().isInterrupted()) {
                int reserved = 0;
                try {
                    if (!identity.acceptingNewWork()) {
                        Thread.sleep(effective.pollTimeout());
                        continue;
                    }
                    listenerSlots.acquire();
                    try {
                        budget.reserve();
                    } catch (InterruptedException error) {
                        listenerSlots.release();
                        throw error;
                    }
                    reserved = 1;
                    // Reserve in-flight slots before Redis reads; a full buffer blocks further
                    // reads.
                    while (reserved < effective.readBatchSize() && listenerSlots.tryAcquire()) {
                        if (!budget.tryReserve()) {
                            listenerSlots.release();
                            break;
                        }
                        reserved++;
                    }
                    if (!identity.acceptingNewWork()) continue;
                    boolean leader = recovery.isLeader();
                    Map<StreamMessageId, Map<String, String>> batch =
                            leader
                                    ? retryIndex.claimRetries(
                                            spec, effective, target, consumer, reserved)
                                    : Map.of();
                    if (leader && batch.isEmpty() && System.nanoTime() >= nextRecovery) {
                        List<String> recovered =
                                retryIndex.recoverablePending(
                                        spec, effective, target, cursor, reserved);
                        cursor = recovered.removeFirst();
                        if (!recovered.isEmpty()) {
                            batch =
                                    target.claim(
                                            spec.group(),
                                            consumer,
                                            effective.visibilityTimeout().toMillis(),
                                            TimeUnit.MILLISECONDS,
                                            recovered.stream()
                                                    .map(retryIndex::streamId)
                                                    .toArray(StreamMessageId[]::new));
                            batch.forEach(
                                    (id, ignored) ->
                                            metrics.get()
                                                    .increment(
                                                            "rdt.queue.reclaim",
                                                            "result",
                                                            "claimed"));
                        }
                        nextRecovery =
                                System.nanoTime()
                                        + Math.max(
                                                TimeUnit.MILLISECONDS.toNanos(100),
                                                effective.visibilityTimeout().toNanos() / 2);
                    }
                    if (batch.isEmpty()) {
                        int count = reserved;
                        batch =
                                budget.read(
                                        () ->
                                                target.readGroup(
                                                        spec.group(),
                                                        consumer,
                                                        StreamReadGroupArgs.neverDelivered()
                                                                .count(count)
                                                                .timeout(effective.pollTimeout())));
                    }
                    for (var entry : batch.entrySet()) {
                        if (!active.get() || !identity.accepting()) break;
                        budget.enqueue(
                                () -> {
                                    if (active.get() && identity.accepting())
                                        lifecycle.processStream(
                                                spec,
                                                effective,
                                                target,
                                                entry.getKey(),
                                                entry.getValue(),
                                                handler);
                                },
                                () -> {
                                    listenerSlots.release();
                                    budget.release(1);
                                });
                        reserved--;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException failure) {
                    if (active.get())
                        log.warn("Stream fetch failed topic={}", spec.destination(), failure);
                    pause(
                            effective.retryDelay().isZero()
                                    ? Duration.ofSeconds(1)
                                    : effective.retryDelay());
                } catch (Exception failure) {
                    if (active.get())
                        log.warn("Stream read failed topic={}", spec.destination(), failure);
                    pause(Duration.ofSeconds(1));
                } finally {
                    if (reserved > 0) {
                        budget.release(reserved);
                        listenerSlots.release(reserved);
                    }
                }
            }
        } finally {
            recovery.close();
        }
    }
}
