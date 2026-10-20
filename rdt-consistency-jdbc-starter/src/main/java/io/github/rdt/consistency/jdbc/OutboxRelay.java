package io.github.rdt.consistency.jdbc;

import io.github.rdt.consistency.OutboxRecord;
import io.github.rdt.consistency.OutboxStore;
import io.github.rdt.queue.MessageQueueTemplate;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryPolicy;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Claims commit before Redis I/O; no JDBC connection is held across a send. */
public final class OutboxRelay implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxStore store;
    private final MessageQueueTemplate messages;
    private final RuntimeIdentity identity;
    private final String namespace;
    private final RdtConsistencyProperties properties;
    private final RetryPolicy retry;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("rdt-outbox-relay").factory());
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public OutboxRelay(OutboxStore store, MessageQueueTemplate messages, RuntimeIdentity identity,
                       String namespace, RdtConsistencyProperties properties) {
        this.store = store;
        this.messages = messages;
        this.identity = identity;
        this.namespace = namespace;
        this.properties = properties;
        retry = new RetryPolicy(properties.maxAttempts(), properties.retryDelay(), 2, properties.maxRetryDelay());
    }

    public synchronized void start() {
        RdtSupport.require(!closed.get(), "Outbox relay is closed");
        if (started.compareAndSet(false, true)) {
            timer.scheduleWithFixedDelay(this::tick, properties.relayInterval().toMillis(),
                    properties.relayInterval().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void tick() {
        try {
            relayOnce();
        } catch (RuntimeException error) {
            // A scheduler exception must not permanently suppress future runs; expired claims recover.
            if (!closed.get()) log.error("Outbox relay failed namespace={}; claims recover after lease expiry", namespace, error);
        }
    }

    public void relayOnce() {
        if (closed.get() || !identity.acceptingNewWork() || !running.compareAndSet(false, true)) return;
        try {
            for (int i = 0; i < properties.batchSize() && !closed.get() && identity.acceptingNewWork(); i++) {
                // Claim one at a time: slow sends must not consume leases of unsent batch members.
                var rows = store.claim(namespace, identity.instanceId() + ":" + identity.runId(), 1, properties.claimLease());
                if (rows.isEmpty() || closed.get() || !identity.acceptingNewWork()) return;
                OutboxRecord row = rows.getFirst();
                try {
                    messages.sendMessage(row.destination(), row.message(), row.queueMode());
                } catch (RuntimeException error) {
                    int failures = row.failureCount() + 1;
                    if (failures >= properties.maxAttempts()) {
                        store.markDead(row, error.toString());
                        log.error("Outbox DEAD namespace={} outboxId={} destination={}", namespace, row.id(), row.destination(), error);
                    } else {
                        store.reschedule(row, retry.delayAfter(failures), error.toString());
                    }
                    continue;
                }
                // A singleton batch acknowledges promptly; later network calls cannot age this claim out.
                store.markPublished(List.of(row));
            }
        } finally {
            running.set(false);
        }
    }

    @Override
    public synchronized void close() {
        closed.set(true);
        timer.shutdownNow();
    }
}
