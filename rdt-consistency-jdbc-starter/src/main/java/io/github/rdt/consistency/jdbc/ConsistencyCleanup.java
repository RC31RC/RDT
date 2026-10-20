package io.github.rdt.consistency.jdbc;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded passes share a namespace lock; each table has its own deletion budget. */
public final class ConsistencyCleanup implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyCleanup.class);
    private final JdbcOutboxStore outbox;
    private final JdbcInboxStore inbox;
    private final DistributedLockTemplate locks;
    private final String namespace;
    private final RdtConsistencyProperties properties;
    private final AtomicBoolean started = new AtomicBoolean();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("rdt-consistency-cleanup").factory());

    public ConsistencyCleanup(JdbcOutboxStore outbox, JdbcInboxStore inbox, DistributedLockTemplate locks,
                              String namespace, RdtConsistencyProperties properties) {
        this.outbox = outbox;
        this.inbox = inbox;
        this.locks = locks;
        this.namespace = namespace;
        this.properties = properties;
    }

    public void start() {
        if (started.compareAndSet(false, true)) schedule(Duration.ZERO);
    }

    private void schedule(Duration delay) {
        long jitter = ThreadLocalRandom.current().nextLong(properties.cleanupJitter().toMillis() + 1);
        timer.schedule(this::run, delay.toMillis() + jitter, TimeUnit.MILLISECONDS);
    }

    private void run() {
        Duration delay = properties.cleanupInterval();
        try {
            if (cleanupOnce().backlog()) delay = properties.cleanupBacklogDelay();
        } catch (Exception error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            log.error("Consistency cleanup failed namespace={}", namespace, error);
        } finally {
            if (!timer.isShutdown()) schedule(delay);
        }
    }

    /** Each batch commits independently, including when invoked from a business transaction. */
    public CleanupResult cleanupOnce() throws Exception {
        var handle = locks.tryLock("consistency-cleanup",
                new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO));
        if (handle.isEmpty()) return new CleanupResult(false, 0, 0, false);
        try (var ignored = handle.get()) {
            int published = 0;
            int consumed = 0;
            boolean outboxDone = false;
            boolean inboxDone = false;
            int limit = properties.cleanupLimit();
            while ((!outboxDone || !inboxDone) && !Thread.currentThread().isInterrupted()) {
                // Alternate tables so a large Outbox never consumes the Inbox's budget.
                if (!outboxDone) {
                    int batch = Math.min(properties.cleanupBatchSize(), limit - published);
                    int removed = outbox.cleanup(namespace, properties.retention(), batch);
                    published += removed;
                    outboxDone = removed < batch || published == limit;
                }
                if (!inboxDone) {
                    int batch = Math.min(properties.cleanupBatchSize(), limit - consumed);
                    int removed = inbox.cleanup(namespace, properties.retention(), batch);
                    consumed += removed;
                    inboxDone = removed < batch || consumed == limit;
                }
                if (!outboxDone || !inboxDone) Thread.sleep(properties.cleanupPause());
            }
            boolean backlog = published == limit || consumed == limit;
            log.info("Consistency cleanup namespace={} publishedOutbox={} inbox={} budgetPerTable={} backlog={}",
                    namespace, published, consumed, limit, backlog);
            return new CleanupResult(true, published, consumed, backlog);
        }
    }

    public record CleanupResult(boolean lockAcquired, int publishedOutbox, int inbox, boolean backlog) { }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
