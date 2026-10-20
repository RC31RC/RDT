package io.github.rdt.consistency.jdbc;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsistencyCleanup implements AutoCloseable {
    private static final Logger log=LoggerFactory.getLogger(ConsistencyCleanup.class);
    private final JdbcOutboxStore outbox;
    private final JdbcInboxStore inbox;
    private final DistributedLockTemplate locks;
    private final String namespace;
    private final RdtConsistencyProperties properties;
    private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("rdt-consistency-cleanup").factory());
    public ConsistencyCleanup(JdbcOutboxStore outbox,JdbcInboxStore inbox,DistributedLockTemplate locks,String namespace,RdtConsistencyProperties properties) {
        this.outbox=outbox;this.inbox=inbox;this.locks=locks;this.namespace=namespace;this.properties=properties;
    }
    public void start() { schedule(); }
    private void schedule() {
        ZonedDateTime now=ZonedDateTime.now();
        ZonedDateTime next=now.toLocalDate().atTime(properties.cleanupTime()).atZone(now.getZone());
        if (!next.isAfter(now)) next=next.plusDays(1);
        long jitter=ThreadLocalRandom.current().nextLong(properties.cleanupJitter().toMillis()+1);
        timer.schedule(this::run,Duration.between(now,next).toMillis()+jitter,TimeUnit.MILLISECONDS);
    }
    private void run() {
        try { cleanupOnce(); }
        catch (Exception error) { if (error instanceof InterruptedException) Thread.currentThread().interrupt(); log.error("Consistency cleanup failed namespace={}",namespace,error); }
        finally { if (!timer.isShutdown()) schedule(); }
    }
    public void cleanupOnce() throws Exception {
        var handle=locks.tryLock("consistency-cleanup",new LockOptions(LockType.REENTRANT,Duration.ZERO,Duration.ZERO));
        if (handle.isEmpty()) return;
        try (var ignored=handle.get()) {
            int remaining=properties.cleanupLimit();
            while (remaining>0 && !Thread.currentThread().isInterrupted()) {
                int batch=Math.min(properties.cleanupBatchSize(),remaining);
                int removed=outbox.cleanup(namespace,properties.retention(),batch);
                remaining-=removed;
                if (remaining>0) { int n=inbox.cleanup(namespace,properties.retention(),Math.min(batch,remaining));removed+=n;remaining-=n; }
                if (removed==0) break;
                Thread.sleep(properties.cleanupPause());
            }
        }
    }
    @Override
    public void close() { timer.shutdownNow(); }
}
