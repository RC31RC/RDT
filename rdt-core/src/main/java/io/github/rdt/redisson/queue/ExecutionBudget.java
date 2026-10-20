package io.github.rdt.redisson.queue;

import io.github.rdt.queue.QueuePolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared execution, buffering and blocking-read budgets; never performs Redis I/O itself. */
final class ExecutionBudget implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ExecutionBudget.class);
    private final Semaphore inFlight;
    private final Semaphore handlers;
    private final Semaphore reads;
    private final ArrayBlockingQueue<Delivery> buffer;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Duration shutdownTimeout;

    ExecutionBudget(QueuePolicy policy) {
        shutdownTimeout = policy.shutdownTimeout();
        inFlight =
                new Semaphore(Math.min(policy.maxConcurrentHandlers() + policy.localBufferCapacity(),
                        policy.localBufferBytes() / policy.maxMessageBytes()), true);
        handlers = new Semaphore(policy.maxConcurrentHandlers(), true);
        reads = new Semaphore(policy.maxBlockingReads(), true);
        buffer = new ArrayBlockingQueue<>(policy.localBufferCapacity(), true);
        for (int i = 0; i < policy.maxConcurrentHandlers(); i++) workers.submit(this::dispatch);
    }

    void reserve() throws InterruptedException {
        inFlight.acquire();
    }

    boolean tryReserve() {
        return inFlight.tryAcquire();
    }

    void release(int count) {
        inFlight.release(count);
    }

    void acquireHandler() throws InterruptedException {
        handlers.acquire();
    }

    boolean tryAcquireHandler() {
        return handlers.tryAcquire();
    }

    void releaseHandler() {
        handlers.release();
    }

    int bufferedMessages() {
        return buffer.size();
    }

    Future<?> submit(Runnable work) {
        return workers.submit(work);
    }

    void acquireRead() throws InterruptedException { reads.acquire(); }

    void releaseRead() { reads.release(); }

    <T> T read(Callable<T> operation) throws Exception {
        reads.acquire();
        try {
            return operation.call();
        } finally {
            reads.release();
        }
    }

    void enqueue(Runnable work, Runnable release) throws InterruptedException {
        buffer.put(new Delivery(work, release));
    }

    private void dispatch() {
        while (!closed.get() || !buffer.isEmpty()) {
            Delivery delivery = null;
            boolean acquired = false;
            try {
                delivery = buffer.poll(500, TimeUnit.MILLISECONDS);
                if (delivery == null) continue;
                acquireHandler();
                acquired = true;
                if (!closed.get()) delivery.work().run();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException failure) {
                log.warn("Buffered handler failed; Pending remains recoverable", failure);
            } finally {
                if (acquired) releaseHandler();
                if (delivery != null) delivery.release().run();
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        // Discarding buffered work must release local permits, never ACK a Redis delivery.
        Delivery discarded;
        while ((discarded = buffer.poll()) != null) discarded.release().run();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS))
                workers.shutdownNow();
        } catch (InterruptedException interrupted) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private record Delivery(Runnable work, Runnable release) {}
}
