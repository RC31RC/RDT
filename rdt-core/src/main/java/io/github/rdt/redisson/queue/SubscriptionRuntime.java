package io.github.rdt.redisson.queue;

import io.github.rdt.queue.Subscription;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class SubscriptionRuntime implements Subscription {
    protected final AtomicBoolean active = new AtomicBoolean(true);
    protected final List<Future<?>> tasks = new CopyOnWriteArrayList<>();

    protected void pause(Duration delay) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        active.set(false);
        tasks.forEach(task -> task.cancel(true));
    }
}
