package io.github.rdt.redisson.queue;

import io.github.rdt.queue.QueuePolicy;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ExecutionBudgetTest {
    @Test
    void sharedBudgetBackpressuresReadsWhileHandlerAndBufferAreFull() throws Exception {
        var policy = new QueuePolicy(1, Duration.ZERO, Duration.ofSeconds(5), Duration.ofMillis(100),
                Duration.ofSeconds(1), false, Duration.ofMinutes(5), Duration.ZERO, 0,
                1, Duration.ofSeconds(1), 1, 1, 1, 1, 262144, 10000, 67108864L, 16777216);
        var running = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var completed = new CountDownLatch(2);
        var secondRan = new AtomicBoolean();
        try (var budget = new ExecutionBudget(policy)) {
            budget.reserve();
            budget.enqueue(() -> {
                running.countDown();
                try { unblock.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }, () -> { budget.release(1); completed.countDown(); });
            assertThat(running.await(2, TimeUnit.SECONDS)).isTrue();
            budget.reserve();
            budget.enqueue(() -> secondRan.set(true), () -> { budget.release(1); completed.countDown(); });
            assertThat(budget.tryReserve()).isFalse();
            assertThat(budget.tryAcquireHandler()).isFalse();
            assertThat(secondRan).isFalse();
            unblock.countDown();
            assertThat(completed.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(secondRan).isTrue();
            assertThat(budget.tryReserve()).isTrue();
            budget.release(1);
        } finally { unblock.countDown(); }
    }
}
