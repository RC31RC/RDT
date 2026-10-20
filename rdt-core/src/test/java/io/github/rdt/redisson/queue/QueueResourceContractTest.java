package io.github.rdt.redisson.queue;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtException;
import io.github.rdt.queue.QueueOptions;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class QueueResourceContractTest {
    private QueuePolicy policy(int maxMessage, int localBytes) {
        return new QueuePolicy(3, Duration.ofMillis(10), Duration.ofSeconds(30), Duration.ofSeconds(1),
                Duration.ofSeconds(1), true, Duration.ofDays(1), Duration.ofDays(7), 10000,
                1, Duration.ofHours(1), 4, 2, 8, 32, maxMessage, 10000, 1048576, localBytes);
    }

    @Test
    void byteReservationsCapFetchingEvenWithAnEmptyCountBuffer() throws Exception {
        try (var budget = new ExecutionBudget(policy(1024, 2048))) {
            budget.reserve();
            budget.reserve();
            assertThat(budget.tryReserve()).isFalse();
            budget.release(1);
            assertThat(budget.tryReserve()).isTrue();
            budget.release(2);
        }
    }

    @Test
    void capacityLimitsAreValidatedAndInheritedWithoutConsumerOverrides() {
        assertThatThrownBy(() -> policy(0, 2048)).isInstanceOf(RdtException.class);
        assertThatThrownBy(() -> policy(4096, 2048)).isInstanceOf(RdtException.class);
        var resolved = policy(1024, 2048).resolve(QueueOptions.builder().maxAttempts(8).build());
        assertThat(resolved.maxMessageBytes()).isEqualTo(1024);
        assertThat(resolved.localBufferBytes()).isEqualTo(2048);
        assertThat(resolved.maxBacklogMessages()).isEqualTo(10000);
    }

    @Test
    void oversizedUtf8EnvelopeIsRejectedBeforeRedisIo() {
        var client = mock(RedissonClient.class);
        var admission = new QueueAdmission(client, new KeySpace("contract"), policy(4, 2048));
        assertThatThrownBy(() -> admission.accept("events", "\u00e9\u00e9\u00e9", null))
                .isInstanceOfSatisfying(RdtException.class,
                        error -> assertThat(error.code()).isEqualTo(RdtErrorCode.MESSAGE_TOO_LARGE));
        verifyNoInteractions(client);
    }

    @Test
    void everyQueueScriptAndHistoryIndexHasASingleHashSlot() {
        var keys = new KeySpace("app");
        String destination = "events:tenant";
        String tag = tag(keys.key("stream", destination));
        for (String feature : new String[]{"message-delay", "queue-capacity", "queue-capacity-state", "queue-limits", "stream-cleanup-cursor"})
            assertThat(tag(keys.key(feature, destination))).isEqualTo(tag);
        for (String feature : new String[]{"message-retries", "message-failures", "message-retry-at", "message-first-failure"})
            assertThat(tag(keys.key(feature, RdtSupport.component(destination) + ":group:1-0"))).isEqualTo(tag);
        assertThat(tag(keys.key("management-index", "dead-letters")))
                .isEqualTo(tag(keys.key("management-expiry", "dead-letters")));
        assertThat(tag(keys.key("schedule-results", "job")))
                .isEqualTo(tag(keys.key("management-expiry", "attempts:job")));
    }

    private String tag(String key) {
        return key.substring(key.indexOf('{'), key.indexOf('}') + 1);
    }
}
