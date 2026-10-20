package io.github.rdt.queue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueuePolicyTest {
    @Test
    void subscriptionOverridesInheritUnspecifiedDefaults() {
        QueuePolicy defaults = new QueuePolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(10), true, Duration.ofDays(3), Duration.ofDays(7), 1000, 2, Duration.ofMinutes(1), 16, 4, 8, 32, 262144, 10000, 67108864L, 16777216);
        QueuePolicy effective = defaults.resolve(QueueOptions.builder()
                .maxAttempts(5)
                .retryMultiplier(3)
                .build());

        assertThat(effective.maxAttempts()).isEqualTo(5);
        assertThat(effective.retryDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(effective.retryMultiplier()).isEqualTo(3);
        assertThat(effective.maxRetryDelay()).isEqualTo(Duration.ofMinutes(1));
        assertThat(effective.visibilityTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(effective.autoDeleteAfterAck()).isTrue();
    }
}
