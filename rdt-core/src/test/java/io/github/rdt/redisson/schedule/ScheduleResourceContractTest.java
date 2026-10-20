package io.github.rdt.redisson.schedule;

import io.github.rdt.exception.RdtException;
import io.github.rdt.schedule.SchedulerSettings;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScheduleResourceContractTest {
    @Test
    void historyLimitMustBePositiveAndBounded() {
        for (int limit : new int[]{0, -1, 1000001})
            assertThatThrownBy(() -> new SchedulerSettings(Duration.ofSeconds(1), Duration.ofDays(1),
                    Duration.ofSeconds(1), 1, 128, Duration.ofSeconds(30), null, limit))
                    .isInstanceOf(RdtException.class);
    }
}
