package io.github.rdt.idempotency;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;

/** Correlates independent observations without claiming business execution. */
public interface EventCoalescer {
    /**
     * Returns a shared round ID. Each source contributes at most one distinct event per round.
     * Matching is FIFO within a bounded window, not proof that two physical events are identical.
     */
    String observe(String fingerprint, String source, String eventId, Policy policy);

    record Policy(Duration window, int maxRounds, int maxSources) {
        public Policy {
            RdtSupport.require(window != null && window.toMillis() > 0
                            && window.compareTo(Duration.ofMinutes(1)) <= 0,
                    "Correlation window must be between one millisecond and one minute");
            RdtSupport.require(maxRounds > 0 && maxRounds <= 256 && maxSources > 0 && maxSources <= 64,
                    "Correlation capacity is out of range");
        }
    }
}
