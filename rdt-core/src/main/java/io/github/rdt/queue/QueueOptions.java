package io.github.rdt.queue;

import java.time.Duration;

/** Per-subscription overrides. Null fields inherit the template's application-wide defaults. */
public record QueueOptions(Integer maxAttempts, Duration retryDelay, Double retryMultiplier,
                           Duration maxRetryDelay, Duration visibilityTimeout, Duration pollTimeout,
                           Boolean autoDeleteAfterAck, Duration deadLetterTtl) {
    public static QueueOptions defaults() {
        return new QueueOptions(null, null, null, null, null, null, null, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Integer maxAttempts;
        private Duration retryDelay;
        private Double retryMultiplier;
        private Duration maxRetryDelay;
        private Duration visibilityTimeout;
        private Duration pollTimeout;
        private Boolean autoDeleteAfterAck;
        private Duration deadLetterTtl;

        private Builder() { }

        public Builder maxAttempts(int value) { maxAttempts = value; return this; }
        public Builder retryDelay(Duration value) { retryDelay = value; return this; }
        public Builder retryMultiplier(double value) { retryMultiplier = value; return this; }
        public Builder maxRetryDelay(Duration value) { maxRetryDelay = value; return this; }
        public Builder visibilityTimeout(Duration value) { visibilityTimeout = value; return this; }
        public Builder pollTimeout(Duration value) { pollTimeout = value; return this; }
        public Builder autoDeleteAfterAck(boolean value) { autoDeleteAfterAck = value; return this; }
        public Builder deadLetterTtl(Duration value) { deadLetterTtl = value; return this; }

        public QueueOptions build() {
            return new QueueOptions(maxAttempts, retryDelay, retryMultiplier, maxRetryDelay,
                    visibilityTimeout, pollTimeout, autoDeleteAfterAck, deadLetterTtl);
        }
    }
}
