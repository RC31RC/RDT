package io.github.rdt.schedule;

import java.time.Duration;

/** Per-task overrides. Null fields inherit the template's application-wide defaults. */
public record ScheduleOptions(Integer maxAttempts, Duration retryDelay, Double retryMultiplier,
                              Duration maxRetryDelay, Duration resultTtl,
                              ScheduleDefinitionChangePolicy definitionChangePolicy,
                              ScheduleMisfirePolicy misfirePolicy, Duration misfireThreshold,
                              ScheduleFailureMode failureMode, JobRoute route, String targetInstanceId) {
    public static ScheduleOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Integer maxAttempts;
        private Duration retryDelay;
        private Double retryMultiplier;
        private Duration maxRetryDelay;
        private Duration resultTtl;
        private ScheduleDefinitionChangePolicy definitionChangePolicy;
        private ScheduleMisfirePolicy misfirePolicy;
        private Duration misfireThreshold;
        private ScheduleFailureMode failureMode;
        private JobRoute route = JobRoute.HEALTH_FIRST;
        private String targetInstanceId = "";

        private Builder() { }

        public Builder maxAttempts(int value) { maxAttempts = value; return this; }
        public Builder retryDelay(Duration value) { retryDelay = value; return this; }
        public Builder retryMultiplier(double value) { retryMultiplier = value; return this; }
        public Builder maxRetryDelay(Duration value) { maxRetryDelay = value; return this; }
        public Builder resultTtl(Duration value) { resultTtl = value; return this; }
        public Builder definitionChangePolicy(ScheduleDefinitionChangePolicy value) {
            definitionChangePolicy = value; return this;
        }
        public Builder misfirePolicy(ScheduleMisfirePolicy value) { misfirePolicy = value; return this; }
        public Builder misfireThreshold(Duration value) { misfireThreshold = value; return this; }
        public Builder failureMode(ScheduleFailureMode value) { failureMode = value; return this; }

        public Builder route(JobRoute value) { route = value; return this; }
        public Builder targetInstanceId(String value) { targetInstanceId = value; return this; }

        public ScheduleOptions build() {
            return new ScheduleOptions(maxAttempts, retryDelay, retryMultiplier, maxRetryDelay, resultTtl,
                    definitionChangePolicy, misfirePolicy, misfireThreshold, failureMode, route, targetInstanceId);
        }
    }
}
