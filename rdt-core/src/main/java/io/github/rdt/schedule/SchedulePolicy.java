package io.github.rdt.schedule;

import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryPolicy;
import java.time.Duration;

/** Effective policy for one cluster task; unrelated task IDs execute independently. */
public record SchedulePolicy(RetryPolicy retry, Duration resultTtl,
                             ScheduleDefinitionChangePolicy definitionChangePolicy,
                             ScheduleMisfirePolicy misfirePolicy, Duration misfireThreshold,
                             ScheduleFailureMode failureMode) {
    public SchedulePolicy {
        RdtSupport.require(retry != null && definitionChangePolicy != null && misfirePolicy != null && failureMode != null,
                "Schedule policies must not be null");
        RdtSupport.require(failureMode != ScheduleFailureMode.RETRY || !retry.delay().isZero(),
                "RETRY mode requires a positive retry delay");
        RdtSupport.positive(resultTtl, "schedule result ttl");
        if (misfirePolicy == ScheduleMisfirePolicy.SKIP) RdtSupport.positive(misfireThreshold, "schedule misfire threshold");
    }

    public SchedulePolicy resolve(ScheduleOptions options) {
        RdtSupport.require(options != null, "Schedule options must not be null");
        return new SchedulePolicy(new RetryPolicy(
                options.maxAttempts() == null ? retry.maxAttempts() : options.maxAttempts(),
                options.retryDelay() == null ? retry.delay() : options.retryDelay(),
                options.retryMultiplier() == null ? retry.multiplier() : options.retryMultiplier(),
                options.maxRetryDelay() == null ? retry.maxDelay() : options.maxRetryDelay()),
                options.resultTtl() == null ? resultTtl : options.resultTtl(),
                options.definitionChangePolicy() == null ? definitionChangePolicy : options.definitionChangePolicy(),
                options.misfirePolicy() == null ? misfirePolicy : options.misfirePolicy(),
                options.misfireThreshold() == null ? misfireThreshold : options.misfireThreshold(),
                options.failureMode() == null ? failureMode : options.failureMode());
    }

    public boolean canRetry(int failedAttempt) {
        return failureMode == ScheduleFailureMode.RETRY && retry.canRetry(failedAttempt);
    }
}
