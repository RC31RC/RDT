package io.github.rdt.support;

import java.time.Duration;

/**
 * Retry budget shared by message consumers and scheduled tasks.
 * maxAttempts includes the initial invocation; zero delay disables additional attempts.
 */
public record RetryPolicy(int maxAttempts, Duration delay, double multiplier, Duration maxDelay) {
    public RetryPolicy {
        RdtSupport.require(maxAttempts > 0, "maxAttempts must be positive");
        RdtSupport.require(delay != null && !delay.isNegative(), "retry delay must not be negative");
        RdtSupport.require(Double.isFinite(multiplier) && multiplier >= 1,
                "retry multiplier must be finite and at least 1");
        RdtSupport.require(maxDelay != null && !maxDelay.isNegative() && maxDelay.compareTo(delay) >= 0,
                "max retry delay must cover retry delay");
        RdtSupport.require(delay.isZero() || delay.toMillis() > 0, "retry delay must be at least 1ms");
    }

    public boolean canRetry(int failedAttempt) {
        return failedAttempt > 0 && failedAttempt < maxAttempts && !delay.isZero();
    }

    /** The first failure uses delay, the second delay * multiplier, capped at maxDelay. */
    public Duration delayAfter(int failedAttempt) {
        RdtSupport.require(failedAttempt > 0, "failed attempt must be positive");
        double millis = delay.toMillis() * Math.pow(multiplier, failedAttempt - 1.0);
        return Duration.ofMillis((long) Math.min(millis, maxDelay.toMillis()));
    }
}
