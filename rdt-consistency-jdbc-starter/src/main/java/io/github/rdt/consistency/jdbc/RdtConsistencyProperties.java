package io.github.rdt.consistency.jdbc;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.LocalTime;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("rdt.consistency")
public record RdtConsistencyProperties(
        @DefaultValue("false")
        boolean enabled,
        @DefaultValue("false")
        boolean jdbcDeadLetters,
        @DefaultValue("false")
        boolean inboxEnabled,
        @DefaultValue("500ms")
        Duration relayInterval,
        @DefaultValue("16")
        int batchSize,
        @DefaultValue("60s")
        Duration claimLease,
        @DefaultValue("10")
        int maxAttempts,
        @DefaultValue("1s")
        Duration retryDelay,
        @DefaultValue("1h")
        Duration maxRetryDelay,
        @DefaultValue("7d")
        Duration retention,
        @DefaultValue("03:30")
        LocalTime cleanupTime,
        @DefaultValue("5m")
        Duration cleanupJitter,
        @DefaultValue("1000")
        int cleanupBatchSize,
        @DefaultValue("10000")
        int cleanupLimit,
        @DefaultValue("100ms")
        Duration cleanupPause,
        @DefaultValue("")
        String schema) {
    public RdtConsistencyProperties {
        RdtSupport.positive(relayInterval, "relay interval");
        RdtSupport.positive(claimLease, "claim lease");
        RdtSupport.positive(retention, "consistency retention");
        RdtSupport.positive(retryDelay, "outbox retry delay");
        RdtSupport.require(maxRetryDelay.compareTo(retryDelay) >= 0 && maxAttempts > 0, "Invalid outbox retry policy");
        RdtSupport.require(batchSize > 0 && batchSize <= 500 && cleanupBatchSize > 0 && cleanupLimit >= cleanupBatchSize,
                "Invalid consistency batch limits");
        RdtSupport.require(!cleanupJitter.isNegative() && !cleanupPause.isNegative(), "Invalid cleanup timing");
        RdtSupport.require(schema.isEmpty() || schema.matches("[A-Za-z_][A-Za-z0-9_]*"), "Invalid SQL schema identifier");
    }
}
